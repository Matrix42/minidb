package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

public class MiniDbAggregate extends Aggregate implements MiniDbRel {

    public MiniDbAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                           ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                           List<AggregateCall> aggCalls) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode newInput,
                          ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                          List<AggregateCall> aggCalls) {
        return new MiniDbAggregate(getCluster(), traitSet, newInput,
                groupSet, groupSets, aggCalls);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        ImmutableBitSet groupSet = getGroupSet();
        List<AggregateCall> calls = getAggCallList();
        List<AccumulatorFactory> factories = new ArrayList<>();
        RelDataType inputRowType = getInput().getRowType();
        for (AggregateCall call : calls) {
            factories.add(factoryFor(call, inputRowType));
        }
        // key: normalized group values (null preserved); insertion order = output order
        Map<List<Object>, GroupState> groups = new LinkedHashMap<>();
        if (groupSet.isEmpty()) {
            // global aggregate: always one output row, even on empty input
            groups.put(List.of(), new GroupState(factories));
        }
        List<ValueVector> args = new ArrayList<>();
        while (input.hasNext()) {
            VectorSchemaRoot batch = input.next();
            args.clear();
            for (AggregateCall call : calls) {
                args.add(argVector(call, batch, ctx));
            }
            for (int row = 0; row < batch.getRowCount(); row++) {
                List<Object> key = groupKey(batch, groupSet, row);
                GroupState st = groups.computeIfAbsent(key, k -> new GroupState(factories));
                for (int i = 0; i < calls.size(); i++) {
                    st.accs.get(i).add(args.get(i), row);
                }
            }
            for (ValueVector v : args) {
                if (v != null) {
                    v.close();
                }
            }
        }
        input.close();
        if (groups.isEmpty()) {
            return BatchIterator.empty();
        }
        VectorSchemaRoot out = buildOutput(groups, ctx);
        boolean[] done = {false};
        return new BatchIterator() {
            @Override
            public boolean hasNext() {
                return !done[0];
            }

            @Override
            public VectorSchemaRoot next() {
                done[0] = true;
                return out;
            }

            @Override
            public void close() {
                out.close();
            }
        };
    }

    private ValueVector argVector(AggregateCall call, VectorSchemaRoot batch, ExecContext ctx) {
        if (!call.rexList.isEmpty()) {
            if (call.rexList.size() == 1) {
                return ctx.interpreter().eval(call.rexList.get(0), batch);
            }
            throw new UnsupportedOperationException("multi-arg aggregate: " + call);
        }
        // Calcite stores plain column references as argList indices with an
        // empty rexList; read the input vector directly.
        if (!call.getArgList().isEmpty()) {
            if (call.getArgList().size() == 1) {
                return RowCopier.copyVector(
                        batch.getFieldVectors().get(call.getArgList().get(0)),
                        ctx.allocator());
            }
            throw new UnsupportedOperationException("multi-arg aggregate: " + call);
        }
        return null; // COUNT(*)
    }

    private List<Object> groupKey(VectorSchemaRoot batch, ImmutableBitSet groupSet, int row) {
        if (groupSet.isEmpty()) {
            return List.of();
        }
        List<Object> key = new ArrayList<>(groupSet.cardinality());
        for (Integer idx : groupSet) {
            key.add(readObject(batch.getVector(idx), row));
        }
        return key;
    }

    private VectorSchemaRoot buildOutput(Map<List<Object>, GroupState> groups, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        int total = groups.size();
        for (FieldVector v : vectors) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }
        try {
            int row = 0;
            for (Map.Entry<List<Object>, GroupState> e : groups.entrySet()) {
                List<Object> key = e.getKey();
                for (int g = 0; g < key.size(); g++) {
                    writeObject(vectors.get(g), row, key.get(g));
                }
                int col = key.size();
                List<Accumulator> accs = e.getValue().accs;
                for (int i = 0; i < accs.size(); i++) {
                    accs.get(i).write(vectors.get(col + i), row);
                }
                row++;
            }
            for (FieldVector v : vectors) {
                v.setValueCount(total);
            }
            return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        } catch (RuntimeException e) {
            for (FieldVector v : vectors) {
                v.close();
            }
            throw e;
        }
    }

    private static AccumulatorFactory factoryFor(AggregateCall call, RelDataType inputRowType) {
        SqlKind kind = call.getAggregation().kind;
        RelDataType outType = call.getType();
        boolean floatingOut = isFloating(outType.getSqlTypeName());
        boolean floatingArg = isFloatingArg(call, inputRowType);
        boolean distinct = call.isDistinct();
        switch (kind) {
            case COUNT:
                return distinct
                        ? () -> new DistinctAcc(kind, false, false, false)
                        : CountAcc::new;
            case SUM:
                return distinct
                        ? () -> new DistinctAcc(kind, floatingArg, floatingOut, false)
                        : () -> new SumAcc(floatingArg, floatingOut);
            case AVG:
                return distinct
                        ? () -> new DistinctAcc(kind, floatingArg, floatingOut, false)
                        : () -> new AvgAcc(floatingOut);
            case MIN:
            case MAX:
                return distinct
                        ? () -> new DistinctAcc(kind, floatingArg, floatingOut, kind == SqlKind.MIN)
                        : () -> new MinMaxAcc(kind == SqlKind.MIN);
            default:
                throw new UnsupportedOperationException("aggregate not supported: " + kind);
        }
    }

    private static boolean isFloatingArg(AggregateCall call, RelDataType inputRowType) {
        if (!call.rexList.isEmpty()) {
            return isFloating(call.rexList.get(0).getType().getSqlTypeName());
        }
        if (!call.getArgList().isEmpty()) {
            int idx = call.getArgList().get(0);
            return isFloating(inputRowType.getFieldList().get(idx)
                    .getType().getSqlTypeName());
        }
        return false; // COUNT(*)
    }

    private static boolean isFloating(SqlTypeName t) {
        return t == SqlTypeName.DOUBLE || t == SqlTypeName.FLOAT
                || t == SqlTypeName.REAL || t == SqlTypeName.DECIMAL;
    }

    private interface Accumulator {
        void add(ValueVector v, int row);

        void write(FieldVector out, int row);
    }

    @FunctionalInterface
    private interface AccumulatorFactory {
        Accumulator create();
    }

    private static final class GroupState {
        final List<Accumulator> accs;

        GroupState(List<AccumulatorFactory> factories) {
            accs = new ArrayList<>(factories.size());
            for (AccumulatorFactory f : factories) {
                accs.add(f.create());
            }
        }
    }

    private static final class DistinctAcc implements Accumulator {
        private final SqlKind kind;
        private final boolean floatingOut;
        private final boolean min;
        private final java.util.LinkedHashSet<Object> set = new java.util.LinkedHashSet<>();

        DistinctAcc(SqlKind kind, boolean floating, boolean floatingOut, boolean min) {
            this.kind = kind;
            this.floatingOut = floatingOut;
            this.min = min;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return; // DISTINCT aggregates ignore NULLs
            }
            set.add(readObject(v, row));
        }

        @Override
        public void write(FieldVector out, int row) {
            switch (kind) {
                case COUNT:
                    writeLong(out, row, set.size());
                    return;
                case SUM: {
                    if (set.isEmpty()) {
                        out.setNull(row);
                        return;
                    }
                    if (floatingOut) {
                        double s = 0;
                        for (Object o : set) {
                            s += ((Number) o).doubleValue();
                        }
                        writeDouble(out, row, s);
                    } else {
                        long s = 0;
                        for (Object o : set) {
                            s += ((Number) o).longValue();
                        }
                        writeLong(out, row, s);
                    }
                    return;
                }
                case AVG: {
                    if (set.isEmpty()) {
                        out.setNull(row);
                        return;
                    }
                    double s = 0;
                    for (Object o : set) {
                        s += ((Number) o).doubleValue();
                    }
                    if (floatingOut) {
                        writeDouble(out, row, s / set.size());
                    } else {
                        writeLong(out, row, (long) (s / set.size()));
                    }
                    return;
                }
                case MIN:
                case MAX: {
                    if (set.isEmpty()) {
                        out.setNull(row);
                        return;
                    }
                    Object best = null;
                    for (Object o : set) {
                        if (best == null) {
                            best = o;
                            continue;
                        }
                        int cmp = compareObjects(best, o);
                        if (min ? cmp > 0 : cmp < 0) {
                            best = o;
                        }
                    }
                    writeObject(out, row, best);
                    return;
                }
                default:
                    throw new UnsupportedOperationException(
                            "distinct aggregate not supported: " + kind);
            }
        }
    }

    private static final class CountAcc implements Accumulator {
        private long n;

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || !v.isNull(row)) {
                n++;
            }
        }

        @Override
        public void write(FieldVector out, int row) {
            writeLong(out, row, n);
        }
    }

    private static final class SumAcc implements Accumulator {
        private final boolean floating;
        private final boolean floatingOut;
        private long lsum;
        private double dsum;
        private boolean has;

        SumAcc(boolean floating, boolean floatingOut) {
            this.floating = floating;
            this.floatingOut = floatingOut;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return;
            }
            has = true;
            if (floating) {
                dsum += readDouble(v, row);
            } else {
                lsum += readLong(v, row);
            }
        }

        @Override
        public void write(FieldVector out, int row) {
            if (!has) {
                out.setNull(row);
                return;
            }
            if (floatingOut) {
                writeDouble(out, row, dsum);
            } else {
                writeLong(out, row, lsum);
            }
        }
    }

    private static final class AvgAcc implements Accumulator {
        private final boolean floatingOut;
        private double sum;
        private long cnt;

        AvgAcc(boolean floatingOut) {
            this.floatingOut = floatingOut;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return;
            }
            sum += readDouble(v, row);
            cnt++;
        }

        @Override
        public void write(FieldVector out, int row) {
            if (cnt == 0) {
                out.setNull(row);
                return;
            }
            if (floatingOut) {
                writeDouble(out, row, sum / cnt);
            } else {
                writeLong(out, row, (long) (sum / cnt));
            }
        }
    }

    private static final class MinMaxAcc implements Accumulator {
        private final boolean min;
        private Object best;
        private boolean has;

        MinMaxAcc(boolean min) {
            this.min = min;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return;
            }
            Object o = readObject(v, row);
            if (!has) {
                best = o;
                has = true;
                return;
            }
            int c = compareObjects(best, o);
            if (min ? c > 0 : c < 0) {
                best = o;
            }
        }

        @Override
        public void write(FieldVector out, int row) {
            if (!has) {
                out.setNull(row);
                return;
            }
            writeObject(out, row, best);
        }
    }

    private static Object readObject(ValueVector v, int row) {
        if (v.isNull(row)) {
            return null;
        }
        if (v instanceof IntVector iv) {
            return iv.get(row);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(row);
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (v instanceof VarCharVector vv) {
            return new String(vv.get(row), StandardCharsets.UTF_8);
        }
        if (v instanceof BitVector bv) {
            return bv.get(row);
        }
        if (v instanceof DateDayVector dv) {
            return dv.get(row);
        }
        if (v instanceof TimeStampMilliVector tv) {
            return tv.get(row);
        }
        throw new UnsupportedOperationException(
                "cannot aggregate column type: " + v.getMinorType());
    }

    private static long readLong(ValueVector v, int row) {
        if (v instanceof IntVector iv) {
            return iv.get(row);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(row);
        }
        throw new UnsupportedOperationException(
                "not an integral vector: " + v.getMinorType());
    }

    private static double readDouble(ValueVector v, int row) {
        if (v instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (v instanceof IntVector iv) {
            return iv.get(row);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(row);
        }
        throw new UnsupportedOperationException(
                "not a numeric vector: " + v.getMinorType());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareObjects(Object a, Object b) {
        return ((Comparable) a).compareTo(b);
    }

    private static void writeLong(FieldVector out, int row, long v) {
        if (out instanceof BigIntVector bv) {
            bv.setSafe(row, v);
        } else if (out instanceof IntVector iv) {
            iv.setSafe(row, (int) v);
        } else {
            throw new UnsupportedOperationException(
                    "no long writer for " + out.getMinorType());
        }
    }

    private static void writeDouble(FieldVector out, int row, double v) {
        if (out instanceof Float8Vector fv) {
            fv.setSafe(row, v);
        } else {
            throw new UnsupportedOperationException(
                    "no double writer for " + out.getMinorType());
        }
    }

    private static void writeObject(FieldVector out, int row, Object o) {
        if (o == null) {
            out.setNull(row);
            return;
        }
        if (out instanceof IntVector iv) {
            iv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof BigIntVector bv) {
            bv.setSafe(row, ((Number) o).longValue());
        } else if (out instanceof Float8Vector fv) {
            fv.setSafe(row, ((Number) o).doubleValue());
        } else if (out instanceof VarCharVector vv) {
            vv.setSafe(row, o.toString().getBytes(StandardCharsets.UTF_8));
        } else if (out instanceof BitVector bv) {
            bv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof DateDayVector dv) {
            dv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof TimeStampMilliVector tv) {
            tv.setSafe(row, ((Number) o).longValue());
        } else {
            throw new UnsupportedOperationException(
                    "cannot write value to " + out.getMinorType());
        }
    }
}
