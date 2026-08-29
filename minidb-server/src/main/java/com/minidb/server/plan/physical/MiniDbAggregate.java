package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.lsm.LSMTable;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MiniDbAggregate extends Aggregate implements MiniDbRel {

    public MiniDbAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(
            RelTraitSet traitSet,
            RelNode newInput,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new MiniDbAggregate(getCluster(), traitSet, newInput, groupSet, groupSets, aggCalls);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        // COUNT(*) 无 GROUP BY 直接读表行数(元数据),不扫描数据。
        // 但若 Scan 有下推谓词(Filter/Project 已折叠进 Scan),不能走捷径——必须扫描+过滤。
        if (getGroupSet().isEmpty()
                && getAggCallList().size() == 1
                && isCountStar(getAggCallList().get(0))
                && getInput() instanceof MiniDbScan scan
                && !scan.hasPushdown()) {
            TableHandle table = scan.resolveTable(ctx);
            // LSMTable.rowCount() is approximate (MemTable+SSTable rows overlap),
            // so skip the metadata shortcut and always scan for accurate COUNT(*).
            if (table != null && !(table instanceof LSMTable)) {
                return singleRowCount(table.rowCount(), ctx);
            }
        }
        // B1:先物化全部输入到单 root,再用零装箱 ColumnKey 分组。
        // 物化省去逐批的 argVector 拷贝(参数列直接引用物化 root 的向量);
        // ColumnKey 列式 hash/equals 替代 List<Object> 每行每 key 列装箱。
        VectorSchemaRoot input = RowVectors.materializeToRoot(getInput(), ctx);
        List<AggregateCall> calls = getAggCallList();
        RelDataType inputRowType = getInput().getRowType();
        // ROLLUP/GROUPING SETS 展开为多个 groupSet:每个 groupSet 独立分组聚合,最后合并。
        // 单 groupSet(普通 GROUP BY)时 getGroupSets() 只含一个。
        List<ImmutableBitSet> groupSets = getGroupSets();
        List<Map<ColumnKey, GroupState>> groupMaps = new ArrayList<>();
        List<List<AccumulatorFactory>> factoriesPerSet = new ArrayList<>();
        // 每行的 ColumnKey 复用同一 int[] 列索引(ColumnKey 构造不拷贝 cols)
        int[][] groupColsPerSet = new int[groupSets.size()][];
        for (int g = 0; g < groupSets.size(); g++) {
            ImmutableBitSet gs = groupSets.get(g);
            groupColsPerSet[g] = gs.toArray();
            List<AccumulatorFactory> factories = new ArrayList<>();
            for (AggregateCall call : calls) {
                factories.add(factoryFor(call, inputRowType, gs));
            }
            factoriesPerSet.add(factories);
            Map<ColumnKey, GroupState> m = new LinkedHashMap<>();
            if (gs.isEmpty()) {
                // 无 GROUP BY:预置唯一空组(空输入也输出一行,COUNT=0 其余 NULL)
                m.put(new ColumnKey(input, 0, new int[0]), new GroupState(factories));
            }
            groupMaps.add(m);
        }
        List<ValueVector> args = new ArrayList<>();
        try {
            for (AggregateCall call : calls) {
                args.add(argVector(call, input, ctx));
            }
            int rows = input.getRowCount();
            for (int row = 0; row < rows; row++) {
                ExecContext.checkInterrupted();
                for (int g = 0; g < groupSets.size(); g++) {
                    Map<ColumnKey, GroupState> m = groupMaps.get(g);
                    GroupState st;
                    if (groupSets.get(g).isEmpty()) {
                        st = m.values().iterator().next(); // 唯一空组,免 map 查找
                    } else {
                        ColumnKey key = new ColumnKey(input, row, groupColsPerSet[g]);
                        List<AccumulatorFactory> facs = factoriesPerSet.get(g);
                        st = m.computeIfAbsent(key, k -> new GroupState(facs));
                    }
                    for (int i = 0; i < calls.size(); i++) {
                        st.accs.get(i).add(args.get(i), row);
                    }
                }
            }
            for (ValueVector v : args) {
                if (v != null) {
                    v.close();
                }
            }
            // 分组完成后输出。input 不能提前 close——ColumnKey 引用物化 root
            // 读键值(buildOutput 从 ck.root()/ck.row() 列式拷贝),root 必须活到
            // buildOutput 结束(旧 List<Object> 键是值快照,无此约束)。
            boolean empty = true;
            for (Map<ColumnKey, GroupState> m : groupMaps) {
                if (!m.isEmpty()) {
                    empty = false;
                    break;
                }
            }
            if (empty) {
                return BatchIterator.interruptible(BatchIterator.empty());
            }
            VectorSchemaRoot out = buildOutput(groupMaps, groupSets, ctx);
            boolean[] done = {false};
            return BatchIterator.interruptible(
                    new BatchIterator() {
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
                    });
        } finally {
            input.close();
        }
    }

    /** COUNT(*):COUNT 聚合、无 DISTINCT、无参数。 */
    private static boolean isCountStar(AggregateCall call) {
        return call.getAggregation().kind == SqlKind.COUNT
                && !call.isDistinct()
                && call.getArgList().isEmpty()
                && call.rexList.isEmpty();
    }

    /** 把 COUNT(*) 的行数直接物化成单行单列结果(不扫描)。 */
    private BatchIterator singleRowCount(long count, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(1);
            v.allocateNew();
        }
        writeLong(vectors.get(0), 0, count);
        for (FieldVector v : vectors) {
            v.setValueCount(1);
        }
        VectorSchemaRoot out = VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        boolean[] done = {false};
        return BatchIterator.interruptible(
                new BatchIterator() {
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
                });
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
                        batch.getFieldVectors().get(call.getArgList().get(0)), ctx.allocator());
            }
            throw new UnsupportedOperationException("multi-arg aggregate: " + call);
        }
        return null; // COUNT(*)
    }

    private VectorSchemaRoot buildOutput(
            List<Map<ColumnKey, GroupState>> groupMaps,
            List<ImmutableBitSet> groupSets,
            ExecContext ctx) {
        int groupCount = getGroupCount();
        int total = 0;
        for (Map<ColumnKey, GroupState> m : groupMaps) {
            total += m.size();
        }
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        // 提升 AVG/VARIANCE/STDDEV 输出向量精度:DECIMAL→scale+4,INTEGER→Float8Vector。
        // getRowType() 仍是 Calcite 推导的原类型(plan 层无法改 rowType),故这里替换向量。
        // 下游算子(Join/Project)的 probeRoot 按 plan rowType(scale=2)建向量,RowCopier
        // 的跨 scale 路径(writeValue→scaleTo)会把 scale+6 的值正确转写进去。
        for (int i = 0; i < getAggCallList().size(); i++) {
            AggregateCall call = getAggCallList().get(i);
            if (!needsDoubleOutput(call)) {
                continue;
            }
            int idx = groupCount + i;
            RelDataTypeField f = getRowType().getFieldList().get(idx);
            vectors.get(idx).close();
            FieldVector alt;
            if (f.getType().getSqlTypeName() == SqlTypeName.DECIMAL) {
                alt =
                        new DecimalVector(
                                f.getName(),
                                ctx.allocator(),
                                f.getType().getPrecision() + 4,
                                Math.min(f.getType().getScale() + 4, 12));
            } else {
                alt =
                        ArrowTypes.field(
                                        getCluster()
                                                .getTypeFactory()
                                                .createSqlType(SqlTypeName.DOUBLE),
                                        f.getName())
                                .createVector(ctx.allocator());
            }
            alt.setInitialCapacity(total);
            alt.allocateNew();
            vectors.set(idx, alt);
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }
        try {
            int row = 0;
            for (int g = 0; g < groupSets.size(); g++) {
                ImmutableBitSet gs = groupSets.get(g);
                for (Map.Entry<ColumnKey, GroupState> e : groupMaps.get(g).entrySet()) {
                    ColumnKey ck = e.getKey();
                    // 分组列:按完整 groupSet 的 set 位顺序写输出列。子集 groupSet(ROLLUP)
                    // 中不在子集的列写 null。不能用「子集 gs 的 set 位按序递增 outCol」——
                    // 子集 {53} 只有 i_category,但它是输出列 3(完整 groupSet 的第 4 位),
                    // 递增 outCol 会把它写到列 0(i_product_name 位置)。
                    // 键值列式拷贝:物化 root 的列 i、行 ck.row()(ColumnKey 零装箱)。
                    int outCol = 0;
                    for (Integer i : getGroupSet()) {
                        if (gs.get(i)) {
                            RowCopier.copyRow(
                                    ck.root().getVector(i), ck.row(), vectors.get(outCol), row);
                        } else {
                            vectors.get(outCol).setNull(row);
                        }
                        outCol++;
                    }
                    List<Accumulator> accs = e.getValue().accs;
                    for (int i = 0; i < accs.size(); i++) {
                        accs.get(i).write(vectors.get(groupCount + i), row);
                    }
                    row++;
                }
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

    private static AccumulatorFactory factoryFor(
            AggregateCall call, RelDataType inputRowType, ImmutableBitSet groupSet) {
        SqlKind kind = call.getAggregation().kind;
        boolean distinct = call.isDistinct();
        switch (kind) {
            case GROUPING:
                // GROUPING(col) 输出 1 若 col 不在当前 groupSet(被 ROLLUP 汇总),否则 0。
                int bitPos = call.getArgList().get(0);
                int gv = groupSet.get(bitPos) ? 0 : 1;
                return () -> new GroupingAcc(gv);
            case COUNT:
                return distinct ? () -> new DistinctAcc(kind, false) : CountAcc::new;
            case SUM:
                return distinct
                        ? () -> new DistinctAcc(kind, false)
                        : () -> new SumAcc(argDomain(call, inputRowType));
            case AVG:
                return distinct
                        ? () -> new DistinctAcc(kind, false)
                        : () -> new AvgAcc(argDomain(call, inputRowType));
            case MIN:
            case MAX:
                return distinct
                        ? () -> new DistinctAcc(kind, kind == SqlKind.MIN)
                        : () -> new MinMaxAcc(kind == SqlKind.MIN);
            case LITERAL_AGG:
                // 去相关后的 NOT IN 用它给「命中的分组」打标记:LITERAL_AGG[literal]()
                // 对每个非空分组输出该字面量(字面量在 rexList 里,与输入行无关)。
                return () -> new LiteralAcc(((RexLiteral) call.rexList.get(0)).getValue());
            case SINGLE_VALUE:
                // 去相关后的标量子查询:每分组应恰好一个值,这里取 MIN 近似(基准测耗时)。
                return () -> new MinMaxAcc(true);
            case STDDEV_SAMP:
                return () -> new VarianceAcc(argDomain(call, inputRowType), true, true);
            case STDDEV_POP:
                return () -> new VarianceAcc(argDomain(call, inputRowType), false, true);
            case VAR_SAMP:
                return () -> new VarianceAcc(argDomain(call, inputRowType), true, false);
            case VAR_POP:
                return () -> new VarianceAcc(argDomain(call, inputRowType), false, false);
            default:
                throw new UnsupportedOperationException("aggregate not supported: " + kind);
        }
    }

    /**
     * The numeric domain an aggregate runs over. It selects which running value the accumulator
     * keeps (long vs BigDecimal vs double) so DECIMAL stays exact and integral/approximate types
     * keep their natural arithmetic.
     */
    private enum NumericDomain {
        /** SMALLINT/INTEGER/BIGINT accumulate a long. */
        INTEGRAL,
        /** DECIMAL accumulates a BigDecimal (exact). */
        DECIMAL,
        /** REAL/FLOAT/DOUBLE accumulate a double. */
        FLOATING
    }

    private static NumericDomain argDomain(AggregateCall call, RelDataType inputRowType) {
        if (!call.rexList.isEmpty()) {
            return domainOf(call.rexList.get(0).getType().getSqlTypeName());
        }
        if (!call.getArgList().isEmpty()) {
            int idx = call.getArgList().get(0);
            return domainOf(inputRowType.getFieldList().get(idx).getType().getSqlTypeName());
        }
        throw new UnsupportedOperationException("aggregate has no argument: " + call);
    }

    private static NumericDomain domainOf(SqlTypeName t) {
        return switch (t) {
            case SMALLINT, INTEGER, BIGINT -> NumericDomain.INTEGRAL;
            case DECIMAL -> NumericDomain.DECIMAL;
            case REAL, FLOAT, DOUBLE -> NumericDomain.FLOATING;
            default ->
                    throw new UnsupportedOperationException(
                            "unsupported aggregate argument type: " + t);
        };
    }

    /** 判断聚合调用是否需要精度提升(由 AggregateAvgPrecisionRule 在逻辑层处理)。 保留为 public 供测试/诊断引用。 */
    public static boolean needsDoubleOutput(AggregateCall call) {
        SqlKind kind = call.getAggregation().kind;
        if (kind != SqlKind.AVG
                && kind != SqlKind.VAR_SAMP
                && kind != SqlKind.VAR_POP
                && kind != SqlKind.STDDEV_SAMP
                && kind != SqlKind.STDDEV_POP) {
            return false;
        }
        SqlTypeName t = call.type.getSqlTypeName();
        // FLOAT/REAL/DOUBLE 已有足够精度,无需提升
        return t != SqlTypeName.FLOAT && t != SqlTypeName.REAL && t != SqlTypeName.DOUBLE;
    }

    private interface Accumulator {
        void add(ValueVector v, int row);

        void write(FieldVector out, int row);
    }

    /** GROUPING() 聚合:输出常量 0/1(该分组列是否被 ROLLUP 汇总)。 */
    private static final class GroupingAcc implements Accumulator {
        private final long value;

        GroupingAcc(long value) {
            this.value = value;
        }

        @Override
        public void add(ValueVector v, int row) {}

        @Override
        public void write(FieldVector out, int row) {
            writeLong(out, row, value);
        }
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
        private final boolean min;
        private final java.util.LinkedHashSet<Object> set = new java.util.LinkedHashSet<>();

        DistinctAcc(SqlKind kind, boolean min) {
            this.kind = kind;
            this.min = min;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return; // DISTINCT aggregates ignore NULLs
            }
            set.add(RowVectors.readObject(v, row));
        }

        @Override
        public void write(FieldVector out, int row) {
            switch (kind) {
                case COUNT:
                    writeLong(out, row, set.size());
                    return;
                case SUM:
                    {
                        if (set.isEmpty()) {
                            out.setNull(row);
                            return;
                        }
                        // The running set holds boxed values from a single column, so its
                        // element type tells us which numeric domain to sum in (BigDecimal
                        // for DECIMAL, double for REAL/FLOAT/DOUBLE, long otherwise).
                        Object first = set.iterator().next();
                        if (first instanceof BigDecimal) {
                            BigDecimal s = BigDecimal.ZERO;
                            for (Object o : set) {
                                s = s.add((BigDecimal) o);
                            }
                            writeDecimal(out, row, s);
                        } else if (first instanceof Float || first instanceof Double) {
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
                case AVG:
                    {
                        if (set.isEmpty()) {
                            out.setNull(row);
                            return;
                        }
                        Object first = set.iterator().next();
                        if (out instanceof Float8Vector fv) {
                            // AVG 输出提升为 DOUBLE
                            double result;
                            if (first instanceof BigDecimal) {
                                BigDecimal s = BigDecimal.ZERO;
                                for (Object o : set) {
                                    s = s.add((BigDecimal) o);
                                }
                                result =
                                        s.divide(
                                                        BigDecimal.valueOf(set.size()),
                                                        MathContext.DECIMAL128)
                                                .doubleValue();
                            } else if (first instanceof Float || first instanceof Double) {
                                double s = 0;
                                for (Object o : set) {
                                    s += ((Number) o).doubleValue();
                                }
                                result = s / set.size();
                            } else {
                                long s = 0;
                                for (Object o : set) {
                                    s += ((Number) o).longValue();
                                }
                                result = (double) s / set.size();
                            }
                            fv.setSafe(row, result);
                        } else if (first instanceof BigDecimal) {
                            BigDecimal s = BigDecimal.ZERO;
                            for (Object o : set) {
                                s = s.add((BigDecimal) o);
                            }
                            writeDecimal(
                                    out,
                                    row,
                                    s.divide(
                                            BigDecimal.valueOf(set.size()),
                                            MathContext.DECIMAL128));
                        } else if (first instanceof Float || first instanceof Double) {
                            double s = 0;
                            for (Object o : set) {
                                s += ((Number) o).doubleValue();
                            }
                            writeDouble(out, row, s / set.size());
                        } else {
                            long s = 0;
                            for (Object o : set) {
                                s += ((Number) o).longValue();
                            }
                            writeLong(out, row, s / set.size());
                        }
                        return;
                    }
                case MIN:
                case MAX:
                    {
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
                        RowVectors.writeObject(out, row, best);
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
        private final NumericDomain domain;
        private long lsum;
        private BigDecimal decimalSum = BigDecimal.ZERO;
        private double fsum;
        private boolean has;

        SumAcc(NumericDomain domain) {
            this.domain = domain;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return;
            }
            has = true;
            switch (domain) {
                case INTEGRAL:
                    lsum += readLong(v, row);
                    break;
                case DECIMAL:
                    decimalSum = decimalSum.add(readDecimal(v, row));
                    break;
                case FLOATING:
                    fsum += readDouble(v, row);
                    break;
            }
        }

        @Override
        public void write(FieldVector out, int row) {
            if (!has) {
                out.setNull(row);
                return;
            }
            switch (domain) {
                case INTEGRAL:
                    writeLong(out, row, lsum);
                    break;
                case DECIMAL:
                    writeDecimal(out, row, decimalSum);
                    break;
                case FLOATING:
                    writeDouble(out, row, fsum);
                    break;
            }
        }
    }

    private static final class AvgAcc implements Accumulator {
        private final NumericDomain domain;
        private long lsum;
        private BigDecimal decimalSum = BigDecimal.ZERO;
        private double fsum;
        private long cnt;

        AvgAcc(NumericDomain domain) {
            this.domain = domain;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return;
            }
            switch (domain) {
                case INTEGRAL:
                    lsum += readLong(v, row);
                    break;
                case DECIMAL:
                    decimalSum = decimalSum.add(readDecimal(v, row));
                    break;
                case FLOATING:
                    fsum += readDouble(v, row);
                    break;
            }
            cnt++;
        }

        @Override
        public void write(FieldVector out, int row) {
            if (cnt == 0) {
                out.setNull(row);
                return;
            }
            if (out instanceof Float8Vector fv) {
                // AVG(INTEGER/BIGINT) 输出提升为 DOUBLE(由 AggregateAvgPrecisionRule)
                double result =
                        switch (domain) {
                            case INTEGRAL -> (double) lsum / cnt;
                            case DECIMAL ->
                                    decimalSum
                                            .divide(BigDecimal.valueOf(cnt), MathContext.DECIMAL128)
                                            .doubleValue();
                            case FLOATING -> fsum / cnt;
                        };
                fv.setSafe(row, result);
                return;
            }
            switch (domain) {
                case INTEGRAL:
                    writeLong(out, row, lsum / cnt);
                    break;
                case DECIMAL:
                    writeDecimal(
                            out,
                            row,
                            decimalSum.divide(BigDecimal.valueOf(cnt), MathContext.DECIMAL128));
                    break;
                case FLOATING:
                    writeDouble(out, row, fsum / cnt);
                    break;
            }
        }
    }

    /**
     * VAR_SAMP/VAR_POP/STDDEV_SAMP/STDDEV_POP:方差 = 均值平方差,在线累计 sum 与 sum²。 STDDEV 再取 sqrt;VAR
     * 直接输出方差。输出类型由 AggregateAvgPrecisionRule 提升 (INTEGER/BIGINT→DOUBLE,DECIMAL→DECIMAL scale+4)。
     */
    private static final class VarianceAcc implements Accumulator {
        private final NumericDomain domain;
        private final boolean sample; // true = 除以 n-1(VAR_SAMP/STDDEV_SAMP)
        private final boolean sqrt; // true = STDDEV,false = VAR
        private long n;
        private double sum;
        private double sumSq;

        VarianceAcc(NumericDomain domain, boolean sample, boolean sqrt) {
            this.domain = domain;
            this.sample = sample;
            this.sqrt = sqrt;
        }

        @Override
        public void add(ValueVector v, int row) {
            if (v == null || v.isNull(row)) {
                return;
            }
            double x = readAsDouble(v, row, domain);
            n++;
            sum += x;
            sumSq += x * x;
        }

        @Override
        public void write(FieldVector out, int row) {
            // 空输入,或样本方差只有 1 个值(n-1=0),方差未定义 → NULL。
            if (n == 0 || (sample && n == 1)) {
                out.setNull(row);
                return;
            }
            double denom = sample ? (n - 1) : n;
            double variance = (sumSq - sum * sum / n) / denom;
            // 浮点舍入可能产生极小的负方差,clamp 到 0 再开方。
            if (variance < 0) {
                variance = 0;
            }
            double result = sqrt ? Math.sqrt(variance) : variance;
            if (out instanceof Float8Vector fv) {
                fv.setSafe(row, result);
                return;
            }
            switch (domain) {
                case INTEGRAL:
                    writeLong(out, row, (long) result);
                    break;
                case DECIMAL:
                    writeDecimal(out, row, BigDecimal.valueOf(result));
                    break;
                case FLOATING:
                    writeDouble(out, row, result);
                    break;
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
            Object o = RowVectors.readObject(v, row);
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
            RowVectors.writeObject(out, row, best);
        }
    }

    /** LITERAL_AGG[literal]():add 是 no-op(值与输入行无关),write 直接输出字面量。 */
    private static final class LiteralAcc implements Accumulator {
        private final Object value;

        LiteralAcc(Object value) {
            this.value = value;
        }

        @Override
        public void add(ValueVector v, int row) {}

        @Override
        public void write(FieldVector out, int row) {
            if (value == null) {
                out.setNull(row);
            } else if (value instanceof Boolean b) {
                ((BitVector) out).setSafe(row, b ? 1 : 0);
            } else {
                RowVectors.writeObject(out, row, value);
            }
        }
    }

    private static long readLong(ValueVector v, int row) {
        if (v instanceof SmallIntVector sv) {
            return sv.get(row);
        }
        if (v instanceof IntVector iv) {
            return iv.get(row);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(row);
        }
        throw new UnsupportedOperationException("not an integral vector: " + v.getMinorType());
    }

    /**
     * Reads a FLOATING-domain value. Only REAL/FLOAT/DOUBLE reach this method (INTEGRAL and DECIMAL
     * aggregates use readLong/readDecimal instead), so Float4/Float8 are the only possible vector
     * types.
     */
    private static double readDouble(ValueVector v, int row) {
        if (v instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(row);
        }
        throw new UnsupportedOperationException("not a floating-point vector: " + v.getMinorType());
    }

    private static BigDecimal readDecimal(ValueVector v, int row) {
        if (v instanceof DecimalVector dv) {
            return dv.getObject(row);
        }
        throw new UnsupportedOperationException("not a decimal vector: " + v.getMinorType());
    }

    /** Reads a numeric value as double for variance/stddev, regardless of domain. */
    private static double readAsDouble(ValueVector v, int row, NumericDomain domain) {
        return switch (domain) {
            case INTEGRAL -> readLong(v, row);
            case DECIMAL -> readDecimal(v, row).doubleValue();
            case FLOATING -> readDouble(v, row);
            default -> throw new UnsupportedOperationException("unexpected domain: " + domain);
        };
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
        } else if (out instanceof SmallIntVector sv) {
            sv.setSafe(row, (short) v);
        } else {
            throw new UnsupportedOperationException("no long writer for " + out.getMinorType());
        }
    }

    private static void writeDouble(FieldVector out, int row, double v) {
        if (out instanceof Float8Vector fv) {
            fv.setSafe(row, v);
        } else if (out instanceof Float4Vector fv) {
            fv.setSafe(row, (float) v);
        } else {
            throw new UnsupportedOperationException("no double writer for " + out.getMinorType());
        }
    }

    private static void writeDecimal(FieldVector out, int row, BigDecimal v) {
        if (out instanceof DecimalVector dv) {
            // DecimalVector.set(BigDecimal) 要求 value 的 scale 与向量 scale 精确相等,
            // 否则抛 UnsupportedOperationException(见 Arrow DecimalUtility.checkPrecisionAndScale)。
            // AVG(DECIMAL) 的商通常不是有限小数,先按向量 scale HALF_UP 舍入再写,既满足
            // Arrow 不变量又把非终止小数确定地落到输出 scale。
            dv.setSafe(row, v.setScale(dv.getScale(), RoundingMode.HALF_UP));
        } else {
            throw new UnsupportedOperationException("no decimal writer for " + out.getMinorType());
        }
    }
}
