package com.minidb.server.plan;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
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
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;

/**
 * INTERSECT / EXCEPT. Multiset semantics: rows are normalized to
 * {@code List<Object>} keys; each input contributes per-key counts.
 * INTERSECT keeps min(count) across inputs, EXCEPT keeps
 * max(0, count(first) - sum(rest)); with all=false each surviving key is
 * emitted once, with all=true it is emitted that many times. Output order
 * follows first-seen order of the first input.
 */
public class MiniDbSetOp extends SetOp implements MiniDbRel {

    public MiniDbSetOp(RelOptCluster cluster, RelTraitSet traitSet,
                       List<RelNode> inputs, SqlKind kind, boolean all) {
        super(cluster, traitSet, inputs, kind, all);
    }

    public boolean isIntersect() {
        return kind == SqlKind.INTERSECT;
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MiniDbSetOp(getCluster(), traitSet, inputs, kind, all);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<Map<List<Object>, Long>> counts = new ArrayList<>();
        List<BatchIterator> iterators = new ArrayList<>();
        for (RelNode input : getInputs()) {
            BatchIterator it = ((MiniDbRel) input).execute(ctx);
            iterators.add(it);
            Map<List<Object>, Long> m = new LinkedHashMap<>();
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                for (int i = 0; i < b.getRowCount(); i++) {
                    m.merge(rowKey(b, i), 1L, Long::sum);
                }
            }
            counts.add(m);
        }
        Map<List<Object>, Long> base = counts.get(0);
        List<List<Object>> keys = new ArrayList<>();
        List<Long> times = new ArrayList<>();
        for (Map.Entry<List<Object>, Long> e : base.entrySet()) {
            long n = e.getValue();
            for (int i = 1; i < counts.size(); i++) {
                long c = counts.get(i).getOrDefault(e.getKey(), 0L);
                n = isIntersect() ? Math.min(n, c) : n - c;
            }
            if (n > 0) {
                keys.add(e.getKey());
                times.add(all ? n : 1L);
            }
        }
        for (BatchIterator it : iterators) {
            it.close();
        }
        if (keys.isEmpty()) {
            return BatchIterator.empty();
        }
        VectorSchemaRoot out = buildOutput(keys, times, ctx);

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

    private VectorSchemaRoot buildOutput(List<List<Object>> keys, List<Long> times,
                                         ExecContext ctx) {
        long total = 0;
        for (long t : times) {
            total += t;
        }
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity((int) Math.min(total, Integer.MAX_VALUE));
            v.allocateNew();
        }
        int dst = 0;
        for (int k = 0; k < keys.size(); k++) {
            List<Object> key = keys.get(k);
            for (long t = 0; t < times.get(k); t++) {
                for (int c = 0; c < key.size(); c++) {
                    writeObject(vectors.get(c), dst, key.get(c));
                }
                dst++;
            }
        }
        for (FieldVector v : vectors) {
            v.setValueCount(dst);
        }
        // of() must come after setValueCount: rowCount derives from valueCount
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    private static List<Object> rowKey(VectorSchemaRoot batch, int row) {
        List<Object> key = new ArrayList<>(batch.getFieldVectors().size());
        for (ValueVector v : batch.getFieldVectors()) {
            key.add(readObject(v, row));
        }
        return key;
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
                "cannot deduplicate column type: " + v.getMinorType());
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
