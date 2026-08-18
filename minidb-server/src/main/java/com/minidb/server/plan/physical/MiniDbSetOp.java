package com.minidb.server.plan.physical;

import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;

/**
 * INTERSECT / EXCEPT. Multiset semantics: each input is materialized into a
 * single columnar root, then counted with {@link ColumnKey} (no per-cell
 * boxing). INTERSECT keeps min(count) across inputs, EXCEPT keeps
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
        int[] allCols = new int[getRowType().getFieldCount()];
        for (int i = 0; i < allCols.length; i++) {
            allCols[i] = i;
        }
        List<VectorSchemaRoot> roots = new ArrayList<>();
        List<Map<ColumnKey, Long>> counts = new ArrayList<>();
        try {
            for (RelNode input : getInputs()) {
                VectorSchemaRoot root = RowVectors.materializeToRoot(input, ctx);
                roots.add(root);
                Map<ColumnKey, Long> m = new LinkedHashMap<>();
                for (int i = 0; i < root.getRowCount(); i++) {
                    m.merge(new ColumnKey(root, i, allCols), 1L, Long::sum);
                }
                counts.add(m);
            }
            Map<ColumnKey, Long> base = counts.get(0);
            List<ColumnKey> keys = new ArrayList<>();
            List<Long> times = new ArrayList<>();
            for (Map.Entry<ColumnKey, Long> e : base.entrySet()) {
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
            if (keys.isEmpty()) {
                for (VectorSchemaRoot root : roots) {
                    root.close();
                }
                return BatchIterator.interruptible(BatchIterator.empty());
            }
            VectorSchemaRoot out = buildOutput(keys, times, allCols, ctx);
            // 数据已 copy 到 out,roots 可释放。
            for (VectorSchemaRoot root : roots) {
                root.close();
            }
            boolean[] done = {false};
            return BatchIterator.interruptible(new BatchIterator() {
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
        } catch (RuntimeException e) {
            for (VectorSchemaRoot root : roots) {
                root.close();
            }
            throw e;
        }
    }

    private VectorSchemaRoot buildOutput(List<ColumnKey> keys, List<Long> times,
                                         int[] allCols, ExecContext ctx) {
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
            ColumnKey key = keys.get(k);
            VectorSchemaRoot src = key.root();
            int srcRow = key.row();
            for (long t = 0; t < times.get(k); t++) {
                for (int c = 0; c < allCols.length; c++) {
                    RowCopier.copyRow(src.getVector(allCols[c]), srcRow, vectors.get(c), dst);
                }
                dst++;
            }
        }
        for (FieldVector v : vectors) {
            v.setValueCount(dst);
        }
        // of() must come after setValueCount: rowCount derives from valueCount.
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }
}
