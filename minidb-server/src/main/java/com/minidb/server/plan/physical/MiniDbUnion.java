package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class MiniDbUnion extends Union implements MiniDbRel {

    public MiniDbUnion(
            RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MiniDbUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<BatchIterator> iterators = new ArrayList<>();
        List<VectorSchemaRoot> batches = new ArrayList<>();
        int total = 0;
        for (RelNode input : getInputs()) {
            BatchIterator it = ((MiniDbRel) input).execute(ctx);
            iterators.add(it);
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                batches.add(b);
                total += b.getRowCount();
            }
        }
        if (batches.isEmpty()) {
            for (BatchIterator it : iterators) {
                it.close();
            }
            return BatchIterator.interruptible(BatchIterator.empty());
        }
        // copy BEFORE closing inputs: Project/Filter own their batches and
        // release them on close
        VectorSchemaRoot out = mergeBatches(batches, total, ctx);
        for (BatchIterator it : iterators) {
            it.close();
        }

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

    private VectorSchemaRoot mergeBatches(
            List<VectorSchemaRoot> batches, int total, ExecContext ctx) {
        boolean distinct = !all;
        // 去重 key 用 ColumnKey(列式 hash/equals),避免每行每列装箱成 List<Object>。
        int[] allCols = new int[getRowType().getFieldCount()];
        for (int i = 0; i < allCols.length; i++) {
            allCols[i] = i;
        }
        Set<ColumnKey> seen = distinct ? new LinkedHashSet<>() : null;
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }
        VectorSchemaRoot out = VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        try {
            int dst = 0;
            for (VectorSchemaRoot batch : batches) {
                if (distinct) {
                    // 去重:逐行检查(跨批行号,无法整批批量),保留行号再拷
                    int[] kept = new int[batch.getRowCount()];
                    int keptCnt = 0;
                    for (int i = 0; i < batch.getRowCount(); i++) {
                        if (seen.add(new ColumnKey(batch, i, allCols))) {
                            kept[keptCnt++] = i;
                        }
                    }
                    RowCopier.copyRowsByIndex(batch, kept, 0, out, dst, keptCnt);
                    dst += keptCnt;
                } else {
                    // UNION ALL:整批连续批量拷贝(固定宽走无检查 copyFrom)
                    RowCopier.copyRows(batch, 0, out, dst, batch.getRowCount());
                    dst += batch.getRowCount();
                }
            }
            // setValueCount BEFORE of() so the root's rowCount picks it up
            // (of() derives rowCount from the first vector's valueCount)
            for (FieldVector v : vectors) {
                v.setValueCount(dst);
            }
            return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        } catch (RuntimeException e) {
            out.close();
            throw e;
        }
    }
}
