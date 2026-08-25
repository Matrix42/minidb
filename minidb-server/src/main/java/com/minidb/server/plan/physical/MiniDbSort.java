package com.minidb.server.plan.physical;

import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.exec.ValueComparators;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.algorithm.sort.CompositeVectorComparator;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class MiniDbSort extends Sort implements MiniDbRel {

    public MiniDbSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                      RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput,
                     RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new MiniDbSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        // materialize all input rows into a single root
        List<VectorSchemaRoot> batches = new ArrayList<>();
        int total = 0;
        while (input.hasNext()) {
            VectorSchemaRoot b = input.next();
            batches.add(b);
            total += b.getRowCount();
        }
        VectorSchemaRoot materialized = batches.isEmpty()
                ? null : mergeBatches(batches, total, ctx);
        // close input only AFTER copying: Filter/Project own their batches
        input.close();
        if (materialized == null) {
            return BatchIterator.interruptible(BatchIterator.empty());
        }

        int rows = materialized.getRowCount();
        IntVector indices = new IntVector("sort_indices", ctx.allocator());
        indices.allocateNew(rows);
        // allocateNew 只分配 buffer 不设 valueCount,而 IndexSorter.quickSort 靠
        // getValueCount() 定排序范围,必须在这里显式设置,否则排序空转。
        indices.setValueCount(rows);
        VectorValueComparator<ValueVector> comparator = buildComparator(materialized);
        try {
            // Arrow 原生索引快排(arrow-algorithm),比较走 ValueComparators 的列式实现。
            new IndexSorter<ValueVector>().sort(materialized.getVector(0), indices, comparator);

            int offsetRows = literalInt(offset, 0);
            int fetchRows = literalInt(fetch, Integer.MAX_VALUE);
            int start = Math.min(offsetRows, rows);
            int end = Math.min(rows, start + fetchRows);

            VectorSchemaRoot out = VectorSchemaRoot.create(materialized.getSchema(), ctx.allocator());
            int outRows = end - start;
            // 预分配 outRows,按排序后行号批量拷贝(固定宽走无检查 copyFrom)
            for (FieldVector v : out.getFieldVectors()) {
                v.setInitialCapacity(outRows);
                v.allocateNew();
            }
            int[] order = new int[rows];
            for (int i = 0; i < rows; i++) {
                order[i] = indices.get(i);
            }
            RowCopier.copyRowsByIndex(materialized, order, start, out, 0, outRows);
            out.setRowCount(outRows);
            materialized.close();

            VectorSchemaRoot single = out;
            boolean emitted = false;
            return BatchIterator.interruptible(new BatchIterator() {
                boolean done = emitted;

                @Override
                public boolean hasNext() {
                    return !done;
                }

                @Override
                public VectorSchemaRoot next() {
                    done = true;
                    return single;
                }

                @Override
                public void close() {
                    single.close();
                }
            });
        } finally {
            indices.close();
        }
    }

    private static int literalInt(RexNode node, int defaultValue) {
        if (node instanceof RexLiteral literal) {
            return literal.getValueAs(BigDecimal.class).intValue();
        }
        return defaultValue;
    }

    /** 多列比较器:每列一个「null 恒排最后、desc 反转」的比较器,列式比较走 ValueComparators。 */
    @SuppressWarnings("unchecked")
    private VectorValueComparator<ValueVector> buildComparator(VectorSchemaRoot root) {
        List<RelFieldCollation> collations = collation.getFieldCollations();
        VectorValueComparator<ValueVector>[] inner = new VectorValueComparator[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            RelFieldCollation fc = collations.get(i);
            boolean desc = fc.getDirection() == RelFieldCollation.Direction.DESCENDING
                    || fc.getDirection() == RelFieldCollation.Direction.STRICTLY_DESCENDING;
            SortComparator sc = new SortComparator(desc);
            sc.attachVector(root.getVector(fc.getFieldIndex()));
            inner[i] = sc;
        }
        return new CompositeVectorComparator(inner);
    }

    /** 包装列式比较,实现「null 恒排最后 + 可选 desc」;非 null 比较走 ValueComparators(无符号字节序)。 */
    private static final class SortComparator extends VectorValueComparator<ValueVector> {
        private final boolean descending;

        SortComparator(boolean descending) {
            this.descending = descending;
        }

        @Override
        public int compare(int i1, int i2) {
            boolean null1 = vector1.isNull(i1);
            boolean null2 = vector2.isNull(i2);
            int cmp;
            if (null1 || null2) {
                cmp = (null1 && null2) ? 0 : (null1 ? 1 : -1); // null last
            } else {
                cmp = ValueComparators.compare(vector1, i1, vector2, i2);
            }
            return descending ? -cmp : cmp;
        }

        @Override
        public int compareNotNull(int i1, int i2) {
            int cmp = ValueComparators.compare(vector1, i1, vector2, i2);
            return descending ? -cmp : cmp;
        }

        @Override
        public VectorValueComparator<ValueVector> createNew() {
            return new SortComparator(descending);
        }
    }

    private VectorSchemaRoot mergeBatches(List<VectorSchemaRoot> batches, int total,
                                          ExecContext ctx) {
        if (batches.isEmpty()) {
            // empty schema: rely on row type; build from first batch if present else null
            throw new IllegalArgumentException("sort received no input batches");
        }
        VectorSchemaRoot merged = VectorSchemaRoot.create(batches.get(0).getSchema(),
                ctx.allocator());
        // 预分配 total(mergeBatches 参数,调用方已累计),批量列拷贝的前提
        for (FieldVector v : merged.getFieldVectors()) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }
        int dst = 0;
        for (VectorSchemaRoot batch : batches) {
            RowCopier.copyRows(batch, 0, merged, dst, batch.getRowCount());
            dst += batch.getRowCount();
        }
        merged.setRowCount(dst);
        return merged;
    }
}
