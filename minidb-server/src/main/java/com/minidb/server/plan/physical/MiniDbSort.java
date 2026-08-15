package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.exec.ValueComparators;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
            return BatchIterator.empty();
        }

        int rows = materialized.getRowCount();
        List<Integer> order = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            order.add(i);
        }
        Comparator<Integer> cmp = buildComparator(materialized);
        order.sort(cmp);

        int offsetRows = literalInt(offset, 0);
        int fetchRows = literalInt(fetch, Integer.MAX_VALUE);
        int start = Math.min(offsetRows, rows);
        int end = Math.min(rows, start + fetchRows);

        VectorSchemaRoot out = VectorSchemaRoot.create(materialized.getSchema(), ctx.allocator());
        out.allocateNew();
        int dst = 0;
        for (int i = start; i < end; i++) {
            RowCopier.copyRow(materialized, order.get(i), out, dst++);
        }
        out.setRowCount(dst);
        materialized.close();

        VectorSchemaRoot single = out;
        boolean emitted = false;
        return new BatchIterator() {
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
        };
    }

    private static int literalInt(RexNode node, int defaultValue) {
        if (node instanceof RexLiteral literal) {
            return literal.getValueAs(BigDecimal.class).intValue();
        }
        return defaultValue;
    }

    private Comparator<Integer> buildComparator(VectorSchemaRoot root) {
        Comparator<Integer> result = null;
        for (RelFieldCollation fc : collation.getFieldCollations()) {
            int field = fc.getFieldIndex();
            boolean desc = fc.getDirection() == RelFieldCollation.Direction.DESCENDING
                    || fc.getDirection() == RelFieldCollation.Direction.STRICTLY_DESCENDING;
            Comparator<Integer> one = (a, b) -> compareCells(root, field, a, b);
            if (desc) {
                one = one.reversed();
            }
            result = result == null ? one : result.thenComparing(one);
        }
        return result == null ? Comparator.comparingInt(i -> i) : result;
    }

    private static int compareCells(VectorSchemaRoot root, int field, int rowA, int rowB) {
        ValueVector v = root.getVector(field);
        boolean nullA = v.isNull(rowA);
        boolean nullB = v.isNull(rowB);
        if (nullA && nullB) {
            return 0;
        }
        if (nullA) {
            return 1; // nulls last
        }
        if (nullB) {
            return -1;
        }
        // 列式比较:VarChar/VarBinary 走字节比较,避免每比较一次分配两个 String/BigDecimal 对象。
        return ValueComparators.compare(v, rowA, v, rowB);
    }

    private VectorSchemaRoot mergeBatches(List<VectorSchemaRoot> batches, int total,
                                          ExecContext ctx) {
        if (batches.isEmpty()) {
            // empty schema: rely on row type; build from first batch if present else null
            throw new IllegalArgumentException("sort received no input batches");
        }
        VectorSchemaRoot merged = VectorSchemaRoot.create(batches.get(0).getSchema(),
                ctx.allocator());
        merged.allocateNew();
        int dst = 0;
        for (VectorSchemaRoot batch : batches) {
            for (int i = 0; i < batch.getRowCount(); i++) {
                RowCopier.copyRow(batch, i, merged, dst++);
            }
        }
        merged.setRowCount(dst);
        return merged;
    }
}
