package com.minidb.server.plan;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.storage.ArrowTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;

public class MiniDbModify extends TableModify implements MiniDbRel {

    private long affected;

    public MiniDbModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                        org.apache.calcite.prepare.Prepare.CatalogReader catalogReader,
                        RelNode input, Operation operation,
                        List<String> updateColumnList,
                        List<org.apache.calcite.rex.RexNode> sourceExpressionList,
                        boolean flattened) {
        super(cluster, traitSet, table, catalogReader, input, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbModify(getCluster(), traitSet, table, getCatalogReader(),
                sole(inputs), getOperation(), getUpdateColumnList(),
                getSourceExpressionList(), isFlattened());
    }

    public long affected() {
        return affected;
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<String> qualified = table.getQualifiedName();
        String tableName = qualified.get(qualified.size() - 1);
        ArrowTable target = ctx.storage().getTable(tableName);
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        if (getOperation() == Operation.INSERT) {
            appendRows(ctx, target, input, tableName);
        } else {
            rewriteTable(ctx, target, input, tableName);
        }
        return BatchIterator.empty();
    }

    private void appendRows(ExecContext ctx, ArrowTable target, BatchIterator input,
                            String tableName) {
        affected = 0;
        while (input.hasNext()) {
            VectorSchemaRoot batch = input.next();
            // copy rows into a table-owned root; never take ownership of
            // batches that may belong to another table (Scan) or the iterator
            VectorSchemaRoot copy = target.newBatchRoot();
            copy.allocateNew();
            for (int i = 0; i < batch.getRowCount(); i++) {
                RowCopier.copyRow(batch, i, copy, i);
            }
            copy.setRowCount(batch.getRowCount());
            affected += batch.getRowCount();
            target.appendBatch(copy);
        }
        input.close();
        ctx.storage().markDirty(tableName);
    }

    /**
     * UPDATE and DELETE must remove the matched rows from the table; the input
     * only produces the matched rows, so we rebuild the table content, keeping
     * unmatched rows and replacing (UPDATE) or dropping (DELETE) matched ones.
     */
    private void rewriteTable(ExecContext ctx, ArrowTable target, BatchIterator input,
                              String tableName) {
        int numTableCols = target.schema().columns().size();
        List<String> updateCols = getOperation() == Operation.UPDATE
                ? getUpdateColumnList() : List.of();
        // Materialize the matched rows into a root we own, then close the input
        // (its batches belong to the operators and the table scan).
        VectorSchemaRoot matched = materializeInput(input, ctx);
        input.close();
        if (matched == null || matched.getRowCount() == 0) {
            if (matched != null) {
                matched.close();
            }
            affected = 0;
            return; // nothing matched, table unchanged
        }
        // One representative matched row per original full-row value: identical
        // original rows always produce identical updated rows.
        Map<List<Object>, Integer> matchRow = new HashMap<>();
        for (int i = 0; i < matched.getRowCount(); i++) {
            matchRow.putIfAbsent(rowKey(matched, i, numTableCols), i);
        }
        List<VectorSchemaRoot> oldBatches = target.batches();
        List<VectorSchemaRoot> newBatches = new ArrayList<>();
        affected = 0;
        for (VectorSchemaRoot old : oldBatches) {
            VectorSchemaRoot nb = target.newBatchRoot();
            nb.allocateNew();
            int kept = 0;
            for (int i = 0; i < old.getRowCount(); i++) {
                Integer srcRow = matchRow.get(rowKey(old, i, numTableCols));
                if (srcRow == null) {
                    RowCopier.copyRow(old, i, nb, kept);
                    kept++;
                    continue;
                }
                affected++;
                if (getOperation() == Operation.DELETE) {
                    continue; // matched row is removed
                }
                // UPDATE: copy the original table columns, then overwrite the
                // updated ones with the trailing source values from the input.
                for (int j = 0; j < numTableCols; j++) {
                    nb.getFieldVectors().get(j)
                            .copyFromSafe(srcRow, kept, matched.getFieldVectors().get(j));
                }
                for (int j = 0; j < updateCols.size(); j++) {
                    int colIndex = target.schema().columnIndex(updateCols.get(j));
                    RowCopier.writeValue(
                            nb.getFieldVectors().get(colIndex), kept,
                            matched.getFieldVectors().get(numTableCols + j), srcRow);
                }
                kept++;
            }
            if (kept > 0) {
                nb.setRowCount(kept);
                newBatches.add(nb);
            } else {
                nb.close();
            }
        }
        target.replaceBatches(newBatches);
        for (VectorSchemaRoot old : oldBatches) {
            old.close();
        }
        matched.close();
        ctx.storage().markDirty(tableName);
    }

    private VectorSchemaRoot materializeInput(BatchIterator input, ExecContext ctx) {
        VectorSchemaRoot merged = null;
        int dst = 0;
        while (input.hasNext()) {
            VectorSchemaRoot batch = input.next();
            if (merged == null) {
                merged = VectorSchemaRoot.create(batch.getSchema(), ctx.allocator());
                merged.allocateNew();
            }
            for (int i = 0; i < batch.getRowCount(); i++) {
                RowCopier.copyRow(batch, i, merged, dst);
                dst++;
            }
        }
        if (merged != null) {
            merged.setRowCount(dst);
        }
        return merged;
    }

    private static List<Object> rowKey(VectorSchemaRoot root, int row, int numCols) {
        List<Object> key = new ArrayList<>(numCols);
        for (int j = 0; j < numCols; j++) {
            key.add(root.getVector(j).getObject(row));
        }
        return key;
    }

}
