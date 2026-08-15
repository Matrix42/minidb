package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ForeignKey;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.storage.ArrowTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        int n = qualified.size();
        String tableName = qualified.get(n - 1);
        // size>=3: [minidb, other, t] → schema is second-to-last.
        // size==2: [minidb, t] (promoted table) → bare name resolves via ctx's
        // current schema.
        String schemaName = n >= 3 ? qualified.get(n - 2) : null;
        ArrowTable target = schemaName != null
                ? ctx.getTable(schemaName, tableName)
                : ctx.getTable(tableName);
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        if (getOperation() == Operation.INSERT) {
            appendRows(ctx, target, input, schemaName, tableName);
        } else {
            rewriteTable(ctx, target, input, schemaName, tableName);
        }
        return BatchIterator.empty();
    }

    private void appendRows(ExecContext ctx, ArrowTable target, BatchIterator input,
                            String schemaName, String tableName) {
        affected = 0;
        try {
            while (input.hasNext()) {
                VectorSchemaRoot batch = input.next();
                validateInsert(ctx, target, batch);
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
        } finally {
            input.close();
        }
        if (schemaName != null) {
            ctx.markDirty(schemaName, tableName);
        } else {
            ctx.markDirty(tableName);
        }
    }

    /** INSERT 前的约束校验:NOT NULL + 主键/唯一冲突 + 外键引用存在。 */
    private void validateInsert(ExecContext ctx, ArrowTable target, VectorSchemaRoot batch) {
        TableSchema schema = target.schema();
        for (ColumnMeta column : schema.columns()) {
            if (!column.nullable()) {
                int idx = schema.columnIndex(column.name());
                for (int i = 0; i < batch.getRowCount(); i++) {
                    if (batch.getVector(idx).isNull(i)) {
                        throw new IllegalArgumentException(
                                "null value in column \"" + column.name()
                                        + "\" violates not-null constraint");
                    }
                }
            }
        }
        if (!schema.primaryKey().isEmpty()) {
            validateUnique(target, batch, schema.primaryKey(), "primary key");
        }
        for (List<String> unique : schema.uniqueKeys()) {
            validateUnique(target, batch, unique, "unique");
        }
        validateForeignKeys(ctx, target, batch);
    }

    /** 外键 INSERT 校验:child 行的外键列值必须存在于引用表(含 null 的键不校验)。 */
    private void validateForeignKeys(ExecContext ctx, ArrowTable target, VectorSchemaRoot batch) {
        for (ForeignKey fk : target.schema().foreignKeys()) {
            ArrowTable refTable = ctx.getTable(fk.refSchema(), fk.refTable());
            List<String> refColumns = fk.refColumns().isEmpty()
                    ? refTable.schema().primaryKey()
                    : fk.refColumns();
            List<Integer> childIdx = columnIndexes(target.schema(), fk.columns());
            List<Integer> refIdx = columnIndexes(refTable.schema(), refColumns);
            Set<List<Object>> refKeys = new HashSet<>();
            for (VectorSchemaRoot b : refTable.batches()) {
                for (int i = 0; i < b.getRowCount(); i++) {
                    List<Object> key = keyOf(b, i, refIdx);
                    if (key != null) {
                        refKeys.add(key);
                    }
                }
            }
            for (int i = 0; i < batch.getRowCount(); i++) {
                List<Object> key = keyOf(batch, i, childIdx);
                if (key != null && !refKeys.contains(key)) {
                    throw new IllegalArgumentException(
                            "foreign key violation: " + fk.columns()
                                    + " references " + fk.refTable() + "." + refColumns);
                }
            }
        }
    }

    private static List<Integer> columnIndexes(TableSchema schema, List<String> names) {
        List<Integer> idxs = new ArrayList<>(names.size());
        for (String name : names) {
            idxs.add(schema.columnIndex(name));
        }
        return idxs;
    }

    /**
     * DELETE 的外键 RESTRICT 校验:被删除行的主键不能仍被其它表引用。只处理 child 外键
     * 引用本表主键的常见情况(引用非主键唯一列暂不校验);UPDATE 本表被引用列(如改主键)
     * 的外键校验暂未覆盖。
     */
    private void validateDeleteRestrict(ExecContext ctx, ArrowTable target, VectorSchemaRoot matched) {
        TableSchema schema = target.schema();
        if (schema.primaryKey().isEmpty()) {
            return;
        }
        List<Integer> pkIdx = columnIndexes(schema, schema.primaryKey());
        Set<List<Object>> deletedKeys = new HashSet<>();
        for (int i = 0; i < matched.getRowCount(); i++) {
            List<Object> key = keyOf(matched, i, pkIdx);
            if (key != null) {
                deletedKeys.add(key);
            }
        }
        MiniDbCatalog catalog = ctx.storage().catalog();
        for (String schemaName : catalog.schemaNames()) {
            for (String tableName : catalog.tableNames(schemaName)) {
                TableSchema childSchema = catalog.getTable(schemaName, tableName);
                for (ForeignKey fk : childSchema.foreignKeys()) {
                    if (!fk.refSchema().equalsIgnoreCase(schema.schemaName())
                            || !fk.refTable().equalsIgnoreCase(schema.name())) {
                        continue;
                    }
                    List<String> refColumns = fk.refColumns().isEmpty()
                            ? schema.primaryKey() : fk.refColumns();
                    if (!refColumns.equals(schema.primaryKey())) {
                        continue; // 引用非主键列,暂不校验
                    }
                    ArrowTable childTable = ctx.getTable(schemaName, tableName);
                    List<Integer> childIdx = columnIndexes(childSchema, fk.columns());
                    for (VectorSchemaRoot b : childTable.batches()) {
                        for (int i = 0; i < b.getRowCount(); i++) {
                            List<Object> key = keyOf(b, i, childIdx);
                            if (key != null && deletedKeys.contains(key)) {
                                throw new IllegalArgumentException(
                                        "foreign key violation: row still referenced by "
                                                + schemaName + "." + tableName);
                            }
                        }
                    }
                }
            }
        }
    }

    /** 主键/唯一冲突校验:新行的键值不能与现有行(或同批早前行)重复。含 null 的键不参与(唯一约束允许多 null)。 */
    private void validateUnique(ArrowTable target, VectorSchemaRoot batch,
                                List<String> columns, String constraintName) {
        List<Integer> idxs = new ArrayList<>(columns.size());
        for (String column : columns) {
            idxs.add(target.schema().columnIndex(column));
        }
        Set<List<Object>> existing = new HashSet<>();
        for (VectorSchemaRoot b : target.batches()) {
            for (int i = 0; i < b.getRowCount(); i++) {
                List<Object> key = keyOf(b, i, idxs);
                if (key != null) {
                    existing.add(key);
                }
            }
        }
        for (int i = 0; i < batch.getRowCount(); i++) {
            List<Object> key = keyOf(batch, i, idxs);
            if (key != null && !existing.add(key)) {
                throw new IllegalArgumentException(
                        constraintName + " constraint violation: " + columns);
            }
        }
    }

    /** 读一行的键值(按列索引);任一列 null 返回 null(该行不参与唯一性)。 */
    private static List<Object> keyOf(VectorSchemaRoot root, int row, List<Integer> idxs) {
        List<Object> key = new ArrayList<>(idxs.size());
        for (int idx : idxs) {
            if (root.getVector(idx).isNull(row)) {
                return null;
            }
            key.add(root.getVector(idx).getObject(row));
        }
        return key;
    }

    /**
     * UPDATE and DELETE must remove the matched rows from the table; the input
     * only produces the matched rows, so we rebuild the table content, keeping
     * unmatched rows and replacing (UPDATE) or dropping (DELETE) matched ones.
     */
    private void rewriteTable(ExecContext ctx, ArrowTable target, BatchIterator input,
                              String schemaName, String tableName) {
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
        if (getOperation() == Operation.DELETE) {
            try {
                validateDeleteRestrict(ctx, target, matched);
            } catch (RuntimeException e) {
                matched.close();
                throw e;
            }
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
        if (schemaName != null) {
            ctx.markDirty(schemaName, tableName);
        } else {
            ctx.markDirty(tableName);
        }
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
