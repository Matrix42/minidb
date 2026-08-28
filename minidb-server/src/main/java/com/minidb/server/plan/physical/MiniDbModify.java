package com.minidb.server.plan.physical;

import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ForeignKey;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.config.MiniDbConfig;
import com.minidb.storage.common.TableSchema;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.TableHandle;
import com.minidb.server.exec.ConstraintChecker;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.IncrementalRefreshEngine;
import com.minidb.server.exec.MVManager;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.plan.Planner;
import com.minidb.storage.common.MVDefinition;
import com.minidb.server.transaction.TransactionManager;
import com.minidb.storage.common.IndexDef;
import com.minidb.storage.common.SimpleTable;
import com.minidb.storage.lsm.LSMTable;
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

    /** 返回被修改的表的 schema 名（当前 schema 上下文）。 */
    public String getTargetSchemaName(String currentSchema) {
        List<String> qualified = table.getQualifiedName();
        int n = qualified.size();
        return n >= 3 ? qualified.get(n - 2) : currentSchema;
    }

    /** 返回被修改的表的裸名。 */
    public String getTargetTableName() {
        List<String> qualified = table.getQualifiedName();
        return qualified.get(qualified.size() - 1);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<String> qualified = table.getQualifiedName();
        int n = qualified.size();
        String tableName = qualified.get(n - 1);
        // size>=3: [minidb, other, t] → schema is second-to-last.
        // size==2: [minidb, t] (promoted table) → bare name resolves via ctx's
        // current schema.
        String schemaName = n >= 3 ? qualified.get(n - 2) : ctx.currentSchema();
        TableHandle target = ctx.getTable(schemaName, tableName);
        // SERIALIZABLE 隔离级别:写入路径必须登记写集(recordWrite),否则 lastWriteTx 恒空,
        // 提交时的读写/写写冲突检测全部漏检(见 TransactionManager.checkSerializableConflict)。
        recordWriteSet(ctx, schemaName, tableName);
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        if (getOperation() == Operation.INSERT) {
            appendRows(ctx, target, input);
        } else if (target instanceof LSMTable) {
            // LSMTable: write matched rows directly to MemTable (no full table rewrite).
            lsmModify(ctx, target, input);
        } else {
            rewriteTable(ctx, (SimpleTable) target, input);
        }
        // 写数据直接落盘,顺带标记统计过期。
        ctx.storage().catalog().markStatsStale(schemaName, tableName);
        // DML 后增量刷新物化视图：有依赖 MV 则全量刷新（简单正确）
        refreshDependentMVs(ctx, schemaName, tableName);
        return BatchIterator.interruptible(BatchIterator.empty());
    }

    /** DML 后检查依赖此表的物化视图并全量刷新。 */
    private void refreshDependentMVs(ExecContext ctx, String schemaName, String tableName) {
        if (affected == 0) return;
        MVManager mvManager = ctx.mvManager();
        if (mvManager == null) return;
        var dependents = mvManager.getDependentMVs(schemaName, tableName);
        if (dependents.isEmpty()) return;
        IncrementalRefreshEngine engine = mvManager.refreshEngine();
        for (String mvKey : dependents) {
            int dot = mvKey.indexOf('.');
            String mvSchema = mvKey.substring(0, dot);
            String mvName = mvKey.substring(dot + 1);
            MVDefinition mvDef = ctx.storage().catalog()
                    .getMaterializedView(mvSchema, mvName);
            if (mvDef == null) continue;
            // 全量刷新：TRUNCATE + 重算
            MiniDbRel mvPlan = (MiniDbRel)
                    new Planner(ctx.storage().catalog()).plan(mvDef.querySql(), mvSchema);
            TableHandle mvTable = ctx.getTable(mvSchema, mvName);
            mvTable.clearParts();
            ExecContext mvCtx = new ExecContext(ctx.storage(), ctx.allocator(), mvSchema);
            try (BatchIterator it = mvPlan.execute(mvCtx)) {
                while (it.hasNext()) {
                    VectorSchemaRoot batch = it.next();
                    VectorSchemaRoot copy = mvTable.newBatchRoot();
                    copy.allocateNew();
                    for (int i = 0; i < batch.getRowCount(); i++) {
                        RowCopier.copyRow(batch, i, copy, i);
                    }
                    copy.setRowCount(batch.getRowCount());
                    try {
                        mvTable.writePart(copy, TableHandle.Operation.INSERT);
                    } finally {
                        copy.close();
                    }
                }
            } finally {
                mvCtx.close();
            }
        }
    }

    private void appendRows(ExecContext ctx, TableHandle target, BatchIterator input) {
        affected = 0;
        try (input) {
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
                try {
                    // 事务写入:将 txId 传给存储层,由存储层决定写入 tx-private 还是主存储。
                    if (ctx.tx() != null) {
                        target.writePart(copy, TableHandle.Operation.INSERT, ctx.tx().txId());
                    } else {
                        target.writePart(copy, TableHandle.Operation.INSERT);
                    }
                    // 先写数据后写索引:索引失败时数据已落盘,下次 DDL 重建可恢复。
                    // 注意:不能直接查 target.schema().indexes()——handleCreateIndex
                    // 调 catalog.alterTable 更新了 catalog 元数据,但 target 句柄 schema
                    // 是创建时的快照,不含 indexes;需查 catalog 最新元数据。
                    TableSchema ts = ctx.storage().catalog().getTable(
                            target.schema().schemaName(), target.schema().name());
                    if (ts != null && !ts.indexes().isEmpty()) {
                        ctx.storage().indexManager().onInsert(ts, copy);
                    }
                } finally {
                    copy.close();
                }
            }
        }
        // 自动合并:part 数超过阈值时按配置目标大小合并。
        MiniDbConfig config = ctx.storage().config();
        if (target.partCount() > config.compactionAutoPartThreshold()) {
            target.compact(config.compactionTargetSizeBytes());
        }
    }

    /**
     * UPDATE/DELETE for LSMTable: write matched rows directly to MemTable
     * with UPDATE/DELETE op, instead of rewriting the entire table.
     * For UPDATE, the input batch has the schema [table columns] + [update columns];
     * we construct a full table-schema batch with updated values before writing.
     */
    private void lsmModify(ExecContext ctx, TableHandle target, BatchIterator input) {
        affected = 0;
        // 用 catalog 而非 target.schema() 查索引:handleCreateIndex 后
        // catalog 元数据已更新但 target 句柄的 schema 仍是创建时的快照。
        TableSchema ts = ctx.storage().catalog().getTable(
                target.schema().schemaName(), target.schema().name());
        TableHandle.Operation op = getOperation() == Operation.UPDATE
                ? TableHandle.Operation.UPDATE : TableHandle.Operation.DELETE;
        try {
            if (op == TableHandle.Operation.DELETE) {
                // Materialize input first so we can validate foreign key RESTRICT
                // before writing tombstones (same as rewriteTable for SimpleTable).
                VectorSchemaRoot matched = materializeInput(input, ctx);
                input.close();
                if (matched == null || matched.getRowCount() == 0) {
                    if (matched != null) {
                        matched.close();
                    }
                    return;
                }
                try {
                    validateDeleteRestrict(ctx, target, matched);
                } catch (RuntimeException e) {
                    matched.close();
                    throw e;
                }
                // 事务写入:将 txId 传给存储层
                if (ctx.tx() != null) {
                    target.writePart(matched, op, ctx.tx().txId());
                } else {
                    target.writePart(matched, op);
                }
                if (ts != null && !ts.indexes().isEmpty()) {
                    ctx.storage().indexManager().onDelete(ts, matched);
                }
                affected = matched.getRowCount();
                matched.close();
            } else {
                // UPDATE: construct full table rows with updated values from the
                // input batch [table cols] + [update cols].
                int numTableCols = target.schema().columns().size();
                List<String> updateCols = getUpdateColumnList();
                List<Integer> updateIdx = new ArrayList<>();
                for (String col : updateCols) {
                    updateIdx.add(target.schema().columnIndex(col));
                }
                while (input.hasNext()) {
                    VectorSchemaRoot batch = input.next();
                    VectorSchemaRoot out = target.newBatchRoot();
                    try (out) {
                        out.allocateNew();
                        for (int i = 0; i < batch.getRowCount(); i++) {
                            // Copy table columns (old values) from input
                            for (int c = 0; c < numTableCols; c++) {
                                out.getVector(c).copyFromSafe(i, i, batch.getVector(c));
                            }
                            // Overwrite updated columns with new values from trailing input cols
                            for (int j = 0; j < updateCols.size(); j++) {
                                RowCopier.writeValue(out.getFieldVectors().get(updateIdx.get(j)), i,
                                        batch.getFieldVectors().get(numTableCols + j), i);
                            }
                        }
                        out.setRowCount(batch.getRowCount());
                        // 外键校验:UPDATE 的新值必须引用存在的父行(与 INSERT 同一套校验)。
                        validateForeignKeys(ctx, target, out);
                        // UNIQUE 索引校验:新值不能与已有行冲突(排除本行,旧值仍存于索引表)
                        validateUpdateUnique(ctx, ts, target, batch, out);
                        // 事务写入:将 txId 传给存储层
                        if (ctx.tx() != null) {
                            target.writePart(out, op, ctx.tx().txId());
                        } else {
                            target.writePart(out, op);
                        }
                        if (ts != null && !ts.indexes().isEmpty()) {
                            // batch 是输入批(前 numTableCols 列为旧值),out 是新值批
                            ctx.storage().indexManager().onUpdate(ts, batch, out);
                        }
                        affected += batch.getRowCount();
                    }
                }
            }
        } finally {
            input.close();
        }
    }

    /** UPDATE 的 UNIQUE 索引校验:新索引键不能与已有行冲突,排除本行(旧值仍在索引表中)。 */
    private void validateUpdateUnique(ExecContext ctx, TableSchema schema,
                                       TableHandle target, VectorSchemaRoot oldBatch,
                                       VectorSchemaRoot newBatch) {
        if (schema == null || schema.indexes().isEmpty()) return;
        List<Integer> pkIdx = new ArrayList<>(schema.primaryKey().size());
        for (String col : schema.primaryKey()) {
            pkIdx.add(schema.columnIndex(col));
        }
        // 收集旧批的主键值(被更新行的旧 PK)
        Set<List<Object>> oldPks = new HashSet<>();
        for (int i = 0; i < oldBatch.getRowCount(); i++) {
            List<Object> pk = new ArrayList<>(pkIdx.size());
            for (int idx : pkIdx) {
                pk.add(oldBatch.getVector(idx).getObject(i));
            }
            oldPks.add(pk);
        }

        for (IndexDef idx : schema.indexes()) {
            if (!idx.unique()) continue;
            TableHandle indexTable = ctx.storage().indexManager()
                    .getIndex(schema.schemaName(), schema.name(), idx.name());
            if (indexTable == null) continue;
            List<Integer> idxPositions = new ArrayList<>(idx.columns().size());
            for (String col : idx.columns()) {
                idxPositions.add(schema.columnIndex(col));
            }
            List<Integer> indexPkPositions = new ArrayList<>(schema.primaryKey().size());
            for (String col : schema.primaryKey()) {
                indexPkPositions.add(schema.columnIndex(col));
            }

            for (int r = 0; r < newBatch.getRowCount(); r++) {
                // 新行的索引键
                List<Object> newIdxKey = new ArrayList<>(idxPositions.size());
                boolean hasNull = false;
                for (int pos : idxPositions) {
                    if (newBatch.getVector(pos).isNull(r)) { hasNull = true; break; }
                    newIdxKey.add(newBatch.getVector(pos).getObject(r));
                }
                if (hasNull) continue; // null 键不参与唯一性

                // 新行的主键
                List<Object> newPk = new ArrayList<>(indexPkPositions.size());
                for (int pos : indexPkPositions) {
                    newPk.add(newBatch.getVector(pos).getObject(r));
                }

                // 扫描索引表:前缀匹配索引列值,确认是否有冲突(不同 PK 的行)
                // 注意:索引表范围扫描是超集语义(只裁剪 SSTable 文件,不裁剪 MemTable 行),
                // 返回的批可能包含前缀外的行,必须逐行检查索引列前缀匹配。
                boolean conflict = false;
                try (BatchIterator it = indexTable.scan(newIdxKey, newIdxKey)) {
                    while (it.hasNext()) {
                        VectorSchemaRoot b = it.next();
                        for (int i = 0; i < b.getRowCount(); i++) {
                            // 先确认索引列前缀匹配(超集扫描可能返回前缀外的行)
                            boolean prefixMatch = true;
                            for (int c = 0; c < idxPositions.size(); c++) {
                                Object v = b.getVector(c).getObject(i);
                                if (v == null || !v.equals(newIdxKey.get(c))) {
                                    prefixMatch = false;
                                    break;
                                }
                            }
                            if (!prefixMatch) continue;

                            // 索引表 schema = (索引列..., 主键列...)
                            List<Object> existingPk = new ArrayList<>(indexPkPositions.size());
                            for (int c = 0; c < indexPkPositions.size(); c++) {
                                int vecIdx = idxPositions.size() + c;
                                existingPk.add(b.getVector(vecIdx).getObject(i));
                            }
                            // 若匹配行的 PK 正是被更新行的旧 PK → 同一行,允许
                            if (!oldPks.contains(existingPk)) {
                                conflict = true;
                                break;
                            }
                        }
                        if (conflict) break;
                    }
                }
                if (conflict) {
                    throw new IllegalArgumentException(
                            "unique index constraint violation: " + idx.name());
                }
            }
        }
    }
    private void validateInsert(ExecContext ctx, TableHandle target, VectorSchemaRoot batch) {
        // 用 catalog 而非 target.schema() 查索引元数据(handle 是创建时的快照)
        TableSchema ts = ctx.storage().catalog().getTable(
                target.schema().schemaName(), target.schema().name());
        ConstraintChecker.validateInsert(ctx, ts != null ? ts : target.schema(), target, batch);
    }

    /** 外键 INSERT 校验:child 行的外键列值必须存在于引用表(含 null 的键不校验)。 */
    private void validateForeignKeys(ExecContext ctx, TableHandle target, VectorSchemaRoot batch) {
        for (ForeignKey fk : target.schema().foreignKeys()) {
            TableHandle refTable = ctx.getTable(fk.refSchema(), fk.refTable());
            List<String> refColumns = fk.refColumns().isEmpty()
                    ? refTable.schema().primaryKey()
                    : fk.refColumns();
            List<Integer> childIdx = columnIndexes(target.schema(), fk.columns());
            List<Integer> refIdx = columnIndexes(refTable.schema(), refColumns);
            Set<List<Object>> refKeys = new HashSet<>();
            try (BatchIterator it = refTable.scan()) {
                while (it.hasNext()) {
                    VectorSchemaRoot b = it.next();
                    for (int i = 0; i < b.getRowCount(); i++) {
                        List<Object> key = keyOf(b, i, refIdx);
                        if (key != null) {
                            refKeys.add(key);
                        }
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
    private void validateDeleteRestrict(ExecContext ctx, TableHandle target, VectorSchemaRoot matched) {
        TableSchema schema = target.schema();
        if (schema.primaryKey().isEmpty()) {
            return;
        }
        List<Integer> pkIdx = ConstraintChecker.columnIndexes(schema, schema.primaryKey());
        Set<List<Object>> deletedKeys = new HashSet<>();
        for (int i = 0; i < matched.getRowCount(); i++) {
            List<Object> key = ConstraintChecker.keyOf(matched, i, pkIdx);
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
                    TableHandle childTable = ctx.getTable(schemaName, tableName);
                    List<Integer> childIdx = ConstraintChecker.columnIndexes(childSchema, fk.columns());
                    try (BatchIterator it = childTable.scan()) {
                        while (it.hasNext()) {
                            VectorSchemaRoot b = it.next();
                            for (int i = 0; i < b.getRowCount(); i++) {
                                List<Object> key = ConstraintChecker.keyOf(b, i, childIdx);
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
    }

    /** 主键/唯一冲突校验:新行的键值不能与现有行(或同批早前行)重复。含 null 的键不参与(唯一约束允许多 null)。 */
    private void validateUnique(TableHandle target, VectorSchemaRoot batch,
                                List<String> columns, String constraintName) {
        List<Integer> idxs = new ArrayList<>(columns.size());
        for (String column : columns) {
            idxs.add(target.schema().columnIndex(column));
        }
        Set<List<Object>> existing = new HashSet<>();
        try (BatchIterator it = target.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                for (int i = 0; i < b.getRowCount(); i++) {
                    List<Object> key = keyOf(b, i, idxs);
                    if (key != null) {
                        existing.add(key);
                    }
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
     * only produces the matched rows, so we read all parts, keep unmatched rows
     * and replace (UPDATE) or drop (DELETE) matched ones, then rewrite the parts.
     *
     * <p>SimpleTable 不支持索引(建索引时已拒绝),无需索引维护挂钩。</p>
     */
    private void rewriteTable(ExecContext ctx, SimpleTable target, BatchIterator input) {
        int numTableCols = target.schema().columns().size();
        List<String> updateCols = getOperation() == Operation.UPDATE
                ? getUpdateColumnList() : List.of();
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
        // UPDATE:校验更新后的外键值仍引用存在的父行(SimpleTable 路径)。
        if (getOperation() == Operation.UPDATE && !target.schema().foreignKeys().isEmpty()) {
            VectorSchemaRoot updated = updatedRowsRoot(target, matched, numTableCols, updateCols);
            try {
                validateForeignKeys(ctx, target, updated);
            } catch (RuntimeException e) {
                updated.close();
                matched.close();
                throw e;
            }
            updated.close();
        }
        // One representative matched row per original full-row value: identical
        // original rows always produce identical updated rows.
        Map<List<Object>, Integer> matchRow = new HashMap<>();
        for (int i = 0; i < matched.getRowCount(); i++) {
            matchRow.putIfAbsent(rowKey(matched, i, numTableCols), i);
        }
        // 读所有 part,rebuild 成新 part。事务内需读「自己的快照」(含本事务此前 rewrite),
        // 否则第二次 UPDATE/DELETE 会丢失第一次的变更。
        List<VectorSchemaRoot> newBatches = new ArrayList<>();
        affected = 0;
        try (BatchIterator it = ctx.tx() != null
                ? target.scan(ctx.tx().snapshotTxId(), ctx.tx().txId())
                : target.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot old = it.next();
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
        }
        // UPDATE 唯一性:rewrite 后的新表内容里主键/UNIQUE 键不得重复(SimpleTable 无索引,
        // 不能像 LSM 那样走 validateUpdateUnique,只能对完整新快照做批内查重)。
        if (getOperation() == Operation.UPDATE) {
            try {
                validateUniqueAcross(newBatches, target, target.schema().primaryKey(), "primary key");
                for (List<String> uniqueCols : target.schema().uniqueKeys()) {
                    validateUniqueAcross(newBatches, target, uniqueCols, "unique");
                }
            } catch (RuntimeException e) {
                // 校验失败:释放已构造的新快照批与 matched,避免 Arrow 内存泄漏。
                for (VectorSchemaRoot nb : newBatches) {
                    nb.close();
                }
                matched.close();
                throw e;
            }
        }
        if (ctx.tx() != null) {
            // 事务路径:整表 rewrite 不删 base(否则并发事务读到空表,违反隔离性),
            // 而是把新快照写进 .tx/<txId>/ 并打 rewrite 标记;commit 时替换 base,
            // rollback 时丢弃。自身事务读 scan(snapshotTxId, txId) 会走新快照。
            target.markRewrite(ctx.tx().txId());
            // 同一事务多次改写:先清掉旧快照 part,再写基于最新快照的新 part。
            target.clearRewriteParts(ctx.tx().txId());
            for (VectorSchemaRoot nb : newBatches) {
                target.writePart(nb, TableHandle.Operation.INSERT, ctx.tx().txId());
                nb.close();
            }
        } else {
            // 非事务路径:删旧 part,写新 part。
            target.clearParts();
            for (VectorSchemaRoot nb : newBatches) {
                target.writePart(nb);
                nb.close();
            }
        }
        matched.close();
    }

    /**
     * 对 rewrite 出的完整新表内容做主键/唯一键查重(含 null 的键不参与,主键列 NOT NULL
     * 由 TableSchema 保证故恒参与)。写盘前校验避免把矛盾数据落盘。
     */
    private void validateUniqueAcross(List<VectorSchemaRoot> batches, TableHandle target,
                                      List<String> columns, String constraintName) {
        if (columns.isEmpty()) {
            return;
        }
        List<Integer> idxs = new ArrayList<>(columns.size());
        for (String column : columns) {
            idxs.add(target.schema().columnIndex(column));
        }
        Set<List<Object>> seen = new HashSet<>();
        for (VectorSchemaRoot batch : batches) {
            for (int i = 0; i < batch.getRowCount(); i++) {
                List<Object> key = keyOf(batch, i, idxs);
                if (key != null && !seen.add(key)) {
                    throw new IllegalArgumentException(
                            constraintName + " constraint violation: " + columns);
                }
            }
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

    /**
     * 把 UPDATE 的输入批([表列] + [更新值])物化成「更新后的全表行」批,供外键校验复用
     * 与 INSERT 相同的 {@link #validateForeignKeys} 逻辑。matched 的每行对应一个被更新行。
     */
    private VectorSchemaRoot updatedRowsRoot(SimpleTable target, VectorSchemaRoot matched,
                                             int numTableCols, List<String> updateCols) {
        List<Integer> updateIdx = new ArrayList<>(updateCols.size());
        for (String col : updateCols) {
            updateIdx.add(target.schema().columnIndex(col));
        }
        int rows = matched.getRowCount();
        VectorSchemaRoot out = target.newBatchRoot();
        out.allocateNew();
        for (int i = 0; i < rows; i++) {
            for (int c = 0; c < numTableCols; c++) {
                out.getFieldVectors().get(c)
                        .copyFromSafe(i, i, matched.getFieldVectors().get(c));
            }
            for (int j = 0; j < updateCols.size(); j++) {
                RowCopier.writeValue(
                        out.getFieldVectors().get(updateIdx.get(j)), i,
                        matched.getFieldVectors().get(numTableCols + j), i);
            }
        }
        out.setRowCount(rows);
        return out;
    }

    /**
     * SERIALIZABLE 隔离级别:把被修改表的所有列写入事务写集。MiniDbScan.recordReadSet
     * 按列粒度登记读集,写集必须与读集同粒度(键格式 schema.table.column),否则同一列
     * 的读写冲突无法匹配。非事务或非 SERIALIZABLE 时 recordWrite 内部短路。
     */
    private void recordWriteSet(ExecContext ctx, String schemaName, String tableName) {
        if (!ctx.inTransaction()) {
            return;
        }
        TableSchema schema = ctx.storage().catalog().getTable(schemaName, tableName);
        if (schema == null) {
            return;
        }
        TransactionManager tm = ctx.storage().transactionManager();
        for (ColumnMeta col : schema.columns()) {
            tm.recordWrite(ctx.tx().txId(), schemaName + "." + tableName + "." + col.name());
        }
    }
}