package com.minidb.server.exec;

import com.minidb.parser.ddl.SqlAlterTable;
import com.minidb.server.exec.functions.Kernels;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.ForeignKey;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.common.SimpleTable;
import com.minidb.storage.common.TableSchema;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

/**
 * ALTER TABLE 执行:解析语义 → 构造新 {@link TableSchema}(不可变,new 一个)→
 * 结构类操作(列增删/改类型)重写 part 文件、元数据类操作(改名/not-null/约束)
 * 只替换 catalog + 重建目录句柄。类型转换复用 {@link VectorCasts}(与 CAST 同源),
 * 约束校验复用 {@link ConstraintChecker}(与 INSERT 同源)。
 */
public class AlterTableHandler {

    private final StorageManager storage;
    private final BufferAllocator allocator;
    private String schemaName;
    private String tableName;

    public AlterTableHandler(StorageManager storage, BufferAllocator allocator) {
        this.storage = storage;
        this.allocator = allocator;
    }

    public QueryResult handle(SqlAlterTable alter, String currentSchema) {
        List<String> parts = alter.table().names;
        schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        tableName = parts.get(parts.size() - 1);
        TableSchema oldSchema = storage.catalog().getTable(schemaName, tableName);
        TableHandle oldTable = storage.getTable(schemaName, tableName);
        switch (alter.kind()) {
            case ADD_COLUMN -> handleAddColumn(alter, oldSchema, (SimpleTable) oldTable);
            case DROP_COLUMN -> handleDropColumn(alter, oldSchema, (SimpleTable) oldTable);
            case RENAME_COLUMN -> handleRenameColumn(alter, oldSchema);
            case RENAME_TABLE -> handleRenameTable(alter);
            case ALTER_TYPE -> handleAlterType(alter, oldSchema, (SimpleTable) oldTable);
            case SET_NOT_NULL -> handleNotNull(alter, oldSchema, (SimpleTable) oldTable, false);
            case DROP_NOT_NULL -> handleNotNull(alter, oldSchema, (SimpleTable) oldTable, true);
            case ADD_CONSTRAINT -> handleAddConstraint(alter, oldSchema, (SimpleTable) oldTable);
            case DROP_CONSTRAINT -> handleDropConstraint(alter, oldSchema);
        }
        return new QueryResult.Update(0);
    }

    private void handleAddColumn(SqlAlterTable alter, TableSchema oldSchema, SimpleTable oldTable) {
        String colName = alter.column().getSimple();
        if (hasColumn(oldSchema, colName)) {
            throw new IllegalArgumentException("column already exists: " + colName);
        }
        boolean nullable = !Boolean.FALSE.equals(alter.nullable());
        ColumnMeta newCol = columnFromDataType(colName, alter.dataType(), nullable);
        if (!nullable && alter.defaultExpr() == null && oldTable.rowCount() > 0) {
            throw new IllegalArgumentException("cannot add NOT NULL column \"" + colName
                    + "\" to non-empty table without DEFAULT");
        }
        List<ColumnMeta> newCols = new ArrayList<>(oldSchema.columns());
        newCols.add(newCol);
        TableSchema newSchema = withColumns(oldSchema, newCols);
        rewriteAddColumn(oldTable, newSchema, oldSchema.columns().size(), alter.defaultExpr());
    }

    private void handleDropColumn(SqlAlterTable alter, TableSchema oldSchema, SimpleTable oldTable) {
        String colName = alter.column().getSimple();
        checkColumnNotConstrained(oldSchema, colName, "drop");
        int idx = oldSchema.columnIndex(colName);
        List<ColumnMeta> newCols = new ArrayList<>(oldSchema.columns());
        newCols.remove(idx);
        TableSchema newSchema = withColumns(oldSchema, newCols);
        rewriteDropColumn(oldTable, newSchema, idx);
    }

    private void handleRenameColumn(SqlAlterTable alter, TableSchema oldSchema) {
        String oldName = alter.column().getSimple();
        String newName = alter.newColumn().getSimple();
        checkColumnNotConstrained(oldSchema, oldName, "rename");
        if (hasColumn(oldSchema, newName)) {
            throw new IllegalArgumentException("column already exists: " + newName);
        }
        int idx = oldSchema.columnIndex(oldName);
        List<ColumnMeta> newCols = new ArrayList<>(oldSchema.columns());
        ColumnMeta c = newCols.get(idx);
        newCols.set(idx, new ColumnMeta(newName, c.type(), c.precision(), c.scale(), c.nullable()));
        storage.alterTable(schemaName, tableName, withColumns(oldSchema, newCols));
    }

    private void handleRenameTable(SqlAlterTable alter) {
        List<String> names = alter.newTable().names;
        String newName = names.get(names.size() - 1);
        storage.renameTable(schemaName, tableName, newName);
    }

    private void handleAlterType(SqlAlterTable alter, TableSchema oldSchema, SimpleTable oldTable) {
        String colName = alter.column().getSimple();
        checkColumnNotConstrained(oldSchema, colName, "alter type");
        int idx = oldSchema.columnIndex(colName);
        ColumnMeta old = oldSchema.columns().get(idx);
        ColumnMeta newCol = columnFromDataType(colName, alter.dataType(), old.nullable());
        List<ColumnMeta> newCols = new ArrayList<>(oldSchema.columns());
        newCols.set(idx, newCol);
        TableSchema newSchema = withColumns(oldSchema, newCols);
        rewriteAlterType(oldTable, newSchema, idx, newCol);
    }

    private void handleNotNull(SqlAlterTable alter, TableSchema oldSchema, SimpleTable oldTable,
                               boolean nullable) {
        String colName = alter.column().getSimple();
        int idx = oldSchema.columnIndex(colName);
        ColumnMeta old = oldSchema.columns().get(idx);
        ColumnMeta newCol = new ColumnMeta(old.name(), old.type(), old.precision(), old.scale(), nullable);
        List<ColumnMeta> newCols = new ArrayList<>(oldSchema.columns());
        newCols.set(idx, newCol);
        TableSchema newSchema = withColumns(oldSchema, newCols);
        if (!nullable) {
            ConstraintChecker.validateTableSatisfies(
                    new ExecContext(storage, allocator, schemaName), oldTable, newSchema);
        }
        storage.alterTable(schemaName, tableName, newSchema);
    }

    private void handleAddConstraint(SqlAlterTable alter, TableSchema oldSchema, SimpleTable oldTable) {
        List<String> primaryKey = oldSchema.primaryKey();
        List<List<String>> uniqueKeys = new ArrayList<>(oldSchema.uniqueKeys());
        List<ForeignKey> foreignKeys = new ArrayList<>(oldSchema.foreignKeys());
        if (alter.constraintKind() == SqlKind.PRIMARY_KEY) {
            primaryKey = columnNames(alter.columns());
        } else if (alter.constraintKind() == SqlKind.UNIQUE) {
            uniqueKeys.add(columnNames(alter.columns()));
        } else {
            List<String> cols = columnNames(alter.columns());
            List<String> refNames = alter.refTable().names;
            String refTable = refNames.get(refNames.size() - 1);
            String refSchema = refNames.size() > 1 ? refNames.get(0) : schemaName;
            List<String> refCols = alter.refColumns() != null ? columnNames(alter.refColumns()) : List.of();
            foreignKeys.add(new ForeignKey(cols, refSchema, refTable, refCols));
        }
        TableSchema newSchema = new TableSchema(schemaName, tableName, oldSchema.columns(),
                primaryKey, uniqueKeys, foreignKeys, oldSchema.storageFormat());
        ConstraintChecker.validateTableSatisfies(
                new ExecContext(storage, allocator, schemaName), oldTable, newSchema);
        storage.alterTable(schemaName, tableName, newSchema);
    }

    private void handleDropConstraint(SqlAlterTable alter, TableSchema oldSchema) {
        if (alter.constraintKind() == SqlKind.PRIMARY_KEY) {
            TableSchema newSchema = new TableSchema(schemaName, tableName, oldSchema.columns(),
                    List.of(), oldSchema.uniqueKeys(), oldSchema.foreignKeys(), oldSchema.storageFormat());
            storage.alterTable(schemaName, tableName, newSchema);
            return;
        }
        // 约束名未随 TableSchema 持久化(见 ForeignKey/uniqueKeys 无 name 字段),
        // 故无法按名字定位;首版只支持 DROP PRIMARY KEY。
        throw new IllegalArgumentException(
                "named constraint drop is not supported: " + alter.constraintName().getSimple());
    }

    // ---- 数据重写 ----

    private void rewriteAddColumn(SimpleTable oldTable, TableSchema newSchema,
                                  int newColIndex, SqlNode defaultExpr) {
        List<VectorSchemaRoot> newBatches = new ArrayList<>();
        int oldCols = oldTable.schema().columns().size();
        try (BatchIterator it = oldTable.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot src = it.next();
                int rows = src.getRowCount();
                VectorSchemaRoot dst = newRoot(newSchema);
                dst.allocateNew();
                for (int r = 0; r < rows; r++) {
                    for (int c = 0; c < oldCols; c++) {
                        dst.getVector(c).copyFromSafe(r, r, src.getVector(c));
                    }
                    if (defaultExpr != null) {
                        writeDefault(dst.getVector(newColIndex), r, defaultExpr);
                    }
                }
                dst.setRowCount(rows);
                newBatches.add(dst);
            }
        }
        replace(oldTable, newSchema, newBatches);
    }

    private void rewriteDropColumn(SimpleTable oldTable, TableSchema newSchema, int droppedIndex) {
        List<VectorSchemaRoot> newBatches = new ArrayList<>();
        int oldCols = oldTable.schema().columns().size();
        try (BatchIterator it = oldTable.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot src = it.next();
                int rows = src.getRowCount();
                VectorSchemaRoot dst = newRoot(newSchema);
                dst.allocateNew();
                for (int r = 0; r < rows; r++) {
                    int d = 0;
                    for (int c = 0; c < oldCols; c++) {
                        if (c == droppedIndex) {
                            continue;
                        }
                        dst.getVector(d).copyFromSafe(r, r, src.getVector(c));
                        d++;
                    }
                }
                dst.setRowCount(rows);
                newBatches.add(dst);
            }
        }
        replace(oldTable, newSchema, newBatches);
    }

    private void rewriteAlterType(SimpleTable oldTable, TableSchema newSchema,
                                  int changedIndex, ColumnMeta newCol) {
        List<VectorSchemaRoot> newBatches = new ArrayList<>();
        int oldCols = oldTable.schema().columns().size();
        try (BatchIterator it = oldTable.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot src = it.next();
                int rows = src.getRowCount();
                FieldVector casted = VectorCasts.cast(src.getVector(changedIndex),
                        newCol.type(), newCol.precision(), newCol.scale(), allocator);
                try {
                    VectorSchemaRoot dst = newRoot(newSchema);
                    dst.allocateNew();
                    for (int r = 0; r < rows; r++) {
                        for (int c = 0; c < oldCols; c++) {
                            if (c == changedIndex) {
                                dst.getVector(c).copyFromSafe(r, r, casted);
                            } else {
                                dst.getVector(c).copyFromSafe(r, r, src.getVector(c));
                            }
                        }
                    }
                    dst.setRowCount(rows);
                    newBatches.add(dst);
                } finally {
                    casted.close();
                }
            }
        }
        replace(oldTable, newSchema, newBatches);
    }

    private void replace(SimpleTable oldTable, TableSchema newSchema, List<VectorSchemaRoot> newBatches) {
        oldTable.clearParts();
        storage.alterTable(schemaName, tableName, newSchema);
        SimpleTable newTable = (SimpleTable) storage.getTable(schemaName, tableName);
        for (VectorSchemaRoot b : newBatches) {
            newTable.writePart(b);
            b.close();
        }
    }

    // ---- 辅助 ----

    private VectorSchemaRoot newRoot(TableSchema schema) {
        List<Field> fields = new ArrayList<>();
        for (ColumnMeta c : schema.columns()) {
            fields.add(ArrowTypes.field(c));
        }
        Schema arrowSchema = new Schema(fields, Map.of("schema", schema.schemaName()));
        return VectorSchemaRoot.create(arrowSchema, allocator);
    }

    private TableSchema withColumns(TableSchema old, List<ColumnMeta> newCols) {
        return new TableSchema(old.schemaName(), old.name(), newCols,
                old.primaryKey(), old.uniqueKeys(), old.foreignKeys(), old.storageFormat());
    }

    /** 从 SqlDataTypeSpec 解析列定义(与 QueryExecutor.handleCreate 的列解析一致)。 */
    private static ColumnMeta columnFromDataType(String name, SqlDataTypeSpec dataType, boolean nullable) {
        String typeName = dataType.getTypeName().getSimple();
        ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
        int precision = ColumnMeta.PRECISION_UNSET;
        int scale = ColumnMeta.SCALE_UNSET;
        if (type == ColumnType.DECIMAL || type == ColumnType.NUMERIC) {
            if (dataType.getTypeNameSpec() instanceof SqlBasicTypeNameSpec basicSpec) {
                if (basicSpec.getPrecision() >= 0) {
                    precision = basicSpec.getPrecision();
                }
                if (basicSpec.getScale() >= 0) {
                    scale = basicSpec.getScale();
                }
            }
        }
        return new ColumnMeta(name, type, precision, scale, nullable);
    }

    private static boolean hasColumn(TableSchema schema, String name) {
        for (ColumnMeta c : schema.columns()) {
            if (c.name().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> columnNames(SqlNodeList list) {
        List<String> names = new ArrayList<>();
        for (SqlNode n : list) {
            names.add(((SqlIdentifier) n).getSimple());
        }
        return names;
    }

    /** 结构变更(删列/改列名/改类型)前校验:列不参与主键/唯一/外键,也不被其它表外键引用。 */
    private void checkColumnNotConstrained(TableSchema schema, String columnName, String operation) {
        if (schema.primaryKey().stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
            throw new IllegalArgumentException("column \"" + columnName
                    + "\" is part of primary key; cannot " + operation);
        }
        for (List<String> uk : schema.uniqueKeys()) {
            if (uk.stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
                throw new IllegalArgumentException("column \"" + columnName
                        + "\" is part of unique constraint; cannot " + operation);
            }
        }
        for (ForeignKey fk : schema.foreignKeys()) {
            if (fk.columns().stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
                throw new IllegalArgumentException("column \"" + columnName
                        + "\" is part of foreign key; cannot " + operation);
            }
        }
        for (String s : storage.catalog().schemaNames()) {
            for (String t : storage.catalog().tableNames(s)) {
                TableSchema other = storage.catalog().getTable(s, t);
                for (ForeignKey fk : other.foreignKeys()) {
                    if (fk.refSchema().equalsIgnoreCase(schema.schemaName())
                            && fk.refTable().equalsIgnoreCase(schema.name())
                            && fk.refColumns().stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
                        throw new IllegalArgumentException("column \"" + columnName
                                + "\" is referenced by foreign key in " + s + "." + t
                                + "; cannot " + operation);
                    }
                }
            }
        }
    }

    private static void writeDefault(FieldVector dst, int row, SqlNode defaultExpr) {
        SqlLiteral lit = (SqlLiteral) defaultExpr;
        if (lit.getValue() == null) {
            dst.setNull(row);
            return;
        }
        if (dst instanceof IntVector v) {
            v.setSafe(row, lit.getValueAs(BigDecimal.class).intValue());
        } else if (dst instanceof BigIntVector v) {
            v.setSafe(row, lit.getValueAs(BigDecimal.class).longValue());
        } else if (dst instanceof SmallIntVector v) {
            v.setSafe(row, lit.getValueAs(BigDecimal.class).shortValue());
        } else if (dst instanceof Float4Vector v) {
            v.setSafe(row, lit.getValueAs(BigDecimal.class).floatValue());
        } else if (dst instanceof Float8Vector v) {
            v.setSafe(row, lit.getValueAs(BigDecimal.class).doubleValue());
        } else if (dst instanceof VarCharVector v) {
            v.setSafe(row, lit.getValueAs(String.class).getBytes(StandardCharsets.UTF_8));
        } else if (dst instanceof BitVector v) {
            v.setSafe(row, lit.getValueAs(Boolean.class) ? 1 : 0);
        } else if (dst instanceof DecimalVector v) {
            v.setSafe(row, Kernels.scaleTo(v, lit.getValueAs(BigDecimal.class)));
        } else if (dst instanceof DateDayVector || dst instanceof TimeMilliVector
                || dst instanceof TimeStampMilliVector || dst instanceof VarBinaryVector) {
            throw new IllegalArgumentException(
                    "DEFAULT literal unsupported for column type " + dst.getClass().getSimpleName());
        } else {
            throw new IllegalArgumentException("unsupported DEFAULT target: " + dst.getClass().getSimpleName());
        }
    }
}
