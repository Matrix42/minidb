package com.minidb.server.exec;

import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ForeignKey;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.common.TableSchema;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 约束校验的单一来源:INSERT(DML)与 ALTER TABLE ADD CONSTRAINT / SET NOT NULL 共用。
 * 两类语义:
 * <ul>
 *   <li>{@link #validateInsert}:校验「一批新行」与「表现有行」不冲突(batch 不在 existing 中);</li>
 *   <li>{@link #validateTableSatisfies}:对「表已有数据」自检是否满足 proposed 约束(表内查重)。</li>
 * </ul>
 * 两者不可混用:若用 validateInsert 校验已有数据,会因 batch 本身就是 existing 的行而误报冲突。
 */
public final class ConstraintChecker {

    private ConstraintChecker() {
    }

    /** INSERT 前的约束校验:NOT NULL + 主键/唯一冲突 + 外键引用存在。 */
    public static void validateInsert(ExecContext ctx, TableSchema schema,
                                      TableHandle target, VectorSchemaRoot batch) {
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
            validateUnique(schema, target, batch, schema.primaryKey(), "primary key");
        }
        for (List<String> unique : schema.uniqueKeys()) {
            validateUnique(schema, target, batch, unique, "unique");
        }
        validateForeignKeys(ctx, schema, target, batch);
    }

    /** 对表已有数据自检 proposed 约束(NOT NULL + 主键/唯一查重 + 外键引用存在)。 */
    public static void validateTableSatisfies(ExecContext ctx, TableHandle table,
                                              TableSchema proposed) {
        for (ColumnMeta column : proposed.columns()) {
            if (!column.nullable()) {
                int idx = proposed.columnIndex(column.name());
                try (BatchIterator it = table.scan()) {
                    while (it.hasNext()) {
                        VectorSchemaRoot b = it.next();
                        for (int i = 0; i < b.getRowCount(); i++) {
                            if (b.getVector(idx).isNull(i)) {
                                throw new IllegalArgumentException(
                                        "null value in column \"" + column.name()
                                                + "\" violates not-null constraint");
                            }
                        }
                    }
                }
            }
        }
        if (!proposed.primaryKey().isEmpty()) {
            validateDistinct(table, proposed, proposed.primaryKey(), "primary key");
        }
        for (List<String> unique : proposed.uniqueKeys()) {
            validateDistinct(table, proposed, unique, "unique");
        }
        validateForeignKeysExist(ctx, table, proposed);
    }

    /** 主键/唯一冲突校验:新行的键值不能与现有行(或同批早前行)重复。含 null 的键不参与(唯一约束允许多 null)。 */
    private static void validateUnique(TableSchema schema, TableHandle target,
                                       VectorSchemaRoot batch, List<String> columns,
                                       String constraintName) {
        List<Integer> idxs = columnIndexes(schema, columns);
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

    /** 表内查重(ADD CONSTRAINT 用):整表扫描,发现重复键即报错。 */
    private static void validateDistinct(TableHandle table, TableSchema schema,
                                         List<String> columns, String constraintName) {
        List<Integer> idxs = columnIndexes(schema, columns);
        Set<List<Object>> seen = new HashSet<>();
        try (BatchIterator it = table.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                for (int i = 0; i < b.getRowCount(); i++) {
                    List<Object> key = keyOf(b, i, idxs);
                    if (key != null && !seen.add(key)) {
                        throw new IllegalArgumentException(
                                constraintName + " constraint violation: " + columns);
                    }
                }
            }
        }
    }

    /** 外键 INSERT 校验:child 行的外键列值必须存在于引用表(含 null 的键不校验)。 */
    private static void validateForeignKeys(ExecContext ctx, TableSchema schema,
                                            TableHandle target, VectorSchemaRoot batch) {
        for (ForeignKey fk : schema.foreignKeys()) {
            TableHandle refTable = ctx.getTable(fk.refSchema(), fk.refTable());
            List<String> refColumns = fk.refColumns().isEmpty()
                    ? refTable.schema().primaryKey()
                    : fk.refColumns();
            List<Integer> childIdx = columnIndexes(schema, fk.columns());
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

    /** 外键 ADD 校验:表内每行外键值必须存在于引用表。 */
    private static void validateForeignKeysExist(ExecContext ctx, TableHandle table,
                                                 TableSchema schema) {
        for (ForeignKey fk : schema.foreignKeys()) {
            TableHandle refTable = ctx.getTable(fk.refSchema(), fk.refTable());
            List<String> refColumns = fk.refColumns().isEmpty()
                    ? refTable.schema().primaryKey()
                    : fk.refColumns();
            List<Integer> childIdx = columnIndexes(schema, fk.columns());
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
            try (BatchIterator it = table.scan()) {
                while (it.hasNext()) {
                    VectorSchemaRoot b = it.next();
                    for (int i = 0; i < b.getRowCount(); i++) {
                        List<Object> key = keyOf(b, i, childIdx);
                        if (key != null && !refKeys.contains(key)) {
                            throw new IllegalArgumentException(
                                    "foreign key violation: " + fk.columns()
                                            + " references " + fk.refTable() + "." + refColumns);
                        }
                    }
                }
            }
        }
    }

    public static List<Integer> columnIndexes(TableSchema schema, List<String> names) {
        List<Integer> idxs = new ArrayList<>(names.size());
        for (String name : names) {
            idxs.add(schema.columnIndex(name));
        }
        return idxs;
    }

    /** 读一行的键值(按列索引);任一列 null 返回 null(该行不参与唯一性)。 */
    public static List<Object> keyOf(VectorSchemaRoot root, int row, List<Integer> idxs) {
        List<Object> key = new ArrayList<>(idxs.size());
        for (int idx : idxs) {
            if (root.getVector(idx).isNull(row)) {
                return null;
            }
            key.add(root.getVector(idx).getObject(row));
        }
        return key;
    }
}
