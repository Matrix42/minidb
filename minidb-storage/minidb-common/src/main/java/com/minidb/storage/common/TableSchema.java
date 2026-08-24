package com.minidb.storage.common;

import java.util.ArrayList;
import java.util.List;

public record TableSchema(String schemaName, String name, List<ColumnMeta> columns,
                          List<String> primaryKey, List<List<String>> uniqueKeys,
                          List<ForeignKey> foreignKeys, StorageFormat storageFormat,
                          TableType tableType) {

    public TableSchema {
        primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        uniqueKeys = uniqueKeys == null ? List.of()
                : uniqueKeys.stream().map(List::copyOf).toList();
        foreignKeys = foreignKeys == null ? List.of() : List.copyOf(foreignKeys);
        storageFormat = storageFormat == null ? StorageFormat.DEFAULT : storageFormat;
        // SQL 标准:主键隐含 NOT NULL。主键列强制不可空——① 与 LSM 的主键排序假设一致
        // (LSM 把 null 主键写成空串,会破坏主键序/唯一性);② 让排序跳过优化
        // (MiniDbCalciteTable 要求主键列全 NOT NULL 才声明 collation)对
        // 「PRIMARY KEY (col)」这类未显式写 NOT NULL 的 DDL 也生效。
        if (!primaryKey.isEmpty()) {
            List<ColumnMeta> normalized = new ArrayList<>(columns.size());
            for (ColumnMeta c : columns) {
                boolean inPk = primaryKey.stream()
                        .anyMatch(p -> p.equalsIgnoreCase(c.name()));
                if (inPk && !Boolean.FALSE.equals(c.nullable())) {
                    normalized.add(new ColumnMeta(c.name(), c.type(),
                            c.precision(), c.scale(), false));
                } else {
                    normalized.add(c);
                }
            }
            columns = List.copyOf(normalized);
        }
    }

    public TableSchema(String name, List<ColumnMeta> columns) {
        this("public", name, columns, List.of(), List.of(), List.of(), StorageFormat.DEFAULT, null);
    }

    public TableSchema(String schemaName, String name, List<ColumnMeta> columns) {
        this(schemaName, name, columns, List.of(), List.of(), List.of(), StorageFormat.DEFAULT, null);
    }

    public TableSchema(String schemaName, String name, List<ColumnMeta> columns,
                       List<String> primaryKey, List<List<String>> uniqueKeys,
                       List<ForeignKey> foreignKeys) {
        this(schemaName, name, columns, primaryKey, uniqueKeys, foreignKeys, StorageFormat.DEFAULT, null);
    }

    public TableSchema(String schemaName, String name, List<ColumnMeta> columns,
                       List<String> primaryKey, List<List<String>> uniqueKeys,
                       List<ForeignKey> foreignKeys, StorageFormat storageFormat) {
        this(schemaName, name, columns, primaryKey, uniqueKeys, foreignKeys, storageFormat, null);
    }

    /** 返回带指定存储格式的副本(加载时按引擎格式补用)。 */
    public TableSchema withStorageFormat(StorageFormat format) {
        return new TableSchema(schemaName, name, columns, primaryKey, uniqueKeys, foreignKeys, format, tableType);
    }

    public ColumnMeta column(String name) {
        for (ColumnMeta c : columns) {
            if (c.name().equalsIgnoreCase(name)) {
                return c;
            }
        }
        throw new IllegalArgumentException(
                "no column " + name + " in table " + this.name);
    }

    public int columnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "no column " + name + " in table " + this.name);
    }
}