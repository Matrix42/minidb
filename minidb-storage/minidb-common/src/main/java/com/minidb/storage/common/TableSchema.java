package com.minidb.storage.common;

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