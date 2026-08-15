package com.minidb.storage.common;

import java.util.List;

public record TableSchema(String schemaName, String name, List<ColumnMeta> columns,
                          List<String> primaryKey, List<List<String>> uniqueKeys,
                          List<ForeignKey> foreignKeys) {

    public TableSchema {
        // 旧 catalog.json 无约束字段,反序列化为 null;归一化为空(无约束,向后兼容)。
        primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        uniqueKeys = uniqueKeys == null ? List.of()
                : uniqueKeys.stream().map(List::copyOf).toList();
        foreignKeys = foreignKeys == null ? List.of() : List.copyOf(foreignKeys);
    }

    public TableSchema(String name, List<ColumnMeta> columns) {
        this("public", name, columns, List.of(), List.of(), List.of());
    }

    public TableSchema(String schemaName, String name, List<ColumnMeta> columns) {
        this(schemaName, name, columns, List.of(), List.of(), List.of());
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
