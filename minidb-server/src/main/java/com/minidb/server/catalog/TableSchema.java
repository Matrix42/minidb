package com.minidb.server.catalog;

import java.util.List;

public record TableSchema(String name, List<ColumnMeta> columns) {

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
