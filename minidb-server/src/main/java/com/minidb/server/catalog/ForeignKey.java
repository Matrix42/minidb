package com.minidb.server.catalog;

import java.util.List;

/** 外键约束:本表列 {@code columns} 引用 {@code refSchema.refTable} 的 {@code refColumns}。 */
public record ForeignKey(List<String> columns, String refSchema, String refTable,
                         List<String> refColumns) {
    public ForeignKey {
        columns = List.copyOf(columns);
        refColumns = List.copyOf(refColumns);
    }
}
