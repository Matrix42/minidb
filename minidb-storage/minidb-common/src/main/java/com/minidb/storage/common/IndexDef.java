package com.minidb.storage.common;

import java.util.List;

/** 索引定义元数据:与表 schema 的 indexes 列表一起持久化到 catalog.json。 */
public record IndexDef(String name, boolean unique, List<String> columns) {
    public IndexDef {
        columns = columns == null ? List.of() : List.copyOf(columns);
    }
}
