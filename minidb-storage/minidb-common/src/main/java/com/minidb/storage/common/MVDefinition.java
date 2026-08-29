package com.minidb.storage.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/** 物化视图定义，与 TableSchema 并列——物化视图有物理存储，但定义独立于表结构。 */
public record MVDefinition(
        String schemaName,
        String name,
        String querySql,
        List<ColumnMeta> columns,
        List<TableRef> dependencies,
        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS) MVStructure structure) {

    public MVDefinition {
        dependencies = dependencies == null ? List.of() : List.copyOf(dependencies);
    }

    /** 依赖表引用 */
    public record TableRef(String schemaName, String tableName) {}
}
