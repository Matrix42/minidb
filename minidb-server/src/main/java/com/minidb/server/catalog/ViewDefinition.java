package com.minidb.server.catalog;

import java.util.List;

/**
 * 一条视图定义:名字 + 定义 SQL(规范化后的可重解析文本)+ 查询结果列。
 * 与 {@link TableSchema} 并列——视图无物理存储,查询时由 Calcite 的 ViewTable 展开。
 */
public record ViewDefinition(String schemaName, String name, String querySql,
                             List<ColumnMeta> columns) {
}
