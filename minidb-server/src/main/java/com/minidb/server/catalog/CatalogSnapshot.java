package com.minidb.server.catalog;

import com.minidb.storage.common.MVDefinition;
import com.minidb.storage.common.TableSchema;
import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Map;

/** 格式无关的 catalog 快照:schema 名 + 全部表定义(每表自带所属 schema)+ 视图定义 + 物化视图定义 + 表统计。 */
public record CatalogSnapshot(List<String> schemas, List<TableSchema> tables,
                              List<ViewDefinition> views,
                              List<MVDefinition> materializedViews,
                              Map<String, TableStats> stats) {
    public CatalogSnapshot(List<String> schemas, List<TableSchema> tables) {
        this(schemas, tables, List.of(), List.of(), Map.of());
    }

    public CatalogSnapshot {
        // 旧 catalog.json(视图/统计字段引入之前写的)反序列化时会把 views/stats 置为 null;
        // 这里归一化为空集合,避免 restore() 里 NPE。
        views = views == null ? List.of() : views;
        materializedViews = materializedViews == null ? List.of() : materializedViews;
        stats = stats == null ? Map.of() : stats;
    }
}