package com.minidb.server.catalog;

import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Map;

/** 格式无关的 catalog 快照:schema 名 + 全部表定义(每表自带所属 schema)+ 表统计。 */
public record CatalogSnapshot(List<String> schemas, List<TableSchema> tables,
                              Map<String, TableStats> stats) {
    public CatalogSnapshot(List<String> schemas, List<TableSchema> tables) {
        this(schemas, tables, Map.of());
    }
}
