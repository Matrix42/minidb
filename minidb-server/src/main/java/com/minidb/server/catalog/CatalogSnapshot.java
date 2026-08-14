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

    public CatalogSnapshot {
        // 旧 catalog.json(本次改动之前写的)无 stats 字段,Jackson 走 canonical 构造器
        // 会把 stats 反序列化为 null;这里归一化为空 Map,避免 restore() 里 NPE。
        stats = stats == null ? Map.of() : stats;
    }
}
