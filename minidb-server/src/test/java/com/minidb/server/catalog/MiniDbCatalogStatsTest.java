package com.minidb.server.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.server.stats.Histogram;
import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MiniDbCatalogStatsTest {

    @Test
    void setAndGetStats() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))));
        TableStats ts = new TableStats(Map.of("id",
                new Histogram(ColumnType.INTEGER, List.of(), List.of(), 5, 0, 10)), 10, false);

        catalog.setStats("public", "t", ts);
        assertEquals(10, catalog.getStats("public", "t").rowCount());
        assertNull(catalog.getStats("public", "missing"));
    }

    @Test
    void markStatsStaleIsNoOpWithoutStats() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.markStatsStale("public", "t"); // 无统计:不抛、不产生条目
        assertNull(catalog.getStats("public", "t"));
    }

    @Test
    void dropTableRemovesStats() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t",
                new TableStats(Map.of(), 10, false));
        catalog.dropTable("public", "t");
        assertNull(catalog.getStats("public", "t"));
    }

    @Test
    void snapshotAndRestoreCarryStats() {
        MiniDbCatalog src = new MiniDbCatalog();
        src.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        src.setStats("public", "t", new TableStats(Map.of(), 42, false));

        MiniDbCatalog dst = new MiniDbCatalog();
        dst.restore(src.snapshot());
        assertEquals(42, dst.getStats("public", "t").rowCount());
    }

    @Test
    void legacySnapshotWithoutStatsRestoresCleanly() throws Exception {
        // 旧 catalog.json(改动前写的)无 stats 字段,Jackson 反序列化 stats 为 null,
        // compact 构造器归一化为空 Map,restore 不应抛 NPE。
        CatalogSnapshot legacy = new ObjectMapper()
                .readValue("{\"schemas\":[],\"tables\":[]}", CatalogSnapshot.class);
        assertEquals(Map.of(), legacy.stats());
        new MiniDbCatalog().restore(legacy);
    }
}
