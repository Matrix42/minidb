package com.minidb.server.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.server.calcite.MiniDbCalciteTable;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.TableSchema;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexCboTest {

    @TempDir
    Path dataDir;

    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void uniqueIndexInKeys() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_a ON t (a)");
        executor.execute("CREATE INDEX idx_b ON t (b)");

        TableSchema schema = catalog.getTable("public", "t");
        MiniDbCalciteTable table = new MiniDbCalciteTable(schema, catalog);
        List<ImmutableBitSet> keys = table.getStatistic().getKeys();

        // 主键 id 在 keys 中
        assertTrue(keys.stream().anyMatch(k -> k.equals(ImmutableBitSet.of(0))),
                "主键 id 应在 keys 中");
        // UNIQUE 索引 idx_a 在 keys 中
        assertTrue(keys.stream().anyMatch(k -> k.equals(ImmutableBitSet.of(1))),
                "UNIQUE 索引 idx_a 应在 keys 中");
        // 非唯一索引 idx_b 不在 keys 中
        assertFalse(keys.stream().anyMatch(k -> k.equals(ImmutableBitSet.of(2))),
                "非唯一索引 idx_b 不应在 keys 中");
    }

    @Test
    void compositeUniqueIndexInKeys() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_ab ON t (a, b)");

        TableSchema schema = catalog.getTable("public", "t");
        MiniDbCalciteTable table = new MiniDbCalciteTable(schema, catalog);
        List<ImmutableBitSet> keys = table.getStatistic().getKeys();

        // 复合 UNIQUE (a,b) → 列索引 1,2
        assertTrue(keys.stream().anyMatch(k -> k.equals(ImmutableBitSet.of(1, 2))),
                "复合 UNIQUE 索引 (a,b) 应在 keys 中");
    }
}