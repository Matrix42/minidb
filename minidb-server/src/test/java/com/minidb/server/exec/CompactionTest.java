package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompactionTest {

    @TempDir Path dataDir;
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

    private static long countRows(QueryResult r) {
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        try {
            return root.getRowCount();
        } finally {
            root.close();
        }
    }

    @Test
    void compactMergesPartsAndPreservesData() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        for (int i = 1; i <= 5; i++) {
            executor.execute("INSERT INTO t VALUES (" + i + ")");
        }
        assertEquals(5, storage.getTable("public", "t").partCount());

        executor.execute("COMPACT TABLE t");
        assertEquals(1, storage.getTable("public", "t").partCount());

        QueryResult r = executor.execute("SELECT id FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(5, root.getRowCount());
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(1, id.get(0));
        assertEquals(5, id.get(4));
        root.close();
    }

    @Test
    void compactParquetPreservesData() {
        executor.execute("CREATE TABLE t (id INTEGER) WITH ('format'='parquet')");
        for (int i = 1; i <= 3; i++) {
            executor.execute("INSERT INTO t VALUES (" + i + ")");
        }
        assertEquals(3, storage.getTable("public", "t").partCount());

        executor.execute("COMPACT TABLE t");
        assertEquals(1, storage.getTable("public", "t").partCount());
        assertEquals(3, countRows(executor.execute("SELECT * FROM t")));
    }

    @Test
    void autoCompactsOnThreshold() throws Exception {
        Files.writeString(
                dataDir.resolve("config.yaml"), "compaction:\n  auto-part-threshold: 3\n");
        // 低阈值配置需在 StorageManager 构造前生效,这里用独立存储验证自动触发。
        MiniDbCatalog catalog2 = new MiniDbCatalog();
        StorageManager storage2 = new StorageManager(catalog2, allocator, dataDir);
        try {
            QueryExecutor executor2 =
                    new QueryExecutor(catalog2, storage2, allocator, new StatsManager(storage2));
            executor2.execute("CREATE TABLE t (id INTEGER)");
            for (int i = 1; i <= 4; i++) {
                executor2.execute("INSERT INTO t VALUES (" + i + ")");
            }
            // 第 4 次 INSERT 后 part 数超阈值,自动合并成一个。
            assertEquals(1, storage2.getTable("public", "t").partCount());
            assertEquals(4, storage2.getTable("public", "t").rowCount());
        } finally {
            storage2.close();
        }
    }
}
