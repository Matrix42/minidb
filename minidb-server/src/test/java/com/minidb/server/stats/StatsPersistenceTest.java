package com.minidb.server.stats;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.storage.JsonCatalogStore;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatsPersistenceTest {

    @TempDir Path dataDir;

    @Test
    void analyzePersistsAndSurvivesRestart() throws Exception {
        // 第一次会话:建表、插数据、analyze、关闭(flush)
        {
            BufferAllocator allocator = new RootAllocator();
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor q = new QueryExecutor(catalog, storage, allocator, stats);
            q.execute("CREATE TABLE t (id INTEGER)");
            q.execute("INSERT INTO t VALUES (1), (2), (2)");
            stats.analyze("t");
            assertEquals(3, catalog.getStats("public", "t").rowCount());
            storage.close();
            allocator.close();
        }
        // 第二次会话:只 loadAll,统计应从 catalog.json 恢复
        {
            BufferAllocator allocator = new RootAllocator();
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            storage.loadAll();
            assertNotNull(catalog.getStats("public", "t"));
            assertEquals(3, catalog.getStats("public", "t").rowCount());
            // catalog.json 确实含统计
            JsonCatalogStore store = new JsonCatalogStore(dataDir.resolve("catalog.json"));
            assertNotNull(store.load().stats().get("public.t"));
            storage.close();
            allocator.close();
        }
    }

    @Test
    void dmlMarksStatsStaleAndDropRemovesStats() {
        BufferAllocator allocator = new RootAllocator();
        MiniDbCatalog catalog = new MiniDbCatalog();
        StorageManager storage = new StorageManager(catalog, allocator, dataDir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor q = new QueryExecutor(catalog, storage, allocator, stats);
        q.execute("CREATE TABLE t (id INTEGER)");
        q.execute("INSERT INTO t VALUES (1)");
        stats.analyze("t");
        q.execute("INSERT INTO t VALUES (2)"); // DML → markStatsStale
        assertTrue(catalog.getStats("public", "t").stale());
        q.execute("DROP TABLE t");
        assertNull(catalog.getStats("public", "t"));
        storage.close();
        allocator.close();
    }

    @Test
    void staleFlagSurvivesRestart() {
        // 第一次会话:analyze 后 DML 置 stale,关闭(flush + persist catalog)。
        {
            BufferAllocator allocator = new RootAllocator();
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor q = new QueryExecutor(catalog, storage, allocator, stats);
            q.execute("CREATE TABLE t (id INTEGER)");
            q.execute("INSERT INTO t VALUES (1)");
            stats.analyze("t");
            q.execute("INSERT INTO t VALUES (2)"); // 置 stale
            storage.close();
            allocator.close();
        }
        // 第二次会话:loadAll 后 stale 标记应仍在(而非误判新鲜)。
        {
            BufferAllocator allocator = new RootAllocator();
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            storage.loadAll();
            assertNotNull(catalog.getStats("public", "t"));
            assertTrue(catalog.getStats("public", "t").stale());
            storage.close();
            allocator.close();
        }
    }
}
