package com.minidb.server.storage;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.stats.StatsManager;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.IndexDef;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.common.TableSchema;

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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexStorageTest {

    @TempDir Path dataDir;

    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void createIndexDirectoryAndRowCountMatchData() {
        QueryExecutor executor =
                new QueryExecutor(catalog, storage, allocator, new StatsManager(storage));
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 10, 'z')");

        TableSchema data = catalog.getTable("public", "t");
        IndexDef def = new IndexDef("idx_a", false, List.of("a"));
        TableHandle dataTable = storage.getTable("public", "t");
        TableHandle indexTable = storage.indexManager().createIndex("public", "t", def, data);
        storage.indexManager().populateFromTable(def, dataTable, indexTable);

        Path idxDir = dataDir.resolve("public").resolve("t").resolve(".indexes").resolve("idx_a");
        assertTrue(Files.isDirectory(idxDir), "索引目录应存在");
        assertEquals(3, indexTable.rowCount(), "索引表行数应等于数据行数");
    }

    @Test
    void populatedIndexPrefixScanReturnsPrimaryKeys() {
        QueryExecutor executor =
                new QueryExecutor(catalog, storage, allocator, new StatsManager(storage));
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 10, 'z')");

        TableSchema data = catalog.getTable("public", "t");
        IndexDef def = new IndexDef("idx_a", false, List.of("a"));
        TableHandle dataTable = storage.getTable("public", "t");
        TableHandle indexTable = storage.indexManager().createIndex("public", "t", def, data);
        storage.indexManager().populateFromTable(def, dataTable, indexTable);

        // scan(lo,hi) 是超集语义:先用 a=10 前缀扫,再在返回行中按 a==10 过滤。
        try (BatchIterator it = indexTable.scan(List.of(10), List.of(10))) {
            int count = 0;
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                IntVector a = (IntVector) batch.getVector("a");
                IntVector pk = (IntVector) batch.getVector("id");
                for (int i = 0; i < batch.getRowCount(); i++) {
                    if (a.get(i) != 10) {
                        continue; // 超集语义,过滤边界外相邻键
                    }
                    int id = pk.get(i);
                    assertTrue(id == 1 || id == 3, "主键应为 1 或 3,实际 " + id);
                    count++;
                }
            }
            assertEquals(2, count, "a=10 应有 2 行");
        }
    }

    @Test
    void dropIndexRemovesDirectoryAndHandle() {
        QueryExecutor executor =
                new QueryExecutor(catalog, storage, allocator, new StatsManager(storage));
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10), (2, 20)");

        TableSchema data = catalog.getTable("public", "t");
        IndexDef def = new IndexDef("idx_a", false, List.of("a"));
        storage.indexManager().createIndex("public", "t", def, data);

        Path idxDir = dataDir.resolve("public").resolve("t").resolve(".indexes").resolve("idx_a");
        assertTrue(Files.isDirectory(idxDir));

        storage.indexManager().dropIndex("public", "t", "idx_a");
        assertTrue(Files.notExists(idxDir), "dropIndex 后目录应被删除");
        assertNull(storage.indexManager().getIndex("public", "t", "idx_a"), "句柄应被移除");
    }

    @Test
    void restartRecoveryRebuildsIndexHandles() throws Exception {
        QueryExecutor executor =
                new QueryExecutor(catalog, storage, allocator, new StatsManager(storage));
        executor.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");

        TableSchema data = catalog.getTable("public", "t");
        IndexDef def = new IndexDef("idx_a", false, List.of("a"));
        TableHandle dataTable = storage.getTable("public", "t");
        TableHandle indexTable = storage.indexManager().createIndex("public", "t", def, data);
        storage.indexManager().populateFromTable(def, dataTable, indexTable);
        // 让 catalog 元数据也记录索引,loadAll 才能恢复
        catalog.alterTable("public", "t", data.withIndexes(List.of(def)));

        long before = indexTable.rowCount();

        // 模拟重启:close 后重建 StorageManager,从磁盘恢复
        storage.close();
        allocator.close();

        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        storage.loadAll();

        TableHandle recovered = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(recovered, "重启后应能取到索引句柄");
        assertEquals(before, recovered.rowCount(), "重启后索引行数应与之前一致");
    }

    @Test
    void compositeIndexSchemaOrderAndEmptyTable() {
        QueryExecutor executor =
                new QueryExecutor(catalog, storage, allocator, new StatsManager(storage));
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b VARCHAR)");
        // 空表
        TableSchema data = catalog.getTable("public", "t");
        IndexDef def = new IndexDef("idx_ab", false, List.of("a", "b"));
        TableHandle indexTable = storage.indexManager().createIndex("public", "t", def, data);
        storage.indexManager().populateFromTable(def, storage.getTable("public", "t"), indexTable);

        TableSchema idxSchema = indexTable.schema();
        assertEquals(3, idxSchema.columns().size(), "复合索引(2 列)+ 主键(1 列)= 3 列");
        assertEquals("a", idxSchema.columns().get(0).name());
        assertEquals("b", idxSchema.columns().get(1).name());
        assertEquals("id", idxSchema.columns().get(2).name());
        assertEquals(List.of("a", "b", "id"), idxSchema.primaryKey(), "PK = 全部列");
        assertEquals(0, indexTable.rowCount(), "空表索引行数应为 0");
    }
}
