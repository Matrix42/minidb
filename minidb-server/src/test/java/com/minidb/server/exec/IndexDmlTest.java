package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.TableHandle;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexDmlTest {

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

    @Test
    void insertMaintainsIndex() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 10, 'z')");

        TableHandle idx = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(idx);
        assertEquals(3, idx.rowCount(), "索引表行数应等于数据行数");

        // 扫描索引表,验证内容:列序为 (a, id)
        Set<Integer> seenIds = new HashSet<>();
        try (BatchIterator it = idx.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                IntVector aCol = (IntVector) batch.getVector("a");
                IntVector idCol = (IntVector) batch.getVector("id");
                for (int i = 0; i < batch.getRowCount(); i++) {
                    int a = aCol.get(i);
                    int id = idCol.get(i);
                    assertTrue(a == 10 || a == 20, "索引列值应为 10 或 20,实际 " + a);
                    seenIds.add(id);
                }
            }
        }
        assertEquals(3, seenIds.size(), "索引表应包含 3 个不同的主键");
    }

    @Test
    void updateIndexedColumnChangesIndex() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        // 改索引列值:旧值 10 → 15
        executor.execute("UPDATE t SET a = 15 WHERE id = 1");

        TableHandle idx = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(idx);
        assertEquals(3, idx.rowCount(), "索引表行数应不变");

        // 扫描索引表:确认旧值 10 已移除,新值 15 存在
        Set<Integer> aValues = new HashSet<>();
        try (BatchIterator it = idx.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                IntVector aCol = (IntVector) batch.getVector("a");
                for (int i = 0; i < batch.getRowCount(); i++) {
                    aValues.add(aCol.get(i));
                }
            }
        }
        assertTrue(aValues.contains(15), "索引表应包含新值 15");
        assertTrue(aValues.contains(20));
        assertTrue(aValues.contains(30));
        assertEquals(3, aValues.size());
    }

    @Test
    void updatePrimaryKeyMaintainsIndex() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y')");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        // 改主键值:旧主键 1 → 新主键 5
        executor.execute("UPDATE t SET id = 5 WHERE id = 1");

        TableHandle idx = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(idx);
        assertEquals(2, idx.rowCount(), "索引表行数应不变");

        // 扫描索引表:确认主键 5(a=10)存在,主键 1 已移除
        Set<Integer> seenIds = new HashSet<>();
        try (BatchIterator it = idx.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                IntVector aCol = (IntVector) batch.getVector("a");
                IntVector idCol = (IntVector) batch.getVector("id");
                for (int i = 0; i < batch.getRowCount(); i++) {
                    seenIds.add(idCol.get(i));
                    if (idCol.get(i) == 5) {
                        assertEquals(10, aCol.get(i), "主键 5 的索引列值应为 10");
                    }
                }
            }
        }
        assertTrue(seenIds.contains(5), "索引表应包含新主键 5");
        assertEquals(2, seenIds.size());
    }

    @Test
    void deleteRemovesIndexEntries() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        executor.execute("DELETE FROM t WHERE a = 10");

        TableHandle idx = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(idx);
        assertEquals(2, idx.rowCount(), "删除 1 行后索引表应剩 2 行");

        // 确认索引表不再包含 id=1
        Set<Integer> seenIds = new HashSet<>();
        try (BatchIterator it = idx.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                IntVector idCol = (IntVector) batch.getVector("id");
                for (int i = 0; i < batch.getRowCount(); i++) {
                    seenIds.add(idCol.get(i));
                }
            }
        }
        assertEquals(Set.of(2, 3), seenIds);

        // 数据表查询也应无残留
        QueryResult.Rows rows = (QueryResult.Rows) executor.execute("SELECT id FROM t ORDER BY id");
        List<Integer> dataIds = new ArrayList<>();
        for (int i = 0; i < rows.data().getRowCount(); i++) {
            dataIds.add(((IntVector) rows.data().getVector("id")).get(i));
        }
        rows.data().close();
        assertEquals(List.of(2, 3), dataIds);
    }

    @Test
    void truncateClearsIndex() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y')");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        executor.execute("TRUNCATE TABLE t");

        TableHandle idx = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(idx);
        assertEquals(0, idx.rowCount(), "TRUNCATE 后索引表应空");

        // 数据表也应空
        QueryResult.Rows rows = (QueryResult.Rows) executor.execute("SELECT count(*) FROM t");
        long count = ((org.apache.arrow.vector.BigIntVector) rows.data().getVector(0)).get(0);
        rows.data().close();
        assertEquals(0, count, "TRUNCATE 后数据表应空");
    }

    @Test
    void reconciliationRandomDml() {
        // 两张表:一张带索引、一张不带。相同 DML 序列后,数据一致。
        executor.execute("CREATE TABLE noidx_t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("CREATE TABLE idx_t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("CREATE INDEX idx_a ON idx_t (a)");

        // 初始数据
        executor.execute("INSERT INTO idx_t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        executor.execute("INSERT INTO noidx_t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");

        // DML:INSERT/UPDATE/DELETE 混合
        executor.execute("UPDATE idx_t SET a = 15 WHERE id = 1");
        executor.execute("UPDATE noidx_t SET a = 15 WHERE id = 1");
        executor.execute("DELETE FROM idx_t WHERE id = 3");
        executor.execute("DELETE FROM noidx_t WHERE id = 3");
        executor.execute("INSERT INTO idx_t VALUES (5, 25, 'new')");
        executor.execute("INSERT INTO noidx_t VALUES (5, 25, 'new')");

        // 对账:两表 rowCount 一致
        long c1 = storage.getTable("public", "idx_t").rowCount();
        long c2 = storage.getTable("public", "noidx_t").rowCount();
        assertEquals(c2, c1, "两表 rowCount 应一致");
        assertEquals(3, c1, "DML 后应有 3 行");

        // 索引表行数应与数据表一致
        TableHandle idx = storage.indexManager().getIndex("public", "idx_t", "idx_a");
        assertNotNull(idx);
        assertEquals(c1, idx.rowCount(), "索引表行数应与数据表一致");

        // 扫描索引表验证内容:列序 (a, id)
        try (BatchIterator it = idx.scan()) {
            int scanned = 0;
            while (it.hasNext()) {
                scanned += it.next().getRowCount();
            }
            assertEquals(c1, scanned, "索引表扫描行数应与数据表一致");
        }
    }
}
