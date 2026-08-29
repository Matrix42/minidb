package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ViewTest {

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

    private int[] intCol(String sql, String col) {
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        IntVector v = (IntVector) root.getVector(col);
        int[] out = new int[v.getValueCount()];
        for (int i = 0; i < out.length; i++) {
            out[i] = v.get(i);
        }
        root.close();
        return out;
    }

    private String[] strCol(String sql, String col) {
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        VarCharVector v = (VarCharVector) root.getVector(col);
        String[] out = new String[v.getValueCount()];
        for (int i = 0; i < out.length; i++) {
            out[i] = v.isNull(i) ? null : new String(v.get(i), StandardCharsets.UTF_8);
        }
        root.close();
        return out;
    }

    @Test
    void createAndQueryView() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1,'a'), (2,'b'), (3,'c')");
        executor.execute("CREATE VIEW v AS SELECT id, name FROM t WHERE id > 1");
        int[] ids = intCol("SELECT id FROM v ORDER BY id", "id");
        assertEquals(2, ids.length);
        assertEquals(2, ids[0]);
        assertEquals(3, ids[1]);
    }

    @Test
    void viewSupportsAdditionalFilter() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3), (4)");
        executor.execute("CREATE VIEW v AS SELECT id FROM t WHERE id > 1");
        int[] ids = intCol("SELECT id FROM v WHERE id <= 3 ORDER BY id", "id");
        assertEquals(2, ids.length);
        assertEquals(2, ids[0]);
        assertEquals(3, ids[1]);
    }

    @Test
    void createOrReplaceView() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1,'a'), (2,'b')");
        executor.execute("CREATE VIEW v AS SELECT id FROM t");
        executor.execute("CREATE OR REPLACE VIEW v AS SELECT name FROM t WHERE id = 2");
        String[] names = strCol("SELECT name FROM v", "name");
        assertEquals(1, names.length);
        assertEquals("b", names[0]);
    }

    @Test
    void dropView() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("CREATE VIEW v AS SELECT id FROM t");
        executor.execute("DROP VIEW v");
        assertThrows(Exception.class, () -> executor.execute("SELECT * FROM v"));
        // DROP VIEW IF EXISTS 对不存在的视图是 no-op
        executor.execute("DROP VIEW IF EXISTS v");
    }

    @Test
    void createViewWithColumnList() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1,'a'), (2,'b')");
        executor.execute("CREATE VIEW v (num, label) AS SELECT id, name FROM t");
        int[] nums = intCol("SELECT num FROM v ORDER BY num", "num");
        assertEquals(2, nums.length);
        assertEquals(1, nums[0]);
        assertEquals(2, nums[1]);
        String[] labels = strCol("SELECT label FROM v WHERE num = 2", "label");
        assertEquals(1, labels.length);
        assertEquals("b", labels[0]);
    }

    @Test
    void createViewColumnListMismatch() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        assertThrows(
                Exception.class,
                () -> executor.execute("CREATE VIEW v (a) AS SELECT id, name FROM t"));
    }

    @Test
    void nestedView() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        executor.execute("CREATE VIEW v1 AS SELECT id FROM t WHERE id > 1");
        executor.execute("CREATE VIEW v2 AS SELECT id FROM v1 WHERE id < 3");
        int[] ids = intCol("SELECT id FROM v2 ORDER BY id", "id");
        assertEquals(1, ids.length);
        assertEquals(2, ids[0]);
    }

    @Test
    void viewPersistsAcrossRestart() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("CREATE VIEW v AS SELECT id FROM t WHERE id = 1");

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        BufferAllocator allocator2 = new RootAllocator();
        StorageManager storage2 = new StorageManager(catalog2, allocator2, dataDir);
        storage2.loadAll();
        QueryExecutor executor2 =
                new QueryExecutor(catalog2, storage2, allocator2, new StatsManager(storage2));
        try {
            int[] ids = new int[0];
            VectorSchemaRoot root =
                    ((QueryResult.Rows) executor2.execute("SELECT id FROM v")).data();
            IntVector v = (IntVector) root.getVector("id");
            ids = new int[v.getValueCount()];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = v.get(i);
            }
            root.close();
            assertEquals(1, ids.length);
            assertEquals(1, ids[0]);
        } finally {
            storage2.close();
            allocator2.close();
        }
    }
}
