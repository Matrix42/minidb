package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MaterializedViewTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        executor = new QueryExecutor(catalog, storage, allocator, null);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void createAndQueryMaterializedView() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t WHERE id > 1");

        // 验证 MV 数据
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(
                "SELECT id FROM mv ORDER BY id")).data();
        IntVector iv = (IntVector) root.getVector("id");
        assertEquals(2, iv.getValueCount());
        assertEquals(2, iv.get(0));
        assertEquals(3, iv.get(1));
        root.close();
    }

    @Test
    void dropMaterializedView() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM t");

        assertTrue(catalog.hasMaterializedView("public", "mv"));
        assertTrue(catalog.hasTable("public", "mv"));

        executor.execute("DROP MATERIALIZED VIEW mv");

        assertFalse(catalog.hasMaterializedView("public", "mv"));
        assertFalse(catalog.hasTable("public", "mv"));
    }

    @Test
    void refreshMaterializedView() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t WHERE id > 1");

        // 插入新行后 REFRESH
        executor.execute("INSERT INTO t VALUES (3, 'c')");
        executor.execute("REFRESH MATERIALIZED VIEW mv");

        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(
                "SELECT id FROM mv ORDER BY id")).data();
        IntVector iv = (IntVector) root.getVector("id");
        assertEquals(2, iv.getValueCount());
        assertEquals(2, iv.get(0));
        assertEquals(3, iv.get(1));
        root.close();
    }

    @Test
    void createMaterializedViewWithAggregate() {
        executor.execute("CREATE TABLE t (g INTEGER, v INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)");

        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT g, SUM(v) AS s FROM t GROUP BY g");

        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(
                "SELECT * FROM mv ORDER BY g")).data();
        assertEquals(2, root.getRowCount());
        IntVector gv = (IntVector) root.getVector("g");
        assertEquals(1, gv.get(0));
        assertEquals(2, gv.get(1));
        root.close();
    }

    @Test
    void rejectMultiTableJoinMV() {
        executor.execute("CREATE TABLE t1 (id INTEGER)");
        executor.execute("CREATE TABLE t2 (id INTEGER)");

        assertThrows(UnsupportedOperationException.class, () ->
                executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id"));
    }

    @Test
    void rejectDropTableWithDependentMV() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM t");

        // DROP TABLE 应该拒绝（有 MV 依赖）
        assertThrows(IllegalArgumentException.class, () -> executor.execute("DROP TABLE t"));
    }

    @Test
    void mvTableTypeIsCorrect() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM t");

        TableSchema ts = catalog.getTable("public", "mv");
        assertEquals(TableType.MATERIALIZED_VIEW, ts.tableType());
        assertNotNull(ts.mvDefinition());
        assertEquals("mv", ts.mvDefinition().name());
    }

    @Test
    void truncateBaseTableClearsDependentMV() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t WHERE id > 1");

        // 验证 MV 有数据
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(
                "SELECT * FROM mv")).data();
        assertEquals(2, root.getRowCount());
        root.close();

        // TRUNCATE 基表
        executor.execute("TRUNCATE TABLE t");

        // MV 应该被清空
        root = ((QueryResult.Rows) executor.execute("SELECT * FROM mv")).data();
        assertEquals(0, root.getRowCount());
        root.close();
    }

    @Test
    void dmlAutoRefreshMv() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t WHERE id > 1");

        // INSERT 后自动刷新
        executor.execute("INSERT INTO t VALUES (4, 'd')");
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(
                "SELECT id FROM mv ORDER BY id")).data();
        IntVector iv = (IntVector) root.getVector("id");
        assertEquals(3, iv.getValueCount());
        assertEquals(2, iv.get(0));
        assertEquals(3, iv.get(1));
        assertEquals(4, iv.get(2));
        root.close();

        // DELETE 后自动刷新
        executor.execute("DELETE FROM t WHERE id = 3");
        root = ((QueryResult.Rows) executor.execute(
                "SELECT id FROM mv ORDER BY id")).data();
        iv = (IntVector) root.getVector("id");
        assertEquals(2, iv.getValueCount());
        assertEquals(2, iv.get(0));
        assertEquals(4, iv.get(1));
        root.close();
    }
}