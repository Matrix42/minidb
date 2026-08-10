package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryExecutorTest {

    @TempDir
    Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        executor = new QueryExecutor(catalog, storage, allocator);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void createTableInsertSelect() {
        QueryResult ddl = executor.execute(
                "CREATE TABLE t (id INTEGER, name VARCHAR)");
        assertEquals(0L, ((QueryResult.Update) ddl).count());

        QueryResult insert = executor.execute(
                "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertEquals(3L, ((QueryResult.Update) insert).count());

        QueryResult select = executor.execute(
                "SELECT id, name FROM t WHERE id > 1 ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(2, ((IntVector) root.getVector("id")).get(0));
        assertEquals("b",
                new String(((VarCharVector) root.getVector("name")).get(0)));
        root.close();
    }

    @Test
    void limitTrimsRows() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (3), (1), (2)");
        QueryResult select = executor.execute(
                "SELECT id FROM t ORDER BY id LIMIT 2");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        assertEquals(2, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void insertSelectCopiesRows() {
        executor.execute("CREATE TABLE src (id INTEGER)");
        executor.execute("INSERT INTO src VALUES (1), (2)");
        executor.execute("CREATE TABLE dst (id INTEGER)");
        QueryResult r = executor.execute("INSERT INTO dst SELECT id FROM src");
        assertEquals(2L, ((QueryResult.Update) r).count());
        assertEquals(2L, storage.getTable("dst").rowCount());
    }

    @Test
    void updateModifiesRows() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        QueryResult update = executor.execute(
                "UPDATE t SET name = 'x' WHERE id = 1");
        assertEquals(1L, ((QueryResult.Update) update).count());

        QueryResult select = executor.execute(
                "SELECT id, name FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(3, root.getRowCount());
        assertEquals("x", new String(((VarCharVector) root.getVector("name")).get(0)));
        assertEquals("b", new String(((VarCharVector) root.getVector("name")).get(1)));
        assertEquals("c", new String(((VarCharVector) root.getVector("name")).get(2)));
        root.close();
    }

    @Test
    void updateIntegerColumnWithLiteral() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        QueryResult update = executor.execute("UPDATE t SET id = 10 WHERE id = 1");
        assertEquals(1L, ((QueryResult.Update) update).count());

        QueryResult select = executor.execute("SELECT id, name FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(2, ((IntVector) root.getVector("id")).get(0));
        assertEquals(10, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void updateNoMatchLeavesTableUnchanged() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        QueryResult update = executor.execute("UPDATE t SET name = 'x' WHERE id = 99");
        assertEquals(0L, ((QueryResult.Update) update).count());
        assertEquals(2L, storage.getTable("t").rowCount());
    }

    @Test
    void deleteRemovesRows() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        QueryResult delete = executor.execute("DELETE FROM t WHERE id = 2");
        assertEquals(1L, ((QueryResult.Update) delete).count());

        QueryResult select = executor.execute("SELECT id FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        assertEquals(3, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void dropTableRemovesIt() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("DROP TABLE t");
        assertThrows(Exception.class,
                () -> executor.execute("SELECT * FROM t"));
    }

    @Test
    void truncateEmptiesTableButKeepsSchema() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        QueryResult truncate = executor.execute("TRUNCATE TABLE t");
        assertEquals(0L, ((QueryResult.Update) truncate).count());
        assertEquals(0L, storage.getTable("t").rowCount());
        assertTrue(catalog.hasTable("t"));
        // SELECT over the now-empty table behaves like any empty scan:
        // no batches, so the executor reports the empty result as an error.
        assertThrows(IllegalStateException.class,
                () -> executor.execute("SELECT id, name FROM t"));
    }

    @Test
    void truncateAllowsReinsert() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a')");
        executor.execute("TRUNCATE TABLE t");
        QueryResult insert = executor.execute("INSERT INTO t VALUES (2, 'b')");
        assertEquals(1L, ((QueryResult.Update) insert).count());
        assertEquals(1L, storage.getTable("t").rowCount());
    }

    @Test
    void truncateMissingTableThrows() {
        assertThrows(Exception.class,
                () -> executor.execute("TRUNCATE TABLE nope"));
    }

    @Test
    void badSqlThrowsWithMessage() {
        Exception e = assertThrows(Exception.class,
                () -> executor.execute("SELEC nope"));
        assertTrue(e.getMessage() != null);
    }
}
