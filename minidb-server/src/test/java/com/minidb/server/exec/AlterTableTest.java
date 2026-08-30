package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlterTableTest {

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

    private List<Object> column(String sql, String col) {
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        List<Object> out = new ArrayList<>();
        for (int i = 0; i < root.getRowCount(); i++) {
            out.add(root.getVector(col).getObject(i));
        }
        root.close();
        return out;
    }

    @Test
    void addColumnFillsNullForExistingRows() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("ALTER TABLE t ADD name VARCHAR");
        assertEquals(List.of(1, 2), column("SELECT id FROM t ORDER BY id", "id"));
        assertEquals(Arrays.asList(null, null), column("SELECT name FROM t ORDER BY id", "name"));
    }

    @Test
    void addColumnWithDefault() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("ALTER TABLE t ADD extra INTEGER DEFAULT 42");
        assertEquals(List.of(42, 42), column("SELECT extra FROM t ORDER BY id", "extra"));
    }

    @Test
    void addNotNullColumnWithoutDefaultThrows() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        assertThrows(
                Exception.class, () -> executor.execute("ALTER TABLE t ADD c INTEGER NOT NULL"));
    }

    @Test
    void addNotNullColumnWithDefault() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        executor.execute("ALTER TABLE t ADD c INTEGER NOT NULL DEFAULT 7");
        assertEquals(List.of(7), column("SELECT c FROM t", "c"));
        assertThrows(
                Exception.class, () -> executor.execute("INSERT INTO t (id, c) VALUES (2, NULL)"));
    }

    @Test
    void dropColumn() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        executor.execute("ALTER TABLE t DROP COLUMN name");
        assertEquals(List.of(1, 2), column("SELECT id FROM t ORDER BY id", "id"));
        assertEquals(1, catalog.getTable("public", "t").columns().size());
    }

    @Test
    void dropConstrainedColumnThrows() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR)");
        assertThrows(Exception.class, () -> executor.execute("ALTER TABLE t DROP COLUMN id"));
    }

    @Test
    void renameColumn() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("ALTER TABLE t RENAME COLUMN id TO uid");
        assertEquals(List.of(1, 2), column("SELECT uid FROM t ORDER BY uid", "uid"));
    }

    @Test
    void renameTable() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("ALTER TABLE t RENAME TO t2");
        assertEquals(List.of(1, 2), column("SELECT id FROM t2 ORDER BY id", "id"));
    }

    @Test
    void alterTypeIntToBigInt() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("ALTER TABLE t ALTER COLUMN id SET DATA TYPE BIGINT");
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("SELECT id FROM t ORDER BY id")).data();
        assertTrue(root.getVector("id") instanceof BigIntVector);
        assertEquals(1L, ((BigIntVector) root.getVector("id")).get(0));
        root.close();
    }

    @Test
    void alterTypeIncompatibleThrows() {
        executor.execute("CREATE TABLE t (s VARCHAR)");
        executor.execute("INSERT INTO t VALUES ('abc')");
        assertThrows(
                Exception.class,
                () -> executor.execute("ALTER TABLE t ALTER COLUMN s SET DATA TYPE INTEGER"));
    }

    @Test
    void setNotNullThenRejectNullInsert() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        executor.execute("ALTER TABLE t ALTER id SET NOT NULL");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO t VALUES (NULL)"));
    }

    @Test
    void setNotNullWithExistingNullThrows() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (NULL)");
        assertThrows(
                Exception.class, () -> executor.execute("ALTER TABLE t ALTER id SET NOT NULL"));
    }

    @Test
    void addPrimaryKeyViolatesOnDuplicate() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (1)");
        assertThrows(Exception.class, () -> executor.execute("ALTER TABLE t ADD PRIMARY KEY (id)"));
    }

    @Test
    void addPrimaryKeyOnUniqueData() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("ALTER TABLE t ADD PRIMARY KEY (id)");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO t VALUES (1)"));
    }

    @Test
    void addForeignKeyValidatesExistingRows() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute("INSERT INTO parent VALUES (1)");
        executor.execute("CREATE TABLE child (pid INTEGER)");
        executor.execute("INSERT INTO child VALUES (1)");
        executor.execute("ALTER TABLE child ADD FOREIGN KEY (pid) REFERENCES parent (id)");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO child VALUES (99)"));
    }

    @Test
    void addForeignKeyViolatesOnMissingRef() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute("INSERT INTO parent VALUES (1)");
        executor.execute("CREATE TABLE child (pid INTEGER)");
        executor.execute("INSERT INTO child VALUES (99)");
        assertThrows(
                Exception.class,
                () ->
                        executor.execute(
                                "ALTER TABLE child ADD FOREIGN KEY (pid) REFERENCES parent (id)"));
    }

    @Test
    void dropPrimaryKey() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        executor.execute("INSERT INTO t VALUES (1)");
        executor.execute("ALTER TABLE t DROP PRIMARY KEY");
        executor.execute("INSERT INTO t VALUES (1)");
        assertEquals(List.of(1, 1), column("SELECT id FROM t ORDER BY id", "id"));
    }

    @Test
    void dropNotNull() {
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL)");
        executor.execute("INSERT INTO t VALUES (1)");
        executor.execute("ALTER TABLE t ALTER id DROP NOT NULL");
        executor.execute("INSERT INTO t VALUES (NULL)");
        assertNull(column("SELECT id FROM t ORDER BY id", "id").get(1));
    }
}
