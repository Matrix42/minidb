package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.IndexDef;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.common.TableSchema;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexDdlTest {

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
    void createIndexBasic() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y')");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        // 目录存在
        Path idxDir = dataDir.resolve("public").resolve("t").resolve(".indexes").resolve("idx_a");
        assertTrue(Files.isDirectory(idxDir), "索引目录应存在");

        // 元数据含 def
        TableSchema data = catalog.getTable("public", "t");
        assertNotNull(data.indexes());
        assertEquals(1, data.indexes().size());
        IndexDef def = data.indexes().get(0);
        assertEquals("idx_a", def.name());
        assertEquals(false, def.unique());
        assertEquals(List.of("a"), def.columns());

        // 索引表行数正确
        TableHandle idx = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(idx);
        assertEquals(2, idx.rowCount());
    }

    @Test
    void createUniqueCompositeIndex() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("CREATE UNIQUE INDEX idx_ab ON t (a, b)");

        TableSchema data = catalog.getTable("public", "t");
        assertEquals(1, data.indexes().size());
        IndexDef def = data.indexes().get(0);
        assertEquals("idx_ab", def.name());
        assertEquals(true, def.unique());
        assertEquals(List.of("a", "b"), def.columns());
    }

    @Test
    void dropIndexRemovesMetadataAndDirectory() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        Path idxDir = dataDir.resolve("public").resolve("t").resolve(".indexes").resolve("idx_a");
        assertTrue(Files.isDirectory(idxDir));

        executor.execute("DROP INDEX idx_a ON t");

        assertTrue(Files.notExists(idxDir), "drop 后目录应被删除");
        TableSchema data = catalog.getTable("public", "t");
        assertTrue(data.indexes().isEmpty(), "元数据中索引应被移除");
    }

    @Test
    void dropIndexIfExistsNoop() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        // IF EXISTS 不存在的索引不抛错
        executor.execute("DROP INDEX IF EXISTS nope ON t");
    }

    @Test
    void dropIndexMissingThrows() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        assertThrows(
                IllegalArgumentException.class, () -> executor.execute("DROP INDEX nope ON t"));
    }

    @Test
    void noPrimaryKeyTableThrows() {
        executor.execute("CREATE TABLE t (id INTEGER, a INTEGER)");
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("CREATE INDEX idx_a ON t (a)"));
    }

    @Test
    void simpleTableThrows() {
        executor.execute(
                "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER) WITH ('type' = 'simple')");
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("CREATE INDEX idx_a ON t (a)"));
    }

    @Test
    void doubleColumnThrows() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DOUBLE, a INTEGER)");
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("CREATE INDEX idx_d ON t (d)"));
    }

    @Test
    void missingColumnThrows() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("CREATE INDEX idx_x ON t (x)"));
    }

    @Test
    void duplicateIndexNameThrows() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        executor.execute("CREATE INDEX idx_a ON t (a)");
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("CREATE INDEX idx_a ON t (b)"));
    }

    @Test
    void duplicateColumnsInIndexThrows() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("CREATE INDEX idx ON t (a, a)"));
    }

    @Test
    void restartRecoveryIndexMetadataPreserved() throws Exception {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        executor.execute("CREATE INDEX idx_a ON t (a)");

        long before = storage.indexManager().getIndex("public", "t", "idx_a").rowCount();

        storage.close();
        allocator.close();

        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        storage.loadAll();

        TableSchema data = catalog.getTable("public", "t");
        assertEquals(1, data.indexes().size(), "重启后索引元数据应保留");
        assertEquals("idx_a", data.indexes().get(0).name());

        TableHandle recovered = storage.indexManager().getIndex("public", "t", "idx_a");
        assertNotNull(recovered, "重启后应能取到索引句柄");
        assertEquals(before, recovered.rowCount(), "重启后索引行数应与之前一致");
    }

    @Test
    void schemaQualifiedDropIndex() {
        executor.execute("CREATE SCHEMA other");
        executor.execute("CREATE TABLE other.t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("CREATE INDEX idx_a ON other.t (a)");

        Path idxDir = dataDir.resolve("other").resolve("t").resolve(".indexes").resolve("idx_a");
        assertTrue(Files.isDirectory(idxDir));

        executor.execute("DROP INDEX idx_a ON other.t");

        assertTrue(Files.notExists(idxDir));
        TableSchema data = catalog.getTable("other", "t");
        assertTrue(data.indexes().isEmpty());
    }
}
