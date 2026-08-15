package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.StorageFormat;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageFormatTest {

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
    void defaultIsArrow() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        assertEquals(StorageFormat.ARROW, catalog.getTable("public", "t").storageFormat());
        executor.execute("INSERT INTO t VALUES (1)");
        assertEquals(1, storage.getTable("public", "t").rowCount());
    }

    @Test
    void explicitArrowFormat() {
        executor.execute("CREATE TABLE t (id INTEGER) FORMAT arrow");
        assertEquals(StorageFormat.ARROW, catalog.getTable("public", "t").storageFormat());
        executor.execute("INSERT INTO t VALUES (1)");
    }

    @Test
    void parquetFormatRecorded() {
        executor.execute("CREATE TABLE t (id INTEGER) FORMAT parquet");
        assertEquals(StorageFormat.PARQUET, catalog.getTable("public", "t").storageFormat());
    }

    @Test
    void parquetInsertThrowsNotImplemented() {
        executor.execute("CREATE TABLE t (id INTEGER) FORMAT parquet");
        assertThrows(UnsupportedOperationException.class,
                () -> executor.execute("INSERT INTO t VALUES (1)"));
    }
}
