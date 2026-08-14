package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StatsManagerTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void analyzeDelegatesToCatalog() {
        storage.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        QueryExecutor q = new QueryExecutor(catalog, storage, allocator, stats);
        q.execute("INSERT INTO t VALUES (1), (2), (2)");
        stats.analyze("t");
        TableStats ts = stats.tableStats("t");
        assertNotNull(ts);
        assertEquals(3, ts.rowCount());
    }
}
