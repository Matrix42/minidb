package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatsManagerTest {

    @TempDir
    Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage, allocator, dataDir);
        storage.setStatsManager(stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    private void insertIntRows(String table, int[] ids) {
        var arrowTable = storage.getTable(table);
        VectorSchemaRoot root = arrowTable.newBatchRoot();
        root.allocateNew();
        IntVector v = (IntVector) root.getVector(0);
        for (int i = 0; i < ids.length; i++) {
            v.setSafe(i, ids[i]);
        }
        v.setValueCount(ids.length);
        root.setRowCount(ids.length);
        arrowTable.appendBatch(root);
    }

    private void insertIntVarcharRows(String table, int[] ids, String[] names) {
        var arrowTable = storage.getTable(table);
        VectorSchemaRoot root = arrowTable.newBatchRoot();
        root.allocateNew();
        IntVector iv = (IntVector) root.getVector(0);
        VarCharVector nv = (VarCharVector) root.getVector(1);
        for (int i = 0; i < ids.length; i++) {
            iv.setSafe(i, ids[i]);
            nv.setSafe(i, names[i].getBytes());
        }
        iv.setValueCount(ids.length);
        nv.setValueCount(ids.length);
        root.setRowCount(ids.length);
        arrowTable.appendBatch(root);
    }

    @Test
    void analyzeCollectsHistogramsForAllColumns() {
        storage.createTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR))));
        insertIntVarcharRows("t", new int[]{1, 2, 3}, new String[]{"a", "b", "c"});

        stats.analyze("t");
        TableStats ts = stats.tableStats("t");
        assertNotNull(ts);
        assertNotNull(ts.columnHistograms().get("id"));
        assertNotNull(ts.columnHistograms().get("name"));
        assertTrue(!ts.stale());
    }

    @Test
    void markStaleSetsFlagWithoutRemovingHistograms() {
        storage.createTable(new TableSchema("t",
                List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        insertIntRows("t", new int[]{1, 2});
        stats.analyze("t");
        stats.markStale("t");
        TableStats ts = stats.tableStats("t");
        assertNotNull(ts);
        assertTrue(ts.stale());
        assertNotNull(ts.columnHistograms().get("id"));
    }

    @Test
    void statsPersistAcrossRestart() {
        storage.createTable(new TableSchema("t",
                List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        insertIntRows("t", new int[]{1, 2, 3});
        stats.analyze("t");
        assertNotNull(stats.tableStats("t"));

        // simulate restart: close storage, rebuild, reload
        storage.close();
        storage = new StorageManager(catalog, allocator, dataDir);
        storage.loadAll();
        StatsManager reloaded = new StatsManager(storage, allocator, dataDir);
        reloaded.loadAll();
        TableStats ts = reloaded.tableStats("t");
        assertNotNull(ts);
        assertNotNull(ts.columnHistograms().get("id"));
    }

    @Test
    void analyzeMissingTableThrows() {
        try {
            stats.analyze("nope");
            org.junit.jupiter.api.Assertions.fail("expected exception");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    void tableStatsAbsentReturnsNull() {
        assertNull(stats.tableStats("never"));
    }
}
