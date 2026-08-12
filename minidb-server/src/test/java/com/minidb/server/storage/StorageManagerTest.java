package com.minidb.server.storage;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageManagerTest {

    private TableSchema schema() {
        return new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR)));
    }

    @Test
    void createFlushReloadKeepsData(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            ArrowTable table = storage.createTable(schema());
            VectorSchemaRoot batch = table.newBatchRoot();
            batch.allocateNew();
            ((IntVector) batch.getVector("id")).setSafe(0, 7);
            ((VarCharVector) batch.getVector("name")).setSafe(0, "hello".getBytes());
            batch.setRowCount(1);
            table.appendBatch(batch);
            storage.markDirty("t");
            storage.close();
        }

        assertTrue(Files.exists(dir.resolve("public").resolve("t.arrow")));

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
            storage2.loadAll();
            ArrowTable reloaded = storage2.getTable("t");
            assertEquals(1, reloaded.rowCount());
            IntVector ids = (IntVector) reloaded.batches().get(0).getVector("id");
            assertEquals(7, ids.get(0));
            VarCharVector names =
                    (VarCharVector) reloaded.batches().get(0).getVector("name");
            assertEquals("hello", new String(names.get(0)));
            storage2.close();
        }
    }

    @Test
    void dropTableDeletesFile(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            storage.createTable(schema());
            storage.markDirty("t");
            storage.flushDirty();
            assertTrue(Files.exists(dir.resolve("public").resolve("t.arrow")));
            storage.dropTable("t");
            assertFalse(Files.exists(dir.resolve("public").resolve("t.arrow")));
            storage.close();
        }
    }

    @Test
    void truncateSurvivesReload(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            ArrowTable table = storage.createTable(schema());
            VectorSchemaRoot batch = table.newBatchRoot();
            batch.allocateNew();
            ((IntVector) batch.getVector("id")).setSafe(0, 7);
            ((VarCharVector) batch.getVector("name")).setSafe(0, "hello".getBytes());
            batch.setRowCount(1);
            table.appendBatch(batch);
            storage.truncateTable("t");
            storage.markDirty("t");
            storage.close();
        }

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
            storage2.loadAll();
            ArrowTable reloaded = storage2.getTable("t");
            assertEquals(0, reloaded.rowCount());
            // schema intact: a fresh batch still carries both columns
            VectorSchemaRoot fresh = reloaded.newBatchRoot();
            fresh.allocateNew();
            assertEquals(2, fresh.getFieldVectors().size());
            fresh.close();
            storage2.close();
        }
    }

    @Test
    void loadAllSkipsEmptyDir(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            storage.loadAll();
            assertEquals(0, catalog.tableNames().size());
            storage.close();
        }
    }
}
