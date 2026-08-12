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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaStorageTest {

    private TableSchema schema(String schema, String name) {
        return new TableSchema(schema, name, List.of(
                new ColumnMeta("id", ColumnType.INTEGER)));
    }

    private void insertRow(StorageManager storage, String schema, String table, int id) {
        ArrowTable t = storage.getTable(schema, table);
        VectorSchemaRoot root = t.newBatchRoot();
        root.allocateNew();
        ((IntVector) root.getVector(0)).setSafe(0, id);
        root.setRowCount(1);
        t.appendBatch(root);
    }

    @Test
    void sameTableNameInDifferentSchemasPersistSeparately(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("public", "users"));
            storage.createTable(schema("other", "users"));
            insertRow(storage, "public", "users", 1);
            insertRow(storage, "other", "users", 2);
            storage.markDirty("public", "users");
            storage.markDirty("other", "users");
            storage.close();
        }

        assertTrue(Files.exists(dir.resolve("public").resolve("users.arrow")));
        assertTrue(Files.exists(dir.resolve("other").resolve("users.arrow")));

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        catalog2.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, a, dir);
            storage2.loadAll();
            assertEquals(1, storage2.getTable("public", "users").rowCount());
            assertEquals(1, storage2.getTable("other", "users").rowCount());
            IntVector pv = (IntVector) storage2.getTable("public", "users")
                    .batches().get(0).getVector(0);
            IntVector ov = (IntVector) storage2.getTable("other", "users")
                    .batches().get(0).getVector(0);
            assertEquals(1, pv.get(0));
            assertEquals(2, ov.get(0));
            storage2.close();
        }
    }

    @Test
    void dropSchemaCascadeDeletesTableFiles(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("other", "t1"));
            storage.createTable(schema("other", "t2"));
            insertRow(storage, "other", "t1", 1);
            storage.markDirty("other", "t1");
            storage.flushDirty();
            assertTrue(Files.exists(dir.resolve("other").resolve("t1.arrow")));
            storage.dropSchema("other");
            assertFalse(Files.exists(dir.resolve("other").resolve("t1.arrow")));
            assertFalse(catalog.hasTable("other", "t1"));
            storage.close();
        }
    }

    @Test
    void loadAllRestoresSchemaNameFromMetadata(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("other", "t"));
            storage.markDirty("other", "t");
            storage.close();
        }

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        catalog2.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, a, dir);
            storage2.loadAll();
            assertEquals("other",
                    storage2.getTable("other", "t").schema().schemaName());
            storage2.close();
        }
    }

    @Test
    void dropPublicSchemaThrows(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            assertThrows(IllegalArgumentException.class,
                    () -> storage.dropSchema("public"));
            storage.close();
        }
    }
}
