package com.minidb.server.storage;

import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.common.TableSchema;
import com.minidb.server.exec.BatchIterator;
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
        t.writePart(root);
        root.close();
    }

    private int readFirstId(ArrowTable table) {
        try (BatchIterator it = table.scan()) {
            while (it.hasNext()) {
                IntVector v = (IntVector) it.next().getVector(0);
                if (v.getValueCount() > 0) {
                    return v.get(0);
                }
            }
        }
        return -1;
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
            storage.close();
        }

        assertTrue(Files.exists(dir.resolve("public").resolve("users")));
        assertTrue(Files.exists(dir.resolve("other").resolve("users")));

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        catalog2.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, a, dir);
            storage2.loadAll();
            assertEquals(1, storage2.getTable("public", "users").rowCount());
            assertEquals(1, storage2.getTable("other", "users").rowCount());
            assertEquals(1, readFirstId(storage2.getTable("public", "users")));
            assertEquals(2, readFirstId(storage2.getTable("other", "users")));
            storage2.close();
        }
    }

    @Test
    void dropSchemaCascadeDeletesTableDirs(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("other", "t1"));
            storage.createTable(schema("other", "t2"));
            insertRow(storage, "other", "t1", 1);
            assertTrue(Files.exists(dir.resolve("other").resolve("t1")));
            storage.dropSchema("other");
            assertFalse(Files.exists(dir.resolve("other")));
            assertFalse(catalog.hasTable("other", "t1"));
            storage.close();
        }
    }

    @Test
    void loadAllRestoresSchemaNameFromCatalog(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("other", "t"));
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
