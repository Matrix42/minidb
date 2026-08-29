package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ForeignKeyTest {

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
    void insertReferencingMissingParent() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "FOREIGN KEY (parent_id) REFERENCES parent(id))");
        executor.execute("INSERT INTO parent VALUES (1)");
        executor.execute("INSERT INTO child VALUES (1, 1)");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO child VALUES (2, 2)"));
    }

    @Test
    void columnLevelForeignKey() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, "
                        + "parent_id INTEGER REFERENCES parent(id))");
        executor.execute("INSERT INTO parent VALUES (1)");
        executor.execute("INSERT INTO child VALUES (1, 1)");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO child VALUES (2, 99)"));
    }

    @Test
    void deleteRestrictedWhenReferenced() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "FOREIGN KEY (parent_id) REFERENCES parent(id))");
        executor.execute("INSERT INTO parent VALUES (1), (2)");
        executor.execute("INSERT INTO child VALUES (1, 1)");
        assertThrows(Exception.class, () -> executor.execute("DELETE FROM parent WHERE id = 1"));
        executor.execute("DELETE FROM parent WHERE id = 2");
    }

    @Test
    void nullForeignKeyAllowed() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "FOREIGN KEY (parent_id) REFERENCES parent(id))");
        executor.execute("INSERT INTO child VALUES (1, NULL)");
    }

    @Test
    void schemaQualifiedReference() {
        executor.execute("CREATE SCHEMA other");
        executor.execute("CREATE TABLE other.parent (id INTEGER PRIMARY KEY)");
        executor.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "FOREIGN KEY (parent_id) REFERENCES other.parent(id))");
        executor.execute("INSERT INTO other.parent VALUES (1)");
        executor.execute("INSERT INTO child VALUES (1, 1)");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO child VALUES (2, 99)"));
    }
}
