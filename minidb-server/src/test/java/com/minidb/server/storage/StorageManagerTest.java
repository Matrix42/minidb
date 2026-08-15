package com.minidb.server.storage;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.exec.BatchIterator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

    private void writeRow(ArrowTable table, int id, String name) {
        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        ((IntVector) batch.getVector("id")).setSafe(0, id);
        ((VarCharVector) batch.getVector("name")).setSafe(0, name.getBytes());
        batch.setRowCount(1);
        table.writePart(batch);
        batch.close();
    }

    private List<Integer> readIds(ArrowTable table) {
        List<Integer> ids = new ArrayList<>();
        try (BatchIterator it = table.scan()) {
            while (it.hasNext()) {
                IntVector v = (IntVector) it.next().getVector("id");
                for (int i = 0; i < v.getValueCount(); i++) {
                    ids.add(v.get(i));
                }
            }
        }
        return ids;
    }

    @Test
    void writeReloadKeepsData(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            ArrowTable table = storage.createTable(schema());
            writeRow(table, 7, "hello");
            storage.close();
        }

        assertTrue(Files.exists(dir.resolve("public").resolve("t")));

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
            storage2.loadAll();
            ArrowTable reloaded = storage2.getTable("public", "t");
            assertEquals(1, reloaded.rowCount());
            assertEquals(List.of(7), readIds(reloaded));
            storage2.close();
        }
    }

    @Test
    void dropTableDeletesDir(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            ArrowTable table = storage.createTable(schema());
            writeRow(table, 1, "a");
            assertTrue(Files.exists(dir.resolve("public").resolve("t")));
            storage.dropTable("public", "t");
            assertFalse(Files.exists(dir.resolve("public").resolve("t")));
            storage.close();
        }
    }

    @Test
    void truncateSurvivesReload(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            ArrowTable table = storage.createTable(schema());
            writeRow(table, 7, "hello");
            storage.truncateTable("public", "t");
            storage.close();
        }

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
            storage2.loadAll();
            ArrowTable reloaded = storage2.getTable("public", "t");
            assertEquals(0, reloaded.rowCount());
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
            assertEquals(0, catalog.tableNames("public").size());
            storage.close();
        }
    }

    @Test
    void emptyTableSurvivesRestart(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            storage.createTable(new TableSchema("t", List.of(
                    new ColumnMeta("id", ColumnType.INTEGER),
                    new ColumnMeta("price", ColumnType.DECIMAL, 10, 2))));
            // 不插任何行 → 无 part 文件,但 catalog.json 应已落盘
            storage.close();
        }
        assertTrue(Files.exists(dir.resolve("catalog.json")));

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
            storage2.loadAll();
            assertTrue(catalog2.hasTable("public", "t"));
            List<ColumnMeta> cols = catalog2.getTable("public", "t").columns();
            assertEquals(ColumnType.INTEGER, cols.get(0).type());
            assertEquals(ColumnType.DECIMAL, cols.get(1).type());
            assertEquals(10, cols.get(1).precision());
            assertEquals(2, cols.get(1).scale());
            storage2.close();
        }
    }

    @Test
    void reloadPreservesNewColumnTypesAndDecimalScale(@TempDir Path dir) {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnMeta("s", ColumnType.SMALLINT),
                new ColumnMeta("r", ColumnType.REAL),
                new ColumnMeta("p", ColumnType.DECIMAL, 10, 2),
                new ColumnMeta("c", ColumnType.CHAR),
                new ColumnMeta("b", ColumnType.VARBINARY)));
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            storage.createTable(schema);
            storage.close();
        }
        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
            storage2.loadAll();
            List<ColumnMeta> cols = catalog2.getTable("public", "t").columns();
            assertEquals(ColumnType.SMALLINT, cols.get(0).type());
            assertEquals(ColumnType.REAL, cols.get(1).type());
            assertEquals(ColumnType.DECIMAL, cols.get(2).type());
            assertEquals(10, cols.get(2).precision());
            assertEquals(2, cols.get(2).scale());
            assertEquals(ColumnType.CHAR, cols.get(3).type());
            assertEquals(ColumnType.VARBINARY, cols.get(4).type());
            storage2.close();
        }
    }
}
