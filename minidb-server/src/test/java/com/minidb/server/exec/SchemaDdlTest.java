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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDdlTest {

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
    void createSchemaMakesItVisible() {
        executor.execute("CREATE SCHEMA other", "public");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createSchemaIfNotExistsIsIdempotent() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE SCHEMA IF NOT EXISTS other", "public");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createDuplicateSchemaThrows() {
        executor.execute("CREATE SCHEMA other", "public");
        assertThrows(Exception.class, () -> executor.execute("CREATE SCHEMA other", "public"));
    }

    @Test
    void dropSchemaCascades() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("DROP SCHEMA other", "public");
        assertFalse(catalog.schemaNames().contains("other"));
    }

    @Test
    void dropSchemaIfExistsMissingIsNoop() {
        executor.execute("DROP SCHEMA IF EXISTS ghost", "public");
    }

    @Test
    void useSchemaReturnsUseSchemaResult() {
        executor.execute("CREATE SCHEMA other", "public");
        QueryResult r = executor.execute("USE SCHEMA other", "public");
        assertTrue(r instanceof QueryResult.UseSchema);
        assertEquals("other", ((QueryResult.UseSchema) r).schemaName());
    }

    @Test
    void useSchemaMissingThrows() {
        assertThrows(Exception.class, () -> executor.execute("USE SCHEMA ghost", "public"));
    }

    @Test
    void createTableUnqualifiedUsesCurrentSchema() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE t (id INTEGER)", "other");
        assertTrue(catalog.hasTable("other", "t"));
        assertEquals("other", catalog.getTable("other", "t").schemaName());
    }

    @Test
    void createTableQualifiedNamesSchema() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        assertTrue(catalog.hasTable("other", "t"));
    }

    @Test
    void dropTableQualified() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("DROP TABLE other.t", "public");
        assertFalse(catalog.hasTable("other", "t"));
    }

    @Test
    void selectQualifiedRoundtrips() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("INSERT INTO other.t VALUES (5)", "public");
        QueryResult r = executor.execute("SELECT id FROM other.t", "public");
        assertTrue(r instanceof QueryResult.Rows);
        assertEquals(1, ((QueryResult.Rows) r).data().getRowCount());
        ((QueryResult.Rows) r).data().close();
    }

    @Test
    void useSchemaThenUnqualifiedSelect() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("INSERT INTO other.t VALUES (7)", "other");
        QueryResult r = executor.execute("SELECT id FROM t", "other");
        assertTrue(r instanceof QueryResult.Rows);
        assertEquals(
                7,
                ((org.apache.arrow.vector.IntVector) ((QueryResult.Rows) r).data().getVector(0))
                        .get(0));
        ((QueryResult.Rows) r).data().close();
    }
}
