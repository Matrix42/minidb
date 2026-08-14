package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InformationSchemaQueryTest {

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
        stats = new StatsManager(storage, allocator, dataDir);
        storage.setStatsManager(stats);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void selectsInformationSchemaTables() {
        executor.execute("CREATE TABLE public.t (id INT, price DECIMAL(10,2))");
        QueryResult select = executor.execute("SELECT * FROM information_schema.tables");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(1, root.getRowCount());
        VarCharVector tableName = (VarCharVector) root.getVector("TABLE_NAME");
        assertEquals("t", new String(tableName.get(0)));
        root.close();
    }

    @Test
    void rejectsCreatingReservedSchema() {
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("CREATE SCHEMA information_schema"));
    }

    @Test
    void selectsInformationSchemaColumns() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        QueryResult select = executor.execute(
                "SELECT * FROM information_schema.columns WHERE table_name = 't'");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(1, root.getRowCount());
        assertEquals("price",
                new String(((VarCharVector) root.getVector("COLUMN_NAME")).get(0)));
        assertEquals("DECIMAL",
                new String(((VarCharVector) root.getVector("DATA_TYPE")).get(0)));
        assertEquals(10, ((IntVector) root.getVector("NUMERIC_PRECISION")).get(0));
        assertEquals(2, ((IntVector) root.getVector("NUMERIC_SCALE")).get(0));
        root.close();
    }

    @Test
    void selectsInformationSchemaSchemata() {
        executor.execute("CREATE SCHEMA other");
        QueryResult select = executor.execute("SELECT * FROM information_schema.schemata");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        VarCharVector schemaName = (VarCharVector) root.getVector("SCHEMA_NAME");
        // schema 名按字典序:other < public
        assertEquals("other", new String(schemaName.get(0)));
        assertEquals("public", new String(schemaName.get(1)));
        root.close();
    }
}
