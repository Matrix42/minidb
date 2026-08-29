package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InformationSchemaQueryTest {

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
    void selectsInformationSchemaTables() {
        executor.execute("CREATE TABLE public.t (id INT, price DECIMAL(10,2))");
        QueryResult select = executor.execute("SELECT * FROM information_schema.tables");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        // information_schema 自身 4 张系统表 + public.t
        assertEquals(5, root.getRowCount());
        VarCharVector tableName = (VarCharVector) root.getVector("TABLE_NAME");
        // public 是最后的 schema,t 是最后一行
        assertEquals("t", new String(tableName.get(4)));
        root.close();
    }

    @Test
    void rejectsCreatingReservedSchema() {
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("CREATE SCHEMA information_schema"));
    }

    @Test
    void rejectsDroppingReservedSchema() {
        assertThrows(
                IllegalArgumentException.class,
                () -> executor.execute("DROP SCHEMA information_schema"));
    }

    @Test
    void selectsInformationSchemaColumns() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        QueryResult select =
                executor.execute("SELECT * FROM information_schema.columns WHERE table_name = 't'");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(1, root.getRowCount());
        assertEquals("price", new String(((VarCharVector) root.getVector("COLUMN_NAME")).get(0)));
        assertEquals("DECIMAL", new String(((VarCharVector) root.getVector("DATA_TYPE")).get(0)));
        assertEquals(10, ((IntVector) root.getVector("NUMERIC_PRECISION")).get(0));
        assertEquals(2, ((IntVector) root.getVector("NUMERIC_SCALE")).get(0));
        root.close();
    }

    @Test
    void selectsInformationSchemaSchemata() {
        executor.execute("CREATE SCHEMA other");
        QueryResult select = executor.execute("SELECT * FROM information_schema.schemata");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        // information_schema + other + public
        assertEquals(3, root.getRowCount());
        VarCharVector schemaName = (VarCharVector) root.getVector("SCHEMA_NAME");
        // schema 名按字典序:information_schema < other < public
        assertEquals("information_schema", new String(schemaName.get(0)));
        assertEquals("other", new String(schemaName.get(1)));
        assertEquals("public", new String(schemaName.get(2)));
        root.close();
    }
}
