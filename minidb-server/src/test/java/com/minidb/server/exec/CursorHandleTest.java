package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorHandleTest {

    @Test
    void executeReturnsRowsButCursorReturnsUnmaterialized(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        try {
            executor.execute("CREATE TABLE t (id INTEGER)");
            executor.execute("INSERT INTO t VALUES (1), (2), (3)");

            // execute() still materializes to Rows (compat for tests)
            QueryResult materialized = executor.execute("SELECT id FROM t ORDER BY id");
            assertTrue(materialized instanceof QueryResult.Rows);
            VectorSchemaRoot root = ((QueryResult.Rows) materialized).data();
            assertEquals(3, root.getRowCount());
            root.close();

            // executeCursor() returns an unmaterialized Cursor handle
            QueryResult cursor = executor.executeCursor("SELECT id FROM t ORDER BY id");
            assertTrue(cursor instanceof QueryResult.Cursor);
            VectorSchemaRoot viaHandle = ((QueryResult.Cursor) cursor).handle().materialize();
            assertEquals(3, viaHandle.getRowCount());
            viaHandle.close();
        } finally {
            storage.close();
            allocator.close();
        }
    }
}
