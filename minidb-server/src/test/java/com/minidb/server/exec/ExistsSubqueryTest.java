package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExistsSubqueryTest {

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
        executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
        executor.execute("CREATE TABLE b (id INTEGER, aid INTEGER)");
        executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z'), (4, 'w')");
        // b.aid references a.id; 2 appears twice (dedup check), 4 has no match, null never matches.
        executor.execute("INSERT INTO b VALUES (1, 1), (2, 2), (3, 2), (4, 3), (5, NULL)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    private int[] ids(String sql) {
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        IntVector id = (IntVector) root.getVector("id");
        int[] result = new int[id.getValueCount()];
        for (int i = 0; i < result.length; i++) {
            result[i] = id.get(i);
        }
        root.close();
        return result;
    }

    @Test
    void correlatedExists() {
        // b has aid 1,2,2,3 -> a.id 1,2,3 match; each outer row once despite duplicate b.aid=2.
        int[] result = ids("SELECT id FROM a WHERE EXISTS "
                + "(SELECT 1 FROM b WHERE b.aid = a.id) ORDER BY id");
        assertEquals(3, result.length);
        assertEquals(1, result[0]);
        assertEquals(2, result[1]);
        assertEquals(3, result[2]);
    }

    @Test
    void correlatedNotExists() {
        // a.id 4 has no matching b.aid; NULL b.aid never matches any a.id.
        int[] result = ids("SELECT id FROM a WHERE NOT EXISTS "
                + "(SELECT 1 FROM b WHERE b.aid = a.id) ORDER BY id");
        assertEquals(1, result.length);
        assertEquals(4, result[0]);
    }

    @Test
    void uncorrelatedExists() {
        // subquery is non-empty -> all a rows; empty -> none.
        int[] all = ids("SELECT id FROM a WHERE EXISTS "
                + "(SELECT 1 FROM b WHERE b.id > 0) ORDER BY id");
        assertEquals(4, all.length);

        int[] none = ids("SELECT id FROM a WHERE EXISTS "
                + "(SELECT 1 FROM b WHERE b.id > 100) ORDER BY id");
        assertEquals(0, none.length);
    }

    @Test
    void uncorrelatedNotExists() {
        int[] none = ids("SELECT id FROM a WHERE NOT EXISTS "
                + "(SELECT 1 FROM b WHERE b.id > 0) ORDER BY id");
        assertEquals(0, none.length);

        int[] all = ids("SELECT id FROM a WHERE NOT EXISTS "
                + "(SELECT 1 FROM b WHERE b.id > 100) ORDER BY id");
        assertEquals(4, all.length);
    }

    @Test
    void existsInSelectListWithNot() {
        // NOT wrapping EXISTS is parsed as NOT EXISTS; exercises the NOT(EXISTS(...)) shape.
        int[] result = ids("SELECT id FROM a WHERE NOT (EXISTS "
                + "(SELECT 1 FROM b WHERE b.aid = a.id)) ORDER BY id");
        assertEquals(1, result.length);
        assertEquals(4, result[0]);
    }
}
