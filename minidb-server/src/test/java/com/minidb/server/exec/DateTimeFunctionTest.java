package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DateTimeFunctionTest {

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
        executor.execute("CREATE TABLE t (id INTEGER, d DATE, ts TIMESTAMP)");
        executor.execute("INSERT INTO t VALUES (1, '2025-01-05', '2025-01-05 12:34:56')");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    private long extract(String expr) {
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute("SELECT " + expr + " AS v FROM t WHERE id = 1"))
                        .data();
        long v = ((BigIntVector) root.getVector("v")).get(0);
        root.close();
        return v;
    }

    @Test
    void extractDateParts() {
        assertEquals(2025, extract("EXTRACT(YEAR FROM d)"));
        assertEquals(1, extract("EXTRACT(MONTH FROM d)"));
        assertEquals(5, extract("EXTRACT(DAY FROM d)"));
        assertEquals(1, extract("EXTRACT(QUARTER FROM d)"));
    }

    @Test
    void extractTimestampParts() {
        assertEquals(12, extract("EXTRACT(HOUR FROM ts)"));
        assertEquals(34, extract("EXTRACT(MINUTE FROM ts)"));
        assertEquals(56, extract("EXTRACT(SECOND FROM ts)"));
    }

    @Test
    void currentDate() {
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("SELECT CURRENT_DATE FROM t WHERE id = 1"))
                        .data();
        DateDayVector d = (DateDayVector) root.getVector(0);
        assertFalse(d.isNull(0));
        root.close();
    }

    @Test
    void currentTimestamp() {
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute("SELECT CURRENT_TIMESTAMP FROM t WHERE id = 1"))
                        .data();
        TimeStampMilliVector ts = (TimeStampMilliVector) root.getVector(0);
        assertFalse(ts.isNull(0));
        root.close();
    }
}
