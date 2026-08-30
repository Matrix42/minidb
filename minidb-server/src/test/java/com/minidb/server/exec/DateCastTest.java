package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DateCastTest {

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
    void insertDateLiteral() {
        executor.execute("CREATE TABLE orders (order_id INTEGER, order_date DATE)");
        executor.execute("INSERT INTO orders VALUES (101, '2025-01-05')");
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT order_date FROM orders WHERE order_id = 101"))
                        .data();
        DateDayVector v = (DateDayVector) root.getVector(0);
        assertEquals(new DateString("2025-01-05").getDaysSinceEpoch(), v.get(0));
        root.close();
    }

    @Test
    void insertMultiRowDateLiteral() {
        // 多行 VALUES 走 LogicalUnion 路径(坑 23),每行一个恒等 CAST(date→date)。
        executor.execute("CREATE TABLE orders (order_id INTEGER, order_date DATE)");
        executor.execute(
                "INSERT INTO orders VALUES "
                        + "(101, '2025-01-05'), (102, '2025-02-10'), (103, '2024-12-20')");
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute("SELECT order_date FROM orders ORDER BY order_id"))
                        .data();
        DateDayVector v = (DateDayVector) root.getVector(0);
        assertEquals(3, v.getValueCount());
        assertEquals(new DateString("2025-01-05").getDaysSinceEpoch(), v.get(0));
        assertEquals(new DateString("2025-02-10").getDaysSinceEpoch(), v.get(1));
        assertEquals(new DateString("2024-12-20").getDaysSinceEpoch(), v.get(2));
        root.close();
    }

    @Test
    void insertTimestampLiteral() {
        executor.execute("CREATE TABLE events (id INTEGER, ts TIMESTAMP)");
        executor.execute("INSERT INTO events VALUES (1, '2025-01-05 12:30:00')");
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("SELECT ts FROM events WHERE id = 1")).data();
        TimeStampMilliVector v = (TimeStampMilliVector) root.getVector(0);
        assertEquals(new TimestampString("2025-01-05 12:30:00").getMillisSinceEpoch(), v.get(0));
        root.close();
    }

    @Test
    void castStringToDate() {
        executor.execute("CREATE TABLE t (s VARCHAR)");
        executor.execute("INSERT INTO t VALUES ('2025-01-05')");
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("SELECT CAST(s AS DATE) FROM t")).data();
        DateDayVector v = (DateDayVector) root.getVector(0);
        assertEquals(new DateString("2025-01-05").getDaysSinceEpoch(), v.get(0));
        root.close();
    }

    @Test
    void castStringToTimestamp() {
        executor.execute("CREATE TABLE t (s VARCHAR)");
        executor.execute("INSERT INTO t VALUES ('2025-01-05 12:30:00')");
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("SELECT CAST(s AS TIMESTAMP) FROM t")).data();
        TimeStampMilliVector v = (TimeStampMilliVector) root.getVector(0);
        assertEquals(new TimestampString("2025-01-05 12:30:00").getMillisSinceEpoch(), v.get(0));
        root.close();
    }
}
