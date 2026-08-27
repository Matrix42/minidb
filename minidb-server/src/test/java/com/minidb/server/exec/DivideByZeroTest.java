package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 算术除零必须报错(SQL 标准),而非静默返回 0。覆盖整型/浮点除法内核。
 */
class DivideByZeroTest {

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
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void integerDivisionByZero() {
        executor.execute("CREATE TABLE t (v INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        assertThrows(Exception.class, () -> executor.execute("SELECT v / 0 FROM t"));
    }

    @Test
    void bigintDivisionByZero() {
        executor.execute("CREATE TABLE t (v BIGINT)");
        executor.execute("INSERT INTO t VALUES (1)");
        assertThrows(Exception.class, () -> executor.execute("SELECT v / 0 FROM t"));
    }

    @Test
    void doubleDivisionByZero() {
        executor.execute("CREATE TABLE t (v DOUBLE)");
        executor.execute("INSERT INTO t VALUES (1.0)");
        assertThrows(Exception.class, () -> executor.execute("SELECT v / 0 FROM t"));
    }

    @Test
    void decimalDivisionByZero() {
        executor.execute("CREATE TABLE t (v DECIMAL(10,2))");
        executor.execute("INSERT INTO t VALUES (1.00)");
        assertThrows(Exception.class, () -> executor.execute("SELECT v / 0 FROM t"));
    }
}