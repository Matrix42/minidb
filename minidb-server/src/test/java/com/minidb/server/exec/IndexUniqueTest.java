package com.minidb.server.exec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

class IndexUniqueTest {

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
    void duplicateInsertRejected() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_a ON t (a)");
        executor.execute("INSERT INTO t VALUES (1, 10)");
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("INSERT INTO t VALUES (2, 10)"),
                "UNIQUE 索引冲突应抛异常");
    }

    @Test
    void multipleNullsAllowed() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_a ON t (a)");
        assertDoesNotThrow(() -> {
            executor.execute("INSERT INTO t VALUES (1, NULL)");
            executor.execute("INSERT INTO t VALUES (2, NULL)");
        }, "UNIQUE 索引允许多行 NULL");
    }

    @Test
    void batchDuplicateRejected() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_a ON t (a)");
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("INSERT INTO t VALUES (1, 10), (2, 10)"),
                "单批内同键应报错");
    }

    @Test
    void compositeUnique() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_ab ON t (a, b)");
        executor.execute("INSERT INTO t VALUES (1, 10, 20)");
        // same a, different b → allowed
        assertDoesNotThrow(() -> executor.execute("INSERT INTO t VALUES (2, 10, 30)"));
        // same (a,b) → rejected
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("INSERT INTO t VALUES (3, 10, 20)"));
    }

    @Test
    void updateToConflictingValueRejected() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_a ON t (a)");
        executor.execute("INSERT INTO t VALUES (1, 10), (2, 20)");
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("UPDATE t SET a = 10 WHERE id = 2"),
                "UPDATE 成冲突值应报错");
    }

    @Test
    void updateToNonConflictingValueAllowed() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("CREATE UNIQUE INDEX idx_a ON t (a)");
        executor.execute("INSERT INTO t VALUES (1, 10), (2, 20)");
        assertDoesNotThrow(() -> executor.execute("UPDATE t SET a = 30 WHERE id = 2"));
    }

    @Test
    void createIndexOnExistingDuplicatesFails() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10), (2, 10)");
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("CREATE UNIQUE INDEX idx_a ON t (a)"),
                "存量重复数据上建 UNIQUE 索引应失败");
        // 非唯一索引应成功
        assertDoesNotThrow(() -> executor.execute("CREATE INDEX idx_a_nonuniq ON t (a)"));
    }
}