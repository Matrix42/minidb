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

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SimpleTable(WITH type=simple)路径的 UPDATE 也必须校验主键/UNIQUE 唯一性(bug #8), 与 LSM 路径的
 * validateUpdateUnique 对齐。
 */
class SimpleTableUniqueUpdateTest {

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
    void updateToDuplicatePrimaryKeyRejected() {
        executor.execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER) WITH ('type' = 'simple')");
        executor.execute("INSERT INTO t VALUES (1, 10), (2, 20)");
        // id 改成已存在的 2 → 主键冲突必须被拒绝
        assertThrows(Exception.class, () -> executor.execute("UPDATE t SET id = 2 WHERE id = 1"));
        // 改成不冲突的值应成功
        executor.execute("UPDATE t SET id = 3 WHERE id = 1");
    }

    @Test
    void updateToDuplicateUniqueKeyRejected() {
        executor.execute(
                "CREATE TABLE t (id INTEGER, code INTEGER UNIQUE) WITH ('type' = 'simple')");
        executor.execute("INSERT INTO t VALUES (1, 100), (2, 200)");
        // code 改成已存在的 200 → UNIQUE 冲突必须被拒绝
        assertThrows(
                Exception.class, () -> executor.execute("UPDATE t SET code = 200 WHERE id = 1"));
        executor.execute("UPDATE t SET code = 300 WHERE id = 1");
    }
}
