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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * bug #16:QueryExecutor.handleCreate 传 null 作 indexes。TableSchema compact constructor
 * 已把 null 规范化为 List.of();此测试锁定「CREATE TABLE 后 indexes() 返回空表而非 NPE」。
 */
class CreateTableNullIndexesTest {

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
    void createTableYieldsEmptyIndexesNotNull() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        var ts = catalog.getTable("public", "t");
        // 非 null 且空:下游 indexes().isEmpty() 等调用不会 NPE。
        assertTrue(ts.indexes() != null && ts.indexes().isEmpty(),
                "新建表 indexes 应为空列表(而非 null)");
        // 空索引表上做 DML 也不受影响
        executor.execute("INSERT INTO t VALUES (1)");
    }
}