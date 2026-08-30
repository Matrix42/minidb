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

/** UPDATE 变更外键列时,新值必须引用存在的父行(带主键→LSM 与 type=simple→SimpleTable 两条路径)。 */
class ForeignKeyUpdateTest {

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
    void updateToMissingParentRejectedOnLsm() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "FOREIGN KEY (parent_id) REFERENCES parent(id))");
        executor.execute("INSERT INTO parent VALUES (1)");
        executor.execute("INSERT INTO child VALUES (1, 1)");

        // 更新到不存在的父行必须被拒绝
        assertThrows(
                Exception.class,
                () -> executor.execute("UPDATE child SET parent_id = 99 WHERE id = 1"));
        // 更新到存在的父行应成功
        executor.execute("UPDATE child SET parent_id = 1 WHERE id = 1");
    }

    @Test
    void updateToMissingParentRejectedOnSimpleTable() {
        executor.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
        executor.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "FOREIGN KEY (parent_id) REFERENCES parent(id)) WITH ('type' = 'simple')");
        executor.execute("INSERT INTO parent VALUES (1)");
        executor.execute("INSERT INTO child VALUES (1, 1)");

        assertThrows(
                Exception.class,
                () -> executor.execute("UPDATE child SET parent_id = 99 WHERE id = 1"));
        executor.execute("UPDATE child SET parent_id = 1 WHERE id = 1");
    }
}
