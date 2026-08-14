package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinCostTest {

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
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1), (2), (3), (4), (5)");
        executor.execute("INSERT INTO b VALUES (1), (2)");
        stats.analyze("a"); // 5 行
        stats.analyze("b"); // 2 行
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void equiJoinPicksHashOverNestedLoop() {
        // 5×2 等值 join:Hash 建表成本 5+2=7,比 NestedLoop 逐对比较 5×2=10 便宜,
        // 比 SortMerge(5*log6 + 2*log3 ≈ 11 的内排开销)更便宜,计划应选 Hash。
        RelNode plan = new Planner(catalog).plan("SELECT a.id FROM a JOIN b ON a.id = b.id");
        assertTrue(RelOptUtil.toString(plan).contains("MiniDbHashJoin"),
                "expected MiniDbHashJoin, plan=\n" + RelOptUtil.toString(plan));
    }
}
