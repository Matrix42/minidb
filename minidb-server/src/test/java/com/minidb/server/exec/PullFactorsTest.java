package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FilterPullFactorsRule / JoinPullFactorsRule:把 OR 里公共等值键因子化到顶层 AND, 让 JoinInfo 能抽成 HashJoin
 * 键,而不是整体退化成交叉连接(query13 的回归)。
 */
class PullFactorsTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        StatsManager stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
        executor.execute("CREATE TABLE a (x INTEGER, y INTEGER)");
        executor.execute("CREATE TABLE b (x INTEGER, y INTEGER)");
        executor.execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
        executor.execute("INSERT INTO b VALUES (1, 100), (2, 200)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void factorOrPreservesSemantics() {
        // (a.x=b.x AND a.y=10) OR (a.x=b.x AND a.y=20) 等价于 a.x=b.x AND a.y IN (10,20)。
        // a: (1,10),(2,20),(3,30); b.x: 1,2 -> 结果 (1,100),(2,200)。
        VectorSchemaRoot root =
                rows(
                        "SELECT a.x, b.y FROM a JOIN b ON"
                                + " (a.x = b.x AND a.y = 10) OR (a.x = b.x AND a.y = 20)"
                                + " ORDER BY a.x");
        try {
            assertEquals(2, root.getRowCount());
            assertEquals(1, root.getVector("x").getObject(0));
            assertEquals(100, root.getVector("y").getObject(0));
            assertEquals(2, root.getVector("x").getObject(1));
            assertEquals(200, root.getVector("y").getObject(1));
        } finally {
            root.close();
        }
    }

    @Test
    void factorOrWhereClauseProducesHashJoinNotNestedLoop() {
        // WHERE 子句的 OR 先经 FilterPullFactorsRule 因子化,再由 FilterIntoJoinRule
        // 把等值键下推成 HashJoin 键、单表残留下推成表过滤(query13 的模式)。
        RelNode plan =
                new Planner(catalog)
                        .plan(
                                "SELECT a.x FROM a, b WHERE (a.x = b.x AND a.y = 10) OR (a.x = b.x AND a.y = 20)");
        List<MiniDbJoin> joins = new ArrayList<>();
        collectJoins(plan, joins);
        assertTrue(
                joins.stream().anyMatch(j -> j instanceof MiniDbHashJoin),
                "expected a HashJoin (equijoin factored out), got " + joins);
        assertTrue(
                joins.stream().noneMatch(j -> j instanceof MiniDbNestedLoopJoin),
                "expected no NestedLoopJoin (cartesian), got " + joins);
    }

    private VectorSchemaRoot rows(String sql) {
        return ((QueryResult.Rows) executor.execute(sql)).data();
    }

    private static void collectJoins(RelNode node, List<MiniDbJoin> out) {
        if (node instanceof MiniDbJoin join) {
            out.add(join);
        }
        for (RelNode in : node.getInputs()) {
            collectJoins(in, out);
        }
    }
}
