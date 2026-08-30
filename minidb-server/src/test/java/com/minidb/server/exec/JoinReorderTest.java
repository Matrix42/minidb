package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinReorderTest {

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
        // 大表 big(1000 行) + 两张小表 s1/s2(各 1 行),制造明显的重排空间。
        executor.execute("CREATE TABLE big (id INTEGER)");
        executor.execute("CREATE TABLE s1 (id INTEGER)");
        executor.execute("CREATE TABLE s2 (id INTEGER)");
        StringBuilder bigIns = new StringBuilder("INSERT INTO big VALUES ");
        for (int i = 1; i <= 1000; i++) {
            bigIns.append(i == 1 ? "" : ",").append("(").append(i).append(")");
        }
        executor.execute(bigIns.toString());
        executor.execute("INSERT INTO s1 VALUES (1)");
        executor.execute("INSERT INTO s2 VALUES (1)");
        stats.analyze("big");
        stats.analyze("s1");
        stats.analyze("s2");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void reorderAvoidsCrossJoin() {
        // 正确性:结果不受重排影响(重排只改执行顺序,不改结果集)。
        QueryResult.Rows rows =
                (QueryResult.Rows)
                        executor.execute(
                                "SELECT big.id FROM big JOIN s1 ON big.id = s1.id JOIN s2 ON s1.id = s2.id");
        assertEquals(1, rows.data().getRowCount());
        rows.data().close();

        // 结构断言:重排后每个 join 都有等值条件(无交叉连接)。原 JoinCommuteRule 按行数
        // 重排成右深 big ⋈ (s1 ⋈ s2);现改由 JoinReorderer 按连接度重排(hub s1 优先),
        // 二者结构不同但都消除交叉连接,故只锁「无 cond=true」这一不变式。
        RelNode root =
                new Planner(catalog)
                        .plan(
                                "SELECT big.id FROM big JOIN s1 ON big.id = s1.id JOIN s2 ON s1.id = s2.id");
        collectJoins(root)
                .forEach(
                        join ->
                                assertTrue(
                                        !join.getCondition().isAlwaysTrue(),
                                        "expected no cross join, plan=\n"
                                                + RelOptUtil.toString(root)));
    }

    private static List<MiniDbJoin> collectJoins(RelNode node) {
        List<MiniDbJoin> joins = new java.util.ArrayList<>();
        collectJoinsRec(node, joins);
        return joins;
    }

    private static void collectJoinsRec(RelNode node, List<MiniDbJoin> out) {
        if (node instanceof MiniDbJoin join) {
            out.add(join);
        }
        for (RelNode in : node.getInputs()) {
            collectJoinsRec(in, out);
        }
    }
}
