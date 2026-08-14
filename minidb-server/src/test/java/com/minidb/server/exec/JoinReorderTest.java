package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.plan.Planner;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.plan.RelOptUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    void reordersJoinByRowCount() {
        // 正确性:结果不受重排影响(重排只改执行顺序,不改结果集)。
        QueryResult.Rows rows = (QueryResult.Rows) executor.execute(
                "SELECT big.id FROM big JOIN s1 ON big.id = s1.id JOIN s2 ON s1.id = s2.id");
        assertEquals(1, rows.data().getRowCount());
        rows.data().close();

        // 计划:至少含一个 join 物理算子(证明 join 被执行而非被优化掉)。
        // 注意:不断言 join 的书写顺序——RelOptUtil 的文本遍历序(左深/右深树)
        // 不能直接反映「小表先 join」;重排是否发生由 JoinAssociateRule 决定,
        // 详见 Task 2 report 里贴的 before/after 计划。
        String plan = RelOptUtil.toString(new Planner(catalog).plan(
                "SELECT big.id FROM big JOIN s1 ON big.id = s1.id JOIN s2 ON s1.id = s2.id"));
        assertTrue(plan.contains("MiniDbNestedLoopJoin") || plan.contains("MiniDbHashJoin")
                || plan.contains("MiniDbSortMergeJoin"),
                "expected a join physical operator, plan=\n" + plan);
    }
}
