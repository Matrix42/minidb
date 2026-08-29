package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 回归:大事实表 + 小维度表 + 小表的非等值连接(query8 形态)。
 *
 * <p>连接图:fact --(equi)-- dim_a, fact --(equi)-- dim_b, dim_a --(non-equi substr)-- sub。 三表与
 * fact/事实表都有等值边,连接度上 fact、dim_a 均为 2(平手)。若贪心平手取下标 最小,fact(下标0,行数最多)当选种子,非等值条件落到最后加入的 sub 所在 join,接在
 * (fact⨝dim_a⨝dim_b) 大表结果上。修后按行数破平手,dim_a(小)当选种子,非等值 NestedLoop 接在 dim_a×sub 之间(几百行),不再卷入大表。
 */
class JoinReorderNonEquiTest {

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
        executor.execute("CREATE TABLE fact (sk1 INTEGER, sk2 INTEGER, zip VARCHAR)");
        executor.execute("CREATE TABLE dim_a (pk INTEGER, zip VARCHAR)");
        executor.execute("CREATE TABLE dim_b (pk INTEGER)");
        executor.execute("CREATE TABLE sub (zip VARCHAR)");
        // fact 远大于维度表,模拟 store_sales >> store/V1。行数差异要足够大,
        // 使 ANALYZE 后 mq.getRowCount(fact) >> 其余表,行数破平手才能稳定生效。
        for (int i = 0; i < 60; i++) {
            executor.execute("INSERT INTO fact VALUES (1,1,'10000'),(2,2,'20000'),(3,3,'30000')");
        }
        executor.execute("INSERT INTO dim_a VALUES (1,'10000'),(2,'20000')");
        executor.execute("INSERT INTO dim_b VALUES (1),(2),(3)");
        executor.execute("INSERT INTO sub VALUES ('10000'),('20000')");
        executor.execute("ANALYZE");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void nonEquiJoinNotOnTopOfLargeFactTable() {
        // fact(sk1=dim_a.pk 等值, sk2=dim_b.pk 等值, dim_a.zip 与 sub.zip 非等值 substr)。
        // FROM 顺序按 fact,dim_a,dim_b,sub 书写(模拟 TPC-DS query8)。
        String sql =
                "SELECT fact.sk1 FROM fact, dim_a, dim_b, sub"
                        + " WHERE fact.sk1 = dim_a.pk AND fact.sk2 = dim_b.pk"
                        + " AND substr(dim_a.zip,1,2) = substr(sub.zip,1,2)";

        RelNode plan = new Planner(catalog).plan(sql);
        List<Join> joins = new ArrayList<>();
        collectJoins(plan, joins);

        // 找含非等值 substr 条件的 join:它的左侧不应包含大表 fact(否则 NestedLoop 卷入大表)。
        Join nonEquiJoin = null;
        for (Join j : joins) {
            String cond = String.valueOf(j.getCondition());
            if (cond.contains("SUBSTRING")) {
                nonEquiJoin = j;
                break;
            }
        }
        assertTrue(nonEquiJoin != null, "应有含 substr 的非等值 join,实际 joins=" + joinConds(joins));
        // 非等值 join 的左子树不应含 fact:若含 fact,说明大表被卷进 NestedLoop 左侧。
        String leftLeaf = leftmostScanName(nonEquiJoin.getLeft());
        assertTrue(
                !"fact".equals(leftLeaf),
                "非等值 NestedLoop 的左输入不应是大表 fact(应是小表 dim_a),实际左叶子="
                        + leftLeaf
                        + ",joins="
                        + joinConds(joins));
    }

    private static void collectJoins(RelNode node, List<Join> out) {
        if (node instanceof Join join) {
            out.add(join);
        }
        for (RelNode in : node.getInputs()) {
            collectJoins(in, out);
        }
    }

    private static String leftmostScanName(RelNode node) {
        if (node instanceof TableScan scan) {
            List<String> q = scan.getTable().getQualifiedName();
            return q.get(q.size() - 1);
        }
        List<RelNode> inputs = node.getInputs();
        return inputs.isEmpty() ? node.getClass().getSimpleName() : leftmostScanName(inputs.get(0));
    }

    private static List<String> joinConds(List<Join> joins) {
        List<String> conds = new ArrayList<>();
        for (Join j : joins) {
            conds.add(String.valueOf(j.getCondition()));
        }
        return conds;
    }
}
