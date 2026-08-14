package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbProject;
import com.minidb.server.plan.physical.MiniDbScan;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.plan.Planner;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

        // 结构断言:重排后应是右深树 big ⋈ (s1 ⋈ s2)——小表先 join。
        // 直接走物理 RelNode 结构,不靠 RelOptUtil 字符串遍历序(文本序不能反映
        // 右深树的 join 顺序),这样移除 JoinCommuteRule/JoinAssociateRule 时
        // 会退化成左深 (big ⋈ s1) ⋈ s2,本断言即失败,真正锁住重排。
        RelNode root = new Planner(catalog).plan(
                "SELECT big.id FROM big JOIN s1 ON big.id = s1.id JOIN s2 ON s1.id = s2.id");
        String plan = RelOptUtil.toString(root);

        // Calcite 总插入一个列选择 Project,先展开到顶层 join。
        RelNode node = root;
        while (node instanceof MiniDbProject project) {
            node = project.getInput();
        }
        assertTrue(node instanceof MiniDbJoin, "expected top join, plan=\n" + plan);
        MiniDbJoin top = (MiniDbJoin) node;

        // 顶层 join 的一侧是 big 的扫描,另一侧是内层 join。
        MiniDbScan bigSide = scanNamed(top.getLeft(), top.getRight(), "big");
        MiniDbJoin inner = joinInput(top.getLeft(), top.getRight());
        assertNotNull(bigSide, "expected one top-join input to scan big, plan=\n" + plan);
        assertNotNull(inner, "expected the other top-join input to be an inner join, plan=\n" + plan);

        // 内层 join 的两侧是小表 s1 / s2(顺序不限)。
        Set<String> innerNames = new HashSet<>();
        innerNames.add(scanTableName(inner.getLeft()));
        innerNames.add(scanTableName(inner.getRight()));
        assertEquals(Set.of("s1", "s2"), innerNames,
                "expected inner join to be s1 ⋈ s2, plan=\n" + plan);
    }

    /** The scan of table {@code name} among {@code a}/{@code b}, or null. */
    private static MiniDbScan scanNamed(RelNode a, RelNode b, String name) {
        if (a instanceof MiniDbScan scan && name.equals(scanTableName(scan))) {
            return scan;
        }
        if (b instanceof MiniDbScan scan && name.equals(scanTableName(scan))) {
            return scan;
        }
        return null;
    }

    /** The join among {@code a}/{@code b}, or null. */
    private static MiniDbJoin joinInput(RelNode a, RelNode b) {
        if (a instanceof MiniDbJoin join) {
            return join;
        }
        if (b instanceof MiniDbJoin join) {
            return join;
        }
        return null;
    }

    private static String scanTableName(RelNode node) {
        assertTrue(node instanceof MiniDbScan, "expected MiniDbScan, got " + node);
        List<String> names = ((MiniDbScan) node).getTable().getQualifiedName();
        // 限定名如 [minidb, big] 或 [minidb, schema, t]——最后一段是表名。
        return names.get(names.size() - 1);
    }
}
