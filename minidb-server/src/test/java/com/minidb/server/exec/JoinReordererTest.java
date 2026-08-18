package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JoinReorderer:FROM 顺序把一个表排在它的等值连接伙伴之前时,该表先交叉连接(cond=true)。
 * 贪心重排应按等值连接图把表挪到伙伴之后,消除本可避免的交叉连接(query18 的 cd2 在 customer
 * 之前、靠 c_current_cdemo_sk=cd2.cd_demo_sk 连接,重排前是笛卡尔积)。
 */
class JoinReordererTest {

    @TempDir
    Path dataDir;
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
        executor.execute("CREATE TABLE a (id INTEGER, bid INTEGER, cid INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("CREATE TABLE c (id INTEGER, did INTEGER)");
        executor.execute("CREATE TABLE d (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1, 10, 20), (2, 10, 21)");
        executor.execute("INSERT INTO b VALUES (10)");
        executor.execute("INSERT INTO c VALUES (20, 30), (21, 31)");
        executor.execute("INSERT INTO d VALUES (30), (31)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void reorderEliminatesCrossJoinAndPreservesSemantics() {
        // d 只经 c.did=d.id 连接 c,却排在 c 之前(FROM a,b,d,c);重排后应挪到 c 之后。
        // 选 d.id 是为了验证字段顺序被还原:重排后 d 移到末位,若不补 Project 会读到错列。
        String sql = "SELECT a.id, d.id AS did FROM a, b, d, c"
                + " WHERE a.bid = b.id AND a.cid = c.id AND c.did = d.id"
                + " ORDER BY a.id";
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        try {
            assertEquals(2, root.getRowCount());
            assertEquals(1, root.getVector("id").getObject(0));
            assertEquals(30, root.getVector("did").getObject(0));
            assertEquals(2, root.getVector("id").getObject(1));
            assertEquals(31, root.getVector("did").getObject(1));
        } finally {
            root.close();
        }

        RelNode plan = new Planner(catalog).plan(sql);
        List<Join> joins = new ArrayList<>();
        collectJoins(plan, joins);
        for (Join join : joins) {
            assertTrue(!join.getCondition().isAlwaysTrue(),
                    "expected no cross join after reordering, got " + join.getCondition());
        }
    }

    private static void collectJoins(RelNode node, List<Join> out) {
        if (node instanceof Join join) {
            out.add(join);
        }
        for (RelNode in : node.getInputs()) {
            collectJoins(in, out);
        }
    }
}
