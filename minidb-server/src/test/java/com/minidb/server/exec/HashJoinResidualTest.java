package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HashJoin 支持残留(非等值)条件:等值键先匹配,残留条件再过滤匹配对。若只按等值键匹配而忽略
 * 残留(query13/15 的 AND(等值键, OR 残留)),会多出本不该匹配的行。
 */
class HashJoinResidualTest {

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
        executor.execute("CREATE TABLE a (id INTEGER, val INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER, val INTEGER)");
        executor.execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
        executor.execute("INSERT INTO b VALUES (1, 100), (2, 200), (3, 300)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void hashJoinAppliesResidualCondition() {
        // 残留 OR 引用两侧:只保留 (a.val>15 OR b.val>250) 的匹配对。
        String sql = "SELECT a.id, b.val AS bval FROM a JOIN b"
                + " ON a.id = b.id AND (a.val > 15 OR b.val > 250)"
                + " ORDER BY a.id";
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        try {
            assertEquals(2, root.getRowCount());
            assertEquals(2, root.getVector("id").getObject(0));
            assertEquals(200, root.getVector("bval").getObject(0));
            assertEquals(3, root.getVector("id").getObject(1));
            assertEquals(300, root.getVector("bval").getObject(1));
        } finally {
            root.close();
        }

        RelNode plan = new Planner(catalog).plan(sql);
        List<MiniDbJoin> joins = new ArrayList<>();
        collectJoins(plan, joins);
        assertTrue(joins.stream().anyMatch(j -> j instanceof MiniDbHashJoin),
                "expected a HashJoin (equijoin + residual), got " + joins);
        assertTrue(joins.stream().noneMatch(j -> j instanceof MiniDbNestedLoopJoin),
                "expected no NestedLoopJoin, got " + joins);
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
