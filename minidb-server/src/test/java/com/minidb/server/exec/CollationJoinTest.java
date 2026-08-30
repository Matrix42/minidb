package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.plan.physical.MiniDbSortMergeJoin;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.BatchIterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a join whose both inputs are pre-sorted on the join keys is planned as {@link
 * MiniDbSortMergeJoin} with collation-aware skip-sorting (no internal re-sort of either input).
 */
class CollationJoinTest {

    @TempDir Path dataDir;

    @Test
    void preSortedInputsPickSortMergeJoinAndSkipSorting() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')");
                executor.execute("INSERT INTO b VALUES (2, 'u'), (3, 'v'), (4, 'w')");

                String sql =
                        "SELECT s.id, t.id AS bid "
                                + "FROM (SELECT * FROM a ORDER BY id LIMIT 1000) s "
                                + "JOIN (SELECT * FROM b ORDER BY id LIMIT 1000) t ON s.id = t.id";
                RelNode plan = new Planner(catalog).plan(sql);
                MiniDbSortMergeJoin join = findSortMergeJoin(plan);
                assertTrue(join != null, "expected MiniDbSortMergeJoin, plan=" + plan);
                assertTrue(join.leftInputSorted(), "left input should be pre-sorted");
                assertTrue(join.rightInputSorted(), "right input should be pre-sorted");

                List<String> rows = executeRows(plan, storage, allocator);
                rows.sort(String::compareTo); // join output order not guaranteed
                assertEquals(List.of("2|2", "3|3"), rows);
            } finally {
                storage.close();
            }
        }
    }

    private static MiniDbSortMergeJoin findSortMergeJoin(RelNode node) {
        if (node instanceof MiniDbSortMergeJoin join) {
            return join;
        }
        for (RelNode in : node.getInputs()) {
            MiniDbSortMergeJoin found = findSortMergeJoin(in);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static List<String> executeRows(
            RelNode physical, StorageManager storage, BufferAllocator allocator) {
        // 与 JoinStrategyTest.executeRows 相同的逐字实现,但执行整个物理计划根
        // (顶层 MiniDbProject 做列投影),而非直接执行 join 节点。
        BatchIterator it = ((MiniDbRel) physical).execute(new ExecContext(storage, allocator));
        List<String> rows = new ArrayList<>();
        try {
            while (it.hasNext()) {
                VectorSchemaRoot root = it.next();
                for (int r = 0; r < root.getRowCount(); r++) {
                    StringBuilder sb = new StringBuilder();
                    for (int c = 0; c < root.getFieldVectors().size(); c++) {
                        if (c > 0) {
                            sb.append('|');
                        }
                        sb.append(
                                root.getVector(c).isNull(r)
                                        ? "NULL"
                                        : root.getVector(c).getObject(r));
                    }
                    rows.add(sb.toString());
                }
                root.close();
            }
        } finally {
            it.close();
        }
        return rows;
    }
}
