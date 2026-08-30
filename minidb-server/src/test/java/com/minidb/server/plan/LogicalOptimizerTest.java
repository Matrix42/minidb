package com.minidb.server.plan;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.plan.physical.MiniDbFilter;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbScan;
import com.minidb.server.plan.physical.MiniDbSort;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

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

class LogicalOptimizerTest {

    @TempDir Path dataDir;

    @Test
    void filterIsPushedIntoJoinInputs() {
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

                String sql = "SELECT a.id, b.val FROM a JOIN b ON a.id = b.id WHERE a.id > 2";
                RelNode plan = new Planner(catalog).plan(sql);
                MiniDbJoin join = findJoin(plan);
                assertTrue(join != null, "plan has no join: " + plan);
                // FilterPushDown: join 的输入中应有过滤——可能是 MiniDbFilter 或
                // 下推到 Scan 的 MiniDbScan(含 pushedFilter)
                assertTrue(
                        isDirectInput(join, MiniDbFilter.class)
                                || isDirectInput(
                                        join,
                                        MiniDbScan.class,
                                        scan -> ((MiniDbScan) scan).pushedFilter() != null),
                        "filter should be pushed into join inputs, plan=" + plan);

                List<String> rows = rows(executor, sql);
                assertEquals(1, rows.size());
                assertEquals("3|v", rows.get(0));
            } finally {
                storage.close();
            }
        }
    }

    @Test
    void sortRemovedWhenSortKeysAreConstant() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
                executor.execute("INSERT INTO t VALUES (1, 'a'), (1, 'b'), (2, 'c')");

                String sql = "SELECT * FROM t WHERE id = 1 ORDER BY id";
                RelNode plan = new Planner(catalog).plan(sql);
                // WHERE id=1 makes the sort key constant, so the Sort is redundant.
                assertTrue(
                        !containsSort(plan),
                        "sort should be removed when the sort key is constant, plan=" + plan);

                List<String> rows = rows(executor, sql);
                assertEquals(2, rows.size());
            } finally {
                storage.close();
            }
        }
    }

    private static boolean containsSort(RelNode node) {
        if (node instanceof MiniDbSort) {
            return true;
        }
        for (RelNode in : node.getInputs()) {
            if (containsSort(in)) {
                return true;
            }
        }
        return false;
    }

    private static MiniDbJoin findJoin(RelNode node) {
        if (node instanceof MiniDbJoin join) {
            return join;
        }
        for (RelNode in : node.getInputs()) {
            MiniDbJoin found = findJoin(in);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** True if any direct input of the join is an instance of clazz. */
    private static boolean isDirectInput(MiniDbJoin join, Class<?> clazz) {
        for (RelNode input : join.getInputs()) {
            if (clazz.isInstance(input)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDirectInput(
            MiniDbJoin join, Class<?> clazz, java.util.function.Predicate<RelNode> pred) {
        for (RelNode input : join.getInputs()) {
            if (clazz.isInstance(input) && pred.test(input)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> rows(QueryExecutor executor, String sql) {
        QueryResult result = executor.execute(sql);
        VectorSchemaRoot root = ((QueryResult.Rows) result).data();
        List<String> out = new ArrayList<>();
        try {
            for (int r = 0; r < root.getRowCount(); r++) {
                StringBuilder sb = new StringBuilder();
                for (int c = 0; c < root.getFieldVectors().size(); c++) {
                    if (c > 0) {
                        sb.append('|');
                    }
                    sb.append(
                            root.getVector(c).isNull(r) ? "NULL" : root.getVector(c).getObject(r));
                }
                out.add(sb.toString());
            }
        } finally {
            root.close();
        }
        return out;
    }
}
