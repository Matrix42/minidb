package com.minidb.server.plan;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.plan.physical.MiniDbFilter;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogicalOptimizerTest {

    @TempDir
    Path dataDir;

    @Test
    void filterIsPushedIntoJoinInputs() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage, allocator, dataDir);
            storage.setStatsManager(stats);
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
                // FilterPushDown: join 的直接输入应是 MiniDbFilter(或含之),而非 join 之上
                assertTrue(isDirectInput(join, MiniDbFilter.class),
                        "filter should be pushed into join inputs, plan=" + plan);

                List<String> rows = rows(executor, sql);
                assertEquals(1, rows.size());
                assertEquals("3|v", rows.get(0));
            } finally {
                storage.close();
            }
        }
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
        for (RelNode in : join.getInputs()) {
            if (clazz.isInstance(in)) {
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
                    sb.append(root.getVector(c).isNull(r)
                            ? "NULL" : root.getVector(c).getObject(r));
                }
                out.add(sb.toString());
            }
        } finally {
            root.close();
        }
        return out;
    }
}
