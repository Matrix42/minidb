package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.MiniDbJoin;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies all three join strategies (HASH / SORT_MERGE / NESTED_LOOP) produce
 * identical results on the same equi-join, including outer joins with NULL
 * keys. The strategy is applied by rebuilding the plan's MiniDbJoin node with
 * an explicit Strategy.
 */
class JoinStrategyTest {

    @TempDir
    Path dataDir;

    @Test
    void allStrategiesProduceSameInnerResult() {
        run("SELECT a.id, b.val FROM a JOIN b ON a.id = b.id ORDER BY a.id");
    }

    @Test
    void allStrategiesProduceSameLeftJoinWithNullKeys() {
        run("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.id = b.id ORDER BY aid");
    }

    @Test
    void allStrategiesProduceSameFullJoin() {
        run("SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.id = b.id");
    }

    @Test
    void allStrategiesProduceSameMultiColumnJoin() {
        run("SELECT a.id, b.id AS bid FROM a JOIN b ON a.id = b.id AND a.name = b.val ORDER BY a.id");
    }

    private void run(String sql) {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage, allocator, dataDir);
            storage.setStatsManager(stats);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'y'), (NULL, 'z')");
                executor.execute("INSERT INTO b VALUES (2, 'y'), (3, 'y'), (4, 'w'), (NULL, 'z')");

                List<String> expected = null;
                for (MiniDbJoin.Strategy s : MiniDbJoin.Strategy.values()) {
                    if (s == MiniDbJoin.Strategy.AUTO) {
                        continue;
                    }
                    RelNode plan = new Planner(catalog).plan(sql);
                    MiniDbJoin join = findJoin(plan);
                    MiniDbJoin forced = new MiniDbJoin(join.getCluster(),
                            join.getTraitSet(), join.getLeft(), join.getRight(),
                            join.getCondition(), join.getJoinType(), s);
                    List<String> rows = new ArrayList<>(executeRows(forced, storage, allocator));
                    rows.sort(String::compareTo); // join output order is not guaranteed
                    if (expected == null) {
                        expected = rows;
                    } else {
                        assertEquals(expected, rows,
                                "strategy " + s + " diverged");
                    }
                }
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

    private static List<String> executeRows(MiniDbJoin join, StorageManager storage,
                                            BufferAllocator allocator) {
        BatchIterator it = join.execute(new ExecContext(storage, allocator));
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
                        sb.append(root.getVector(c).isNull(r)
                                ? "NULL" : root.getVector(c).getObject(r));
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
