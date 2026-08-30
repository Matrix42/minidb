package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;
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
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies all three join strategies (HASH / SORT_MERGE / NESTED_LOOP) produce identical results on
 * the same equi-join, including outer joins with NULL keys. Each strategy is applied by rebuilding
 * the plan's MiniDbJoin node with the corresponding concrete implementation.
 */
class JoinStrategyTest {

    @TempDir Path dataDir;

    private static final List<Function<MiniDbJoin, MiniDbJoin>> MAKERS =
            List.of(
                    j ->
                            new MiniDbHashJoin(
                                    j.getCluster(),
                                    j.getTraitSet(),
                                    j.getLeft(),
                                    j.getRight(),
                                    j.getCondition(),
                                    j.getJoinType()),
                    j ->
                            new MiniDbSortMergeJoin(
                                    j.getCluster(),
                                    j.getTraitSet(),
                                    j.getLeft(),
                                    j.getRight(),
                                    j.getCondition(),
                                    j.getJoinType()),
                    j ->
                            new MiniDbNestedLoopJoin(
                                    j.getCluster(),
                                    j.getTraitSet(),
                                    j.getLeft(),
                                    j.getRight(),
                                    j.getCondition(),
                                    j.getJoinType()));

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
        run(
                "SELECT a.id, b.id AS bid FROM a JOIN b ON a.id = b.id AND a.name = b.val ORDER BY a.id");
    }

    @Test
    void allStrategiesHandleSimultaneousNullKeysInFullJoin() {
        // left=[1,NULL], right=[1,NULL]: after matching id=1 both merge pointers
        // hit the null-keyed region at the same time (SortMerge's
        // both-null branch). FULL must keep both null rows (one from each side).
        run(
                "SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.id = b.id",
                List.of(
                        "CREATE TABLE a (id INTEGER, name VARCHAR)",
                        "CREATE TABLE b (id INTEGER, val VARCHAR)",
                        "INSERT INTO a VALUES (1, 'x'), (NULL, 'z')",
                        "INSERT INTO b VALUES (1, 'y'), (NULL, 'w')"));
    }

    private void run(String sql) {
        run(
                sql,
                List.of(
                        "CREATE TABLE a (id INTEGER, name VARCHAR)",
                        "CREATE TABLE b (id INTEGER, val VARCHAR)",
                        "INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'y'), (NULL, 'z')",
                        "INSERT INTO b VALUES (2, 'y'), (3, 'y'), (4, 'w'), (NULL, 'z')"));
    }

    private void run(String sql, List<String> setup) {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                for (String stmt : setup) {
                    executor.execute(stmt);
                }

                List<String> expected = null;
                for (Function<MiniDbJoin, MiniDbJoin> maker : MAKERS) {
                    RelNode plan = new Planner(catalog).plan(sql);
                    MiniDbJoin join = findJoin(plan);
                    MiniDbJoin forced = maker.apply(join);
                    List<String> rows = new ArrayList<>(executeRows(forced, storage, allocator));
                    rows.sort(String::compareTo); // join output order is not guaranteed
                    if (expected == null) {
                        expected = rows;
                    } else {
                        assertEquals(expected, rows, "strategy diverged");
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

    private static List<String> executeRows(
            MiniDbJoin join, StorageManager storage, BufferAllocator allocator) {
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
