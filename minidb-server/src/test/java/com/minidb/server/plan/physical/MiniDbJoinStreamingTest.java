package com.minidb.server.plan.physical;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.plan.Planner;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.TableHandle;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * B2 join 输出流式化回归:大结果集分批产出(每批 ≤ 4096,内存 O(批大小) 而非
 * O(结果行数)),批边界/余数正确,outer join 的 null-pad 行跨批分布正确。
 */
class MiniDbJoinStreamingTest {

    @TempDir
    Path dataDir;

    @Test
    void joinOutputStreamsInBatches(@TempDir Path dir) throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, val VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                // 100×100 = 10000 行匹配 > 2×4096,必须分多批产出
                writeRowsSameKey(storage, "a", 100);
                writeRowsSameKey(storage, "b", 100);

                for (MiniDbJoin join : joinStrategies(catalog, "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id")) {
                    BatchStats stats1 = collect(join, storage, allocator);
                    assertTrue(stats1.batches >= 3, join.getClass().getSimpleName()
                            + " 应分多批产出,实际 " + stats1.batches);
                    assertEquals(10_000, stats1.rows, join.getClass().getSimpleName() + " 总行数");
                    assertTrue(stats1.maxBatch <= 4096, join.getClass().getSimpleName()
                            + " 单批不得超过 4096,实际 " + stats1.maxBatch);
                }
            } finally {
                storage.close();
            }
        }
    }

    @Test
    void batchBoundaryRemainder(@TempDir Path dir) throws Exception {
        // 4096×2 + 17 = 8209 行:验证余数批(17 行)正确
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, val VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                // a 全 key=1 × b 1 行 key=1 → 8209 行输出
                writeRowsSameKey(storage, "a", 8209);
                writeRowsSameKey(storage, "b", 1);

                for (MiniDbJoin join : joinStrategies(catalog,
                        "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id")) {
                    BatchStats stats1 = collect(join, storage, allocator);
                    assertEquals(8209, stats1.rows, join.getClass().getSimpleName());
                    assertTrue(stats1.batches >= 3, join.getClass().getSimpleName());
                    // 最后一批是余数(≤4096)
                    assertTrue(stats1.lastBatch <= 4096, join.getClass().getSimpleName());
                }
            } finally {
                storage.close();
            }
        }
    }

    @Test
    void fullJoinNullPadsAcrossBatchBoundary(@TempDir Path dir) throws Exception {
        // 匹配 100×100=10000 + 右表 50 行无匹配 → null-pad 行跨批(阶段 2/3 产出)
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, val VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                writeRowsSameKey(storage, "a", 100);
                writeRowsSameKey(storage, "b", 100);
                writeRows(storage, "b", 50, 1000); // 右表 50 行无匹配(key 1000..1049)

                for (MiniDbJoin join : joinStrategies(catalog,
                        "SELECT a.id, b.id FROM a FULL JOIN b ON a.id = b.id")) {
                    // FULL:10000 匹配 + 50 右未匹配 + 0 左未匹配(a 全匹配) = 10050
                    List<int[]> pairs = collectPairs(join, storage, allocator);
                    assertEquals(10_050, pairs.size(), join.getClass().getSimpleName());
                    long nullPadded = pairs.stream().filter(p -> p[0] == -1 || p[1] == -1).count();
                    assertEquals(50, nullPadded, join.getClass().getSimpleName() + " null-pad 行数");
                }
            } finally {
                storage.close();
            }
        }
    }

    private List<MiniDbJoin> joinStrategies(MiniDbCatalog catalog, String sql) {
        RelNode plan = new Planner(catalog).plan(sql);
        MiniDbJoin join = findJoin(plan);
        return List.of(
                new MiniDbHashJoin(join.getCluster(), join.getTraitSet(),
                        join.getLeft(), join.getRight(), join.getCondition(), join.getJoinType()),
                new MiniDbSortMergeJoin(join.getCluster(), join.getTraitSet(),
                        join.getLeft(), join.getRight(), join.getCondition(), join.getJoinType()),
                new MiniDbNestedLoopJoin(join.getCluster(), join.getTraitSet(),
                        join.getLeft(), join.getRight(), join.getCondition(), join.getJoinType()));
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

    private record BatchStats(int batches, long rows, int maxBatch, int lastBatch) {}

    private static BatchStats collect(MiniDbJoin join, StorageManager storage,
                                      BufferAllocator allocator) {
        BatchIterator it = join.execute(new ExecContext(storage, allocator));
        int batches = 0;
        long rows = 0;
        int maxBatch = 0;
        int lastBatch = 0;
        while (it.hasNext()) {
            VectorSchemaRoot b = it.next();
            batches++;
            rows += b.getRowCount();
            maxBatch = Math.max(maxBatch, b.getRowCount());
            lastBatch = b.getRowCount();
        }
        it.close();
        return new BatchStats(batches, rows, maxBatch, lastBatch);
    }

    /** 收集行对:null-pad 侧记为 -1(join 输出该侧为 null)。 */
    private static List<int[]> collectPairs(MiniDbJoin join, StorageManager storage,
                                            BufferAllocator allocator) {
        BatchIterator it = join.execute(new ExecContext(storage, allocator));
        int leftCols = join.getLeft().getRowType().getFieldCount();
        int rightCols = join.getRight().getRowType().getFieldCount();
        List<int[]> pairs = new ArrayList<>();
        while (it.hasNext()) {
            VectorSchemaRoot b = it.next();
            for (int r = 0; r < b.getRowCount(); r++) {
                boolean leftNull = true;
                for (int c = 0; c < leftCols; c++) {
                    if (!b.getVector(c).isNull(r)) {
                        leftNull = false;
                        break;
                    }
                }
                boolean rightNull = true;
                for (int c = leftCols; c < leftCols + rightCols; c++) {
                    if (!b.getVector(c).isNull(r)) {
                        rightNull = false;
                        break;
                    }
                }
                pairs.add(new int[]{leftNull ? -1 : 1, rightNull ? -1 : 1});
            }
        }
        it.close();
        return pairs;
    }

    private static void writeRowsSameKey(StorageManager storage, String table, int n) {
        TableHandle t = storage.getTable("public", table);
        VectorSchemaRoot root = t.newBatchRoot();
        root.allocateNew();
        root.setRowCount(n);
        for (int i = 0; i < n; i++) {
            ((IntVector) root.getVector(0)).setSafe(i, 1); // 全同 key,产生 n×n 匹配
            ((VarCharVector) root.getVector(1)).setSafe(i, ("v" + i).getBytes());
        }
        t.writePart(root, TableHandle.Operation.INSERT);
        root.close();
    }

    private static void writeRows(StorageManager storage, String table, int n, int keyBase) {
        TableHandle t = storage.getTable("public", table);
        VectorSchemaRoot root = t.newBatchRoot();
        root.allocateNew();
        root.setRowCount(n);
        for (int i = 0; i < n; i++) {
            ((IntVector) root.getVector(0)).setSafe(i, keyBase + i);
            ((VarCharVector) root.getVector(1)).setSafe(i, ("v" + (keyBase + i)).getBytes());
        }
        t.writePart(root, TableHandle.Operation.INSERT);
        root.close();
    }
}
