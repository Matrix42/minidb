package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 主键等值点查:WHERE pk = literal(全部主键列绑定)时 MiniDbScan 走
 * LSM 的 Bloom + getByKey 返回单行,而非全表扫描 + 逐行求值
 * (见 MiniDbScan.PointLookup)。本测试验证点查结果正确性与残留条件/
 * 投影/复合主键/矛盾条件等边界,并用 EXPLAIN ANALYZE 的批数验证
 * 点查确实只读一个 block。
 */
class PointLookupTest {

    @TempDir
    Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;
    ExplainExecutor explain;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
        explain = new ExplainExecutor(new Planner(catalog), stats, storage, allocator);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void intPkPointLookup() {
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertEquals(List.of("b"), rows(executor, "SELECT name FROM t WHERE id = 2"));
        assertEquals(List.of("b"), rows(executor, "SELECT name FROM t WHERE 2 = id"));
        // 未命中:空结果(而非错误)
        assertEquals(List.of(), rows(executor, "SELECT name FROM t WHERE id = 999"));
        // 点查后列裁剪
        assertEquals(List.of("2|b"), rows(executor, "SELECT id, name FROM t WHERE id = 2"));
    }

    @Test
    void bigintPkPointLookup() {
        // TPC-DS 维度 SK 列是 BIGINT 主键
        executor.execute("CREATE TABLE t (sk BIGINT NOT NULL PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (2450810, 'x'), (2450811, 'y')");
        assertEquals(List.of("y"), rows(executor, "SELECT name FROM t WHERE sk = 2450811"));
    }

    @Test
    void residualConditionFilters() {
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        // 残留条件(非主键等值)在点查单行上求值
        assertEquals(List.of("b"), rows(executor, "SELECT name FROM t WHERE id = 2 AND name = 'b'"));
        assertEquals(List.of(), rows(executor, "SELECT name FROM t WHERE id = 2 AND name = 'x'"));
    }

    @Test
    void contradictoryPkEqualityReturnsEmpty() {
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        // id=1 AND id=2 恒假:主键列重复等值不能丢条件
        assertEquals(List.of(), rows(executor, "SELECT name FROM t WHERE id = 1 AND id = 2"));
    }

    @Test
    void compositePkRequiresFullKey() {
        executor.execute("CREATE TABLE t (a INTEGER NOT NULL, b INTEGER NOT NULL, "
                + "name VARCHAR, PRIMARY KEY (a, b))");
        executor.execute("INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')");
        // 全部主键列绑定 → 点查
        assertEquals(List.of("y"), rows(executor, "SELECT name FROM t WHERE a = 1 AND b = 2"));
        // 只绑定部分主键 → 回退扫描,结果仍正确
        assertEquals(List.of("x", "y"), rows(executor, "SELECT name FROM t WHERE a = 1 ORDER BY b"));
    }

    @Test
    void nonPkAndRangeFallBackToScan() {
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        // 非主键列等值 / 范围 / 非等值:不点查,结果正确
        assertEquals(List.of("b"), rows(executor, "SELECT name FROM t WHERE name = 'b'"));
        assertEquals(List.of("b", "c"), rows(executor, "SELECT name FROM t WHERE id > 1 ORDER BY id"));
        assertEquals(List.of("a", "b"), rows(executor, "SELECT name FROM t WHERE id <= 2 ORDER BY id"));
        assertEquals(List.of("a", "c"), rows(executor, "SELECT name FROM t WHERE id <> 2 ORDER BY id"));
    }

    @Test
    void pointLookupStillReturnsLatestVersion() {
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'old')");
        executor.execute("UPDATE t SET name = 'new' WHERE id = 1");
        assertEquals(List.of("new"), rows(executor, "SELECT name FROM t WHERE id = 1"));
    }

    @Test
    void explainAnalyzePointLookupReadsSingleBlock() throws Exception {
        // 小 memtable 阈值让 INSERT 期间 flush 出多 block 的 SSTable,
        // 这样点查(1 block)与范围扫描(全 block)的批数可区分。
        Files.writeString(dataDir.resolve("config.yaml"), "lsm:\n  memtable-size-mb: 1\n");
        MiniDbCatalog catalog2 = new MiniDbCatalog();
        StorageManager storage2 = new StorageManager(catalog2, allocator, dataDir);
        try {
            QueryExecutor executor2 = new QueryExecutor(catalog2, storage2, allocator,
                    new StatsManager(storage2));
            executor2.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO t VALUES ");
            for (int i = 1; i <= 20000; i++) {
                if (i > 1) sb.append(", ");
                sb.append('(').append(i).append(", 'name-").append(i).append('-')
                        .append("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')");
            }
            executor2.execute(sb.toString());
            // 数据量 > memtable 阈值(1MB)已触发 flush,确认落盘多块
            ExplainExecutor explain2 = new ExplainExecutor(new Planner(catalog2),
                    new StatsManager(storage2), storage2, allocator);

            int pointBatches = scanBatches(explain2, "SELECT * FROM t WHERE id = 15000");
            int rangeBatches = scanBatches(explain2, "SELECT * FROM t WHERE id > 100");
            assertEquals(1, pointBatches, "点查应只读 1 个 block");
            assertTrue(rangeBatches > 1, "范围扫描应读多个 block,实际 " + rangeBatches);
        } finally {
            storage2.close();
        }
    }

    private static int scanBatches(ExplainExecutor explain, String sql) {
        QueryResult r = explain.analyze(sql);
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        try {
            VarCharVector op = (VarCharVector) root.getVector("operation");
            IntVector batches = (IntVector) root.getVector("batches");
            for (int i = 0; i < root.getRowCount(); i++) {
                // 下推 filter 的扫描在 EXPLAIN 中显示为 Filter(table)(见 operationName)
                if (new String(op.get(i)).startsWith("Filter")) {
                    return batches.isNull(i) ? 0 : batches.get(i);
                }
            }
            return -1;
        } finally {
            root.close();
        }
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
