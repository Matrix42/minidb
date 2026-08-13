package com.minidb.server.exec.functions;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.math.BigDecimal;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端验证表达式层对新增标量类型走原生 Arrow 向量(SmallInt/Float4/Decimal/Time/VarBinary),
 * 而非退化到 Int/Float8。DECIMAL 算术必须精确(BigDecimal 域),证明走的是 Decimal128 而非 Float8。
 */
class NewTypeExpressionTest {

    @TempDir
    Path dataDir;
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
        stats = new StatsManager(storage, allocator, dataDir);
        storage.setStatsManager(stats);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    private static VectorSchemaRoot rows(QueryResult r) {
        return ((QueryResult.Rows) r).data();
    }

    @Test
    void decimalArithmeticIsExact() {
        QueryResult r = executor.execute("SELECT 0.1 + 0.2 AS x");
        VectorSchemaRoot root = rows(r);
        ValueVector x = root.getVector("x");
        // DECIMAL 字面量相加必须产 DecimalVector(BigDecimal 域,精确 0.3),而非 Float8Vector。
        assertTrue(x instanceof DecimalVector,
                "expected DecimalVector, got " + x.getClass().getSimpleName());
        assertEquals(0, new BigDecimal("0.3").compareTo((BigDecimal) x.getObject(0)));
        root.close();
    }

    @Test
    void decimalLiteralProducesDecimalVector() {
        // 裸 `SELECT 1.25`(无 FROM)会被 Calcite 规划成 LogicalValues,走 MiniDbValues 而非表达式层;
        // 用表扫描强制走 RexInterpreter.literalVector,验证 DECIMAL 字面量产 DecimalVector。
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        QueryResult r = executor.execute("SELECT 1.25 AS x FROM t");
        VectorSchemaRoot root = rows(r);
        ValueVector x = root.getVector("x");
        assertTrue(x instanceof DecimalVector,
                "expected DecimalVector, got " + x.getClass().getSimpleName());
        assertEquals(0, new BigDecimal("1.25").compareTo((BigDecimal) x.getObject(0)));
        root.close();
    }

    @Test
    void smallIntArithmeticAndComparison() {
        QueryResult r = executor.execute(
                "SELECT CAST(1 AS SMALLINT) + CAST(2 AS SMALLINT) AS s, "
              + "CAST(1 AS SMALLINT) < CAST(2 AS SMALLINT) AS lt");
        VectorSchemaRoot root = rows(r);
        SmallIntVector s = (SmallIntVector) root.getVector("s");
        assertEquals(3, s.get(0));
        BitVector lt = (BitVector) root.getVector("lt");
        assertEquals(1, lt.get(0), "SMALLINT < SMALLINT 应走 Short 比较核");
        root.close();
    }

    @Test
    void timeComparison() {
        QueryResult r = executor.execute(
                "SELECT TIME '10:00:00' > TIME '09:00:00' AS gt");
        VectorSchemaRoot root = rows(r);
        BitVector gt = (BitVector) root.getVector("gt");
        assertEquals(1, gt.get(0), "TIME > TIME 应走毫秒比较核");
        root.close();
    }

    @Test
    void decimalCastProducesDecimalVector() {
        // CAST(列 AS DECIMAL) 不可常量折叠,真正走 RexInterpreter.evalCast 的 DECIMAL 分支。
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1)");
        QueryResult r = executor.execute("SELECT CAST(id AS DECIMAL(10,2)) AS x FROM t");
        VectorSchemaRoot root = rows(r);
        ValueVector x = root.getVector("x");
        assertTrue(x instanceof DecimalVector,
                "expected DecimalVector, got " + x.getClass().getSimpleName());
        assertEquals(0, new BigDecimal("1.00").compareTo((BigDecimal) x.getObject(0)));
        root.close();
    }
}
