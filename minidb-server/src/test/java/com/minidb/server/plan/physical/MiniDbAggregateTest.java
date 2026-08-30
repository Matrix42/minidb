package com.minidb.server.plan.physical;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端验证聚合累加器对新增标量类型按 Calcite 推导的输出类型落向量: SUM/AVG(DECIMAL) -> DecimalVector、SUM/AVG(REAL) ->
 * Float4Vector、 SUM/AVG/MIN/MAX(SMALLINT) -> SmallIntVector。
 */
class MiniDbAggregateTest {

    @TempDir Path dataDir;
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
        stats = new StatsManager(storage);
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
    void sumAvgDecimalWritesDecimalVector() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        executor.execute("INSERT INTO t VALUES (10.50), (20.50), (30.00)");
        QueryResult r = executor.execute("SELECT SUM(price) AS s, AVG(price) AS a FROM t");
        VectorSchemaRoot root = rows(r);
        ValueVector s = root.getVector("s");
        ValueVector a = root.getVector("a");
        // SUM(DECIMAL(10,2)) -> DECIMAL(19,2)、AVG(DECIMAL(10,2)) -> 提升 scale 避免截断(scale=6)。
        assertTrue(
                s instanceof DecimalVector,
                "expected DecimalVector for SUM, got " + s.getClass().getSimpleName());
        assertTrue(
                a instanceof DecimalVector,
                "expected DecimalVector for AVG, got " + a.getClass().getSimpleName());
        assertEquals(0, new BigDecimal("61.00").compareTo((BigDecimal) s.getObject(0)));
        // 61.00 / 3 = 20.333333...,scale=2 时舍入为 20.33,scale=6 时保留 20.333333。
        assertEquals(0, new BigDecimal("20.333333").compareTo((BigDecimal) a.getObject(0)));
        root.close();
    }

    @Test
    void sumAvgRealWritesFloat4Vector() {
        executor.execute("CREATE TABLE t (x REAL)");
        executor.execute("INSERT INTO t VALUES (1.5), (2.5)");
        QueryResult r = executor.execute("SELECT SUM(x) AS s, AVG(x) AS a FROM t");
        VectorSchemaRoot root = rows(r);
        ValueVector s = root.getVector("s");
        ValueVector a = root.getVector("a");
        // SUM/AVG(REAL) -> REAL,落 Float4Vector(非 Float8Vector)。
        assertTrue(
                s instanceof Float4Vector,
                "expected Float4Vector for SUM, got " + s.getClass().getSimpleName());
        assertTrue(
                a instanceof Float4Vector,
                "expected Float4Vector for AVG, got " + a.getClass().getSimpleName());
        assertEquals(4.0f, ((Float4Vector) s).get(0), 1e-6f);
        assertEquals(2.0f, ((Float4Vector) a).get(0), 1e-6f);
        root.close();
    }

    @Test
    void sumAvgMinMaxSmallIntWritesSmallIntVector() {
        executor.execute("CREATE TABLE t (x SMALLINT)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        QueryResult r =
                executor.execute(
                        "SELECT SUM(x) AS s, AVG(x) AS a, MIN(x) AS mn, MAX(x) AS mx FROM t");
        VectorSchemaRoot root = rows(r);
        // SUM/MIN/MAX(SMALLINT) -> SMALLINT,落 SmallIntVector;AVG(SMALLINT) -> Float8Vector(精度提升)。
        assertTrue(root.getVector("s") instanceof SmallIntVector, "expected SmallIntVector for s");
        assertTrue(
                root.getVector("a") instanceof Float8Vector,
                "expected Float8Vector for a, got "
                        + root.getVector("a").getClass().getSimpleName());
        assertTrue(
                root.getVector("mn") instanceof SmallIntVector, "expected SmallIntVector for mn");
        assertTrue(
                root.getVector("mx") instanceof SmallIntVector, "expected SmallIntVector for mx");
        assertEquals(6, ((SmallIntVector) root.getVector("s")).get(0));
        assertEquals(2.0, ((Float8Vector) root.getVector("a")).get(0), 0.001);
        assertEquals(1, ((SmallIntVector) root.getVector("mn")).get(0));
        assertEquals(3, ((SmallIntVector) root.getVector("mx")).get(0));
        root.close();
    }

    @Test
    void sumDistinctDecimalWritesDecimalVector() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        executor.execute("INSERT INTO t VALUES (10.50), (20.50), (10.50)");
        QueryResult r = executor.execute("SELECT SUM(DISTINCT price) AS s FROM t");
        VectorSchemaRoot root = rows(r);
        ValueVector s = root.getVector("s");
        assertTrue(
                s instanceof DecimalVector,
                "expected DecimalVector for SUM(DISTINCT), got " + s.getClass().getSimpleName());
        assertEquals(0, new BigDecimal("31.00").compareTo((BigDecimal) s.getObject(0)));
        root.close();
    }

    @Test
    void stddevAndVarDouble() {
        executor.execute("CREATE TABLE t (x DOUBLE)");
        executor.execute("INSERT INTO t VALUES (1.0), (2.0), (3.0)");
        QueryResult r =
                executor.execute(
                        "SELECT STDDEV_SAMP(x) AS ss, STDDEV_POP(x) AS sp,"
                                + " VAR_SAMP(x) AS vs, VAR_POP(x) AS vp FROM t");
        VectorSchemaRoot root = rows(r);
        try {
            // 样本标准差 = sqrt(((1-2)^2+(2-2)^2+(3-2)^2)/2) = sqrt(1) = 1。
            assertEquals(1.0, ((Float8Vector) root.getVector("ss")).get(0), 1e-9);
            // 总体标准差 = sqrt(2/3)。
            assertEquals(Math.sqrt(2.0 / 3.0), ((Float8Vector) root.getVector("sp")).get(0), 1e-9);
            assertEquals(1.0, ((Float8Vector) root.getVector("vs")).get(0), 1e-9);
            assertEquals(2.0 / 3.0, ((Float8Vector) root.getVector("vp")).get(0), 1e-9);
        } finally {
            root.close();
        }
    }

    @Test
    void stddevSampSingleRowIsNull() {
        executor.execute("CREATE TABLE t (x INTEGER)");
        executor.execute("INSERT INTO t VALUES (42)");
        QueryResult r = executor.execute("SELECT STDDEV_SAMP(x) AS ss FROM t");
        VectorSchemaRoot root = rows(r);
        try {
            // n-1 = 0,样本标准差未定义,返回 NULL。
            assertTrue(root.getVector("ss").isNull(0));
        } finally {
            root.close();
        }
    }

    @Test
    void stddevEmptyTableIsNull() {
        executor.execute("CREATE TABLE t (x INTEGER)");
        QueryResult r = executor.execute("SELECT STDDEV_POP(x) AS sp FROM t");
        VectorSchemaRoot root = rows(r);
        try {
            assertTrue(root.getVector("sp").isNull(0));
        } finally {
            root.close();
        }
    }

    private long countOf(QueryResult r) {
        VectorSchemaRoot root = rows(r);
        try {
            return ((BigIntVector) root.getVector(0)).get(0);
        } finally {
            root.close();
        }
    }

    @Test
    void countStarReadsMetadata() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        assertEquals(3, countOf(executor.execute("SELECT COUNT(*) FROM t")));
    }

    @Test
    void countStarEmptyTableIsZero() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        assertEquals(0, countOf(executor.execute("SELECT COUNT(*) FROM t")));
    }

    @Test
    void countStarWithWhereStillScans() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        assertEquals(2, countOf(executor.execute("SELECT COUNT(*) FROM t WHERE id >= 2")));
    }

    @Test
    void countColumnStillIgnoresNulls() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (NULL), (3)");
        assertEquals(2, countOf(executor.execute("SELECT COUNT(id) FROM t")));
    }

    @Test
    void countStarParquet() {
        executor.execute("CREATE TABLE t (id INTEGER) WITH ('format'='parquet')");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        assertEquals(3, countOf(executor.execute("SELECT COUNT(*) FROM t")));
    }
}
