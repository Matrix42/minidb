package com.minidb.server.plan.physical;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.math.BigDecimal;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
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
 * 端到端验证聚合累加器对新增标量类型按 Calcite 推导的输出类型落向量:
 * SUM/AVG(DECIMAL) -> DecimalVector、SUM/AVG(REAL) -> Float4Vector、
 * SUM/AVG/MIN/MAX(SMALLINT) -> SmallIntVector。
 */
class MiniDbAggregateTest {

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
    void sumAvgDecimalWritesDecimalVector() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        executor.execute("INSERT INTO t VALUES (10.50), (20.50), (30.00)");
        QueryResult r = executor.execute("SELECT SUM(price) AS s, AVG(price) AS a FROM t");
        VectorSchemaRoot root = rows(r);
        ValueVector s = root.getVector("s");
        ValueVector a = root.getVector("a");
        // SUM(DECIMAL(10,2)) -> DECIMAL(19,2)、AVG(DECIMAL(10,2)) -> DECIMAL(10,2),都落 DecimalVector。
        assertTrue(s instanceof DecimalVector,
                "expected DecimalVector for SUM, got " + s.getClass().getSimpleName());
        assertTrue(a instanceof DecimalVector,
                "expected DecimalVector for AVG, got " + a.getClass().getSimpleName());
        assertEquals(0, new BigDecimal("61.00").compareTo((BigDecimal) s.getObject(0)));
        // 61.00 / 3 = 20.3333...,按输出向量 scale=2 HALF_UP 舍入为 20.33。
        assertEquals(0, new BigDecimal("20.33").compareTo((BigDecimal) a.getObject(0)));
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
        assertTrue(s instanceof Float4Vector,
                "expected Float4Vector for SUM, got " + s.getClass().getSimpleName());
        assertTrue(a instanceof Float4Vector,
                "expected Float4Vector for AVG, got " + a.getClass().getSimpleName());
        assertEquals(4.0f, ((Float4Vector) s).get(0), 1e-6f);
        assertEquals(2.0f, ((Float4Vector) a).get(0), 1e-6f);
        root.close();
    }

    @Test
    void sumAvgMinMaxSmallIntWritesSmallIntVector() {
        executor.execute("CREATE TABLE t (x SMALLINT)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        QueryResult r = executor.execute(
                "SELECT SUM(x) AS s, AVG(x) AS a, MIN(x) AS mn, MAX(x) AS mx FROM t");
        VectorSchemaRoot root = rows(r);
        // SUM/AVG/MIN/MAX(SMALLINT) -> SMALLINT,落 SmallIntVector。
        for (String name : new String[]{"s", "a", "mn", "mx"}) {
            assertTrue(root.getVector(name) instanceof SmallIntVector,
                    "expected SmallIntVector for " + name + ", got "
                            + root.getVector(name).getClass().getSimpleName());
        }
        assertEquals(6, ((SmallIntVector) root.getVector("s")).get(0));
        assertEquals(2, ((SmallIntVector) root.getVector("a")).get(0));
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
        assertTrue(s instanceof DecimalVector,
                "expected DecimalVector for SUM(DISTINCT), got " + s.getClass().getSimpleName());
        assertEquals(0, new BigDecimal("31.00").compareTo((BigDecimal) s.getObject(0)));
        root.close();
    }
}
