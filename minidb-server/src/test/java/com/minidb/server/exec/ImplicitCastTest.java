package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
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

class ImplicitCastTest {

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
        executor.execute("CREATE TABLE t (i INTEGER, b BIGINT, d DOUBLE)");
        executor.execute("INSERT INTO t VALUES (5, 100, 2.5), (NULL, NULL, NULL)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    private ValueVector column(String sql) {
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        return root.getVector(0);
    }

    @Test
    void intPlusDouble() {
        Float8Vector v = (Float8Vector) column("SELECT i + d FROM t WHERE i IS NOT NULL");
        assertEquals(7.5, v.get(0), 1e-9);
        v.close();
    }

    @Test
    void intTimesDouble() {
        Float8Vector v = (Float8Vector) column("SELECT i * d FROM t WHERE i IS NOT NULL");
        assertEquals(12.5, v.get(0), 1e-9);
        v.close();
    }

    @Test
    void intDivideByDouble() {
        Float8Vector v = (Float8Vector) column("SELECT i / d FROM t WHERE i IS NOT NULL");
        assertEquals(2.0, v.get(0), 1e-9);
        v.close();
    }

    @Test
    void doublePlusIntLiteral() {
        Float8Vector v = (Float8Vector) column("SELECT d + 1 FROM t WHERE i IS NOT NULL");
        assertEquals(3.5, v.get(0), 1e-9);
        v.close();
    }

    @Test
    void bigintPlusDouble() {
        Float8Vector v = (Float8Vector) column("SELECT b + d FROM t WHERE i IS NOT NULL");
        assertEquals(102.5, v.get(0), 1e-9);
        v.close();
    }

    @Test
    void intPlusDecimalLiteral() {
        // 2.5 是 DECIMAL 字面量(非 DOUBLE),结果 DECIMAL。
        DecimalVector v = (DecimalVector) column("SELECT i + 2.5 FROM t WHERE i IS NOT NULL");
        assertEquals(0, v.getObject(0).compareTo(new BigDecimal("7.5")));
        v.close();
    }

    @Test
    void intMinusDecimalLiteral() {
        DecimalVector v = (DecimalVector) column("SELECT i - 2.5 FROM t WHERE i IS NOT NULL");
        assertEquals(0, v.getObject(0).compareTo(new BigDecimal("2.5")));
        v.close();
    }

    @Test
    void nullPropagatesThroughMixedArithmetic() {
        // NULL 操作数 → 结果 NULL(跨族混合算术同样遵守 STRICT 语义)。
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("SELECT i + d FROM t WHERE i IS NULL")).data();
        Float8Vector v = (Float8Vector) root.getVector(0);
        assertEquals(1, v.getValueCount());
        assertTrue(v.isNull(0));
        root.close();
    }
}
