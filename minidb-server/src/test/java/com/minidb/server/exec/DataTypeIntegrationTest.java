package com.minidb.server.exec;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端集成测试:证明 Task 1-9 引入的每个新标量类型都能走通 CREATE → INSERT(VALUES) →
 * SELECT round-trip,值语义正确、落正确的原生 Arrow 向量;并覆盖 DECIMAL 精确算术/聚合、
 * 比较过滤与重启持久化(precision/scale 不丢)。
 *
 * 各测试故意用单列表(每类型一表),避开 Calcite 对「含 CAST 的多行/多列 VALUES」生成
 * LogicalUnion 的历史坑(见 CLAUDE.md 坑 23);值断言直接读 VectorSchemaRoot 的向量取值。
 */
class DataTypeIntegrationTest {

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

    // ---- CREATE + INSERT + SELECT round-trip ----

    @Test
    void smallIntRoundTrip() {
        executor.execute("CREATE TABLE t (v SMALLINT)");
        executor.execute("INSERT INTO t VALUES (-32768), (0), (32767)");
        QueryResult r = executor.execute("SELECT v FROM t ORDER BY v");
        VectorSchemaRoot root = rows(r);
        assertEquals(3, root.getRowCount());
        assertTrue(root.getVector("v") instanceof SmallIntVector,
                "expected SmallIntVector, got " + root.getVector("v").getClass().getSimpleName());
        SmallIntVector v = (SmallIntVector) root.getVector("v");
        assertEquals(-32768, v.get(0));
        assertEquals(0, v.get(1));
        assertEquals(32767, v.get(2));
        root.close();
    }

    @Test
    void realAndFloatRoundTrip() {
        // REAL 与 FLOAT 都归一为单精度 Float4(见 ArrowTypes.toCalciteType),两者同落 Float4Vector。
        executor.execute("CREATE TABLE t (r REAL, f FLOAT)");
        executor.execute("INSERT INTO t VALUES (1.5, 2.25)");
        QueryResult r = executor.execute("SELECT r, f FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(1, root.getRowCount());
        assertTrue(root.getVector("r") instanceof Float4Vector,
                "expected Float4Vector for REAL, got "
                        + root.getVector("r").getClass().getSimpleName());
        assertTrue(root.getVector("f") instanceof Float4Vector,
                "expected Float4Vector for FLOAT, got "
                        + root.getVector("f").getClass().getSimpleName());
        assertEquals(1.5f, ((Float4Vector) root.getVector("r")).get(0), 1e-6f);
        assertEquals(2.25f, ((Float4Vector) root.getVector("f")).get(0), 1e-6f);
        root.close();
    }

    @Test
    void decimalRoundTripKeepsExactScale() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        // 先断言 DDL 落库的 precision/scale,再走 INSERT → SELECT 验证值。
        ColumnMeta price = catalog.getTable("t").columns().get(0);
        assertEquals(ColumnType.DECIMAL, price.type());
        assertEquals(10, price.precision());
        assertEquals(2, price.scale());

        executor.execute("INSERT INTO t VALUES (1.23), (-45.67), (100.00)");
        QueryResult r = executor.execute("SELECT price FROM t ORDER BY price");
        VectorSchemaRoot root = rows(r);
        assertEquals(3, root.getRowCount());
        assertTrue(root.getVector("price") instanceof DecimalVector,
                "expected DecimalVector, got "
                        + root.getVector("price").getClass().getSimpleName());
        DecimalVector v = (DecimalVector) root.getVector("price");
        assertEquals(0, new BigDecimal("-45.67").compareTo(v.getObject(0)));
        assertEquals(0, new BigDecimal("1.23").compareTo(v.getObject(1)));
        assertEquals(0, new BigDecimal("100.00").compareTo(v.getObject(2)));
        root.close();
    }

    @Test
    void numericFoldsToDecimal() {
        // NUMERIC 被 Calcite 归一为 DECIMAL,端到端同样落 DecimalVector(与 DataTypeDdlTest 对齐)。
        executor.execute("CREATE TABLE t (qty NUMERIC(8))");
        ColumnMeta qty = catalog.getTable("t").columns().get(0);
        assertEquals(ColumnType.DECIMAL, qty.type());
        assertEquals(8, qty.precision());

        executor.execute("INSERT INTO t VALUES (42)");
        QueryResult r = executor.execute("SELECT qty FROM t");
        VectorSchemaRoot root = rows(r);
        assertTrue(root.getVector("qty") instanceof DecimalVector,
                "expected DecimalVector for NUMERIC, got "
                        + root.getVector("qty").getClass().getSimpleName());
        assertEquals(0, new BigDecimal("42").compareTo(
                ((DecimalVector) root.getVector("qty")).getObject(0)));
        root.close();
    }

    @Test
    void charRoundTrip() {
        // CHAR 变长存储、不做空格填充(简化),落 VarCharVector(Utf8)。
        executor.execute("CREATE TABLE t (c CHAR)");
        executor.execute("INSERT INTO t VALUES ('abc')");
        QueryResult r = executor.execute("SELECT c FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(1, root.getRowCount());
        assertTrue(root.getVector("c") instanceof VarCharVector,
                "expected VarCharVector for CHAR, got "
                        + root.getVector("c").getClass().getSimpleName());
        assertEquals("abc", new String(
                ((VarCharVector) root.getVector("c")).get(0), StandardCharsets.UTF_8));
        root.close();
    }

    @Test
    void nvarcharRoundTripKeepsDeclaredName() {
        // NVARCHAR 与 CHAR 一样变长(Utf8)、不做空格填充;声明名靠 Arrow 元数据保真,
        // 落 VarCharVector、值精确往返(含中文)。
        executor.execute("CREATE TABLE t (nv NVARCHAR)");
        List<ColumnMeta> cols = catalog.getTable("t").columns();
        assertEquals(ColumnType.NVARCHAR, cols.get(0).type());

        executor.execute("INSERT INTO t VALUES ('字符')");
        QueryResult r = executor.execute("SELECT nv FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(1, root.getRowCount());
        assertTrue(root.getVector("nv") instanceof VarCharVector,
                "expected VarCharVector for NVARCHAR, got "
                        + root.getVector("nv").getClass().getSimpleName());
        assertEquals("字符", new String(
                ((VarCharVector) root.getVector("nv")).get(0), StandardCharsets.UTF_8));
        root.close();
    }

    @Test
    void ncharIsNotDdlCreatableButMetadataFidelityHolds() {
        // NCHAR 无法经 SQL DDL 创建:Calcite 1.42 的 DDL 解析器把 NCHAR 当作保留字、不列入
        // 类型名语法(SqlDdlParserImpl.TypeName() 只认 CHAR/CHARACTER/VARCHAR),`CREATE TABLE
        // ... NCHAR` 抛 ParseException。这是解析器上限,不是 MiniDB 存储/元数据层的问题:
        // 程序化建表 → 落盘 → 重载,ColumnType.NCHAR/NVARCHAR/BINARY 声明名经 Arrow
        // "minidb.type" 元数据仍端到端保真。
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("CREATE TABLE t (n NCHAR)"));

        storage.createTable(new TableSchema("nt", List.of(
                new ColumnMeta("n", ColumnType.NCHAR),
                new ColumnMeta("nv", ColumnType.NVARCHAR),
                new ColumnMeta("b", ColumnType.BINARY))));
        storage.markDirty("nt");
        storage.close();

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        StorageManager storage2 = new StorageManager(catalog2, allocator, dataDir);
        try {
            storage2.loadAll();
            List<ColumnMeta> cols = catalog2.getTable("nt").columns();
            assertEquals(ColumnType.NCHAR, cols.get(0).type());
            assertEquals(ColumnType.NVARCHAR, cols.get(1).type());
            assertEquals(ColumnType.BINARY, cols.get(2).type());
        } finally {
            storage2.close();
        }
    }

    @Test
    void timeRoundTrip() {
        executor.execute("CREATE TABLE t (tm TIME)");
        executor.execute("INSERT INTO t VALUES (TIME '10:30:00')");
        QueryResult r = executor.execute("SELECT tm FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(1, root.getRowCount());
        assertTrue(root.getVector("tm") instanceof TimeMilliVector,
                "expected TimeMilliVector, got "
                        + root.getVector("tm").getClass().getSimpleName());
        // 10:30:00 = 10*3600000 + 30*60000 = 37800000 毫秒(TimeMilliVector 存毫秒)。
        assertEquals(37_800_000, ((TimeMilliVector) root.getVector("tm")).get(0));
        root.close();
    }

    @Test
    void varbinaryRoundTrip() {
        // BINARY/VARBINARY 都落 VarBinaryVector;X'...' 是 Calcite 的十六进制二进制字面量。
        executor.execute("CREATE TABLE t (b VARBINARY)");
        executor.execute("INSERT INTO t VALUES (X'CAFE')");
        QueryResult r = executor.execute("SELECT b FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(1, root.getRowCount());
        assertTrue(root.getVector("b") instanceof VarBinaryVector,
                "expected VarBinaryVector, got "
                        + root.getVector("b").getClass().getSimpleName());
        assertArrayEquals(new byte[]{(byte) 0xCA, (byte) 0xFE},
                ((VarBinaryVector) root.getVector("b")).get(0));
        root.close();
    }

    @Test
    void binaryRoundTripDoesNotZeroPad() {
        // BINARY 与 VARBINARY 同为变长 Binary 存储(设计简化),不应像 CHAR 那样补零到声明
        // 长度;X'CAFE' 必须精确往返 {0xCA, 0xFE},无尾随零字节。
        executor.execute("CREATE TABLE t (b BINARY)");
        executor.execute("INSERT INTO t VALUES (X'CAFE')");
        QueryResult r = executor.execute("SELECT b FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(1, root.getRowCount());
        assertTrue(root.getVector("b") instanceof VarBinaryVector,
                "expected VarBinaryVector for BINARY, got "
                        + root.getVector("b").getClass().getSimpleName());
        byte[] value = ((VarBinaryVector) root.getVector("b")).get(0);
        assertEquals(2, value.length, "BINARY 不应补零到声明长度");
        assertArrayEquals(new byte[]{(byte) 0xCA, (byte) 0xFE}, value);
        root.close();
    }

    // ---- DECIMAL 精确算术 ----

    @Test
    void decimalArithmeticIsExact() {
        QueryResult r = executor.execute("SELECT 0.1 + 0.2 AS x");
        VectorSchemaRoot root = rows(r);
        ValueVector x = root.getVector("x");
        assertTrue(x instanceof DecimalVector,
                "expected DecimalVector, got " + x.getClass().getSimpleName());
        // 0.1 + 0.2 在 DECIMAL 域必须精确为 0.3,而非 Float8 的 0.30000000000000004。
        assertEquals(0, new BigDecimal("0.3").compareTo((BigDecimal) x.getObject(0)));
        root.close();
    }

    // ---- 聚合 ----

    @Test
    void aggregateOverDecimalIncludesMinMax() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        executor.execute("INSERT INTO t VALUES (10.50), (20.50), (30.00)");
        QueryResult r = executor.execute(
                "SELECT SUM(price) AS s, AVG(price) AS a, MIN(price) AS mn, MAX(price) AS mx FROM t");
        VectorSchemaRoot root = rows(r);
        for (String name : new String[]{"s", "a", "mn", "mx"}) {
            assertTrue(root.getVector(name) instanceof DecimalVector,
                    "expected DecimalVector for " + name + ", got "
                            + root.getVector(name).getClass().getSimpleName());
        }
        assertEquals(0, new BigDecimal("61.00").compareTo(
                (BigDecimal) root.getVector("s").getObject(0)));
        // 61.00 / 3 = 20.3333...,按输出向量 scale=2 HALF_UP 舍入为 20.33。
        assertEquals(0, new BigDecimal("20.33").compareTo(
                (BigDecimal) root.getVector("a").getObject(0)));
        // MIN/MAX(DECIMAL) 早前评审标记为未测试:值经 Comparable 比较后落回 DecimalVector。
        assertEquals(0, new BigDecimal("10.50").compareTo(
                (BigDecimal) root.getVector("mn").getObject(0)));
        assertEquals(0, new BigDecimal("30.00").compareTo(
                (BigDecimal) root.getVector("mx").getObject(0)));
        root.close();
    }

    @Test
    void aggregateOverSmallInt() {
        executor.execute("CREATE TABLE t (x SMALLINT)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        QueryResult r = executor.execute(
                "SELECT SUM(x) AS s, AVG(x) AS a, MIN(x) AS mn, MAX(x) AS mx FROM t");
        VectorSchemaRoot root = rows(r);
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
    void aggregateOverReal() {
        executor.execute("CREATE TABLE t (x REAL)");
        executor.execute("INSERT INTO t VALUES (1.5), (2.5)");
        QueryResult r = executor.execute("SELECT SUM(x) AS s, AVG(x) AS a FROM t");
        VectorSchemaRoot root = rows(r);
        assertTrue(root.getVector("s") instanceof Float4Vector,
                "expected Float4Vector for SUM, got "
                        + root.getVector("s").getClass().getSimpleName());
        assertTrue(root.getVector("a") instanceof Float4Vector,
                "expected Float4Vector for AVG, got "
                        + root.getVector("a").getClass().getSimpleName());
        assertEquals(4.0f, ((Float4Vector) root.getVector("s")).get(0), 1e-6f);
        assertEquals(2.0f, ((Float4Vector) root.getVector("a")).get(0), 1e-6f);
        root.close();
    }

    // ---- 比较过滤 ----

    @Test
    void comparisonFilteringOverNewTypes() {
        executor.execute("CREATE TABLE s (v SMALLINT)");
        executor.execute("INSERT INTO s VALUES (1), (2), (3)");
        QueryResult rs = executor.execute("SELECT v FROM s WHERE v > 1 ORDER BY v");
        VectorSchemaRoot root = rows(rs);
        assertEquals(2, root.getRowCount());
        SmallIntVector sv = (SmallIntVector) root.getVector("v");
        assertEquals(2, sv.get(0));
        assertEquals(3, sv.get(1));
        root.close();

        executor.execute("CREATE TABLE d (v DECIMAL(10,2))");
        executor.execute("INSERT INTO d VALUES (1.23), (2.50), (3.00)");
        QueryResult rd = executor.execute("SELECT v FROM d WHERE v = 1.23");
        VectorSchemaRoot rootD = rows(rd);
        assertEquals(1, rootD.getRowCount());
        assertEquals(0, new BigDecimal("1.23").compareTo(
                ((DecimalVector) rootD.getVector("v")).getObject(0)));
        rootD.close();

        executor.execute("CREATE TABLE tm (v TIME)");
        executor.execute("INSERT INTO tm VALUES (TIME '09:00:00'), (TIME '10:30:00'), (TIME '11:00:00')");
        QueryResult rt = executor.execute("SELECT v FROM tm WHERE v >= TIME '10:30:00'");
        VectorSchemaRoot rootT = rows(rt);
        assertEquals(2, rootT.getRowCount());
        TimeMilliVector tv = (TimeMilliVector) rootT.getVector("v");
        assertEquals(37_800_000, tv.get(0)); // 10:30:00
        assertEquals(39_600_000, tv.get(1)); // 11:00:00
        rootT.close();
    }

    // ---- 重启持久化 ----

    @Test
    void restartPreservesColumnTypesAndDecimalScale() {
        executor.execute("CREATE TABLE t "
                + "(s SMALLINT, p DECIMAL(10,2), c CHAR, b BINARY)");
        executor.execute("INSERT INTO t VALUES (1, 1.23, 'x', X'CAFE')");
        // close 触发 flushDirty,把表落盘到 data/<schema>/<table>.arrow。
        storage.close();

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        StorageManager storage2 = new StorageManager(catalog2, allocator, dataDir);
        try {
            storage2.loadAll();
            List<ColumnMeta> cols = catalog2.getTable("t").columns();
            assertEquals(ColumnType.SMALLINT, cols.get(0).type());
            assertEquals(ColumnType.DECIMAL, cols.get(1).type());
            assertEquals(10, cols.get(1).precision());
            assertEquals(2, cols.get(1).scale());
            assertEquals(ColumnType.CHAR, cols.get(2).type());
            assertEquals(ColumnType.BINARY, cols.get(3).type());

            // 数据本身也随持久化往返,证明 DECIMAL 值按 scale 精确落盘(而非退化浮点),
            // BINARY 不补零、声明名经 Arrow 元数据保真。
            VectorSchemaRoot batch = storage2.getTable("t").batches().get(0);
            assertEquals(1, batch.getRowCount());
            assertEquals(1, ((SmallIntVector) batch.getVector("s")).get(0));
            assertEquals(0, new BigDecimal("1.23").compareTo(
                    ((DecimalVector) batch.getVector("p")).getObject(0)));
            assertEquals("x", new String(
                    ((VarCharVector) batch.getVector("c")).get(0), StandardCharsets.UTF_8));
            assertArrayEquals(new byte[]{(byte) 0xCA, (byte) 0xFE},
                    ((VarBinaryVector) batch.getVector("b")).get(0));
        } finally {
            storage2.close();
        }
    }

    /**
     * Regression test for the window-function eager path: a SELECT with a window
     * function (ROW_NUMBER) over a table containing SMALLINT and DECIMAL columns
     * must not throw "cannot write value to <MinorType>" from MiniDbProject's
     * writeObject, which previously only handled the 7 original types.
     */
    @Test
    void windowFunctionOverNewTypes() {
        executor.execute("CREATE TABLE t (s SMALLINT, price DECIMAL(10,2))");
        executor.execute("INSERT INTO t VALUES (42, 123.45)");
        QueryResult r = executor.execute("SELECT s, price, ROW_NUMBER() OVER () FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(1, root.getRowCount());
        assertTrue(root.getVector("s") instanceof SmallIntVector,
                "expected SmallIntVector, got " + root.getVector("s").getClass().getSimpleName());
        assertEquals(42, ((SmallIntVector) root.getVector("s")).get(0));
        assertTrue(root.getVector("price") instanceof DecimalVector,
                "expected DecimalVector, got " + root.getVector("price").getClass().getSimpleName());
        assertEquals(0, new BigDecimal("123.45").compareTo(
                (BigDecimal) root.getVector("price").getObject(0)));
        // ROW_NUMBER over empty window with a single row must be 1
        assertEquals(1, ((Number) root.getVector(2).getObject(0)).intValue());
        root.close();
    }

    /**
     * Window SUM over a DECIMAL column must accumulate as BigDecimal (exact),
     * not via doubleValue() — 0.10+0.20 must be exactly 0.30, not a float.
     */
    @Test
    void windowSumDecimalIsExact() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        executor.execute("INSERT INTO t VALUES (0.10)");
        executor.execute("INSERT INTO t VALUES (0.20)");
        QueryResult r = executor.execute("SELECT SUM(price) OVER () FROM t");
        VectorSchemaRoot root = rows(r);
        assertEquals(2, root.getRowCount());
        ValueVector v = root.getVector(0);
        assertTrue(v instanceof DecimalVector,
                "expected DecimalVector, got " + v.getClass().getSimpleName());
        // Both rows share one window (no partition): SUM = 0.30 exactly.
        assertEquals(0, new BigDecimal("0.30").compareTo((BigDecimal) v.getObject(0)));
        assertEquals(0, new BigDecimal("0.30").compareTo((BigDecimal) v.getObject(1)));
        root.close();
    }
}
