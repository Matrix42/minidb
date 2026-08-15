package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableSchema;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 存储格式(Arrow/Parquet)的路由与读写。SQL 层不再暴露 FORMAT 子句(见 #P232),
 * 存储格式由 {@link TableSchema#storageFormat()} 决定;parquet 表经程序化建表验证。
 */
class StorageFormatTest {

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
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    /** 程序化建一张 parquet 表(SQL 层无 FORMAT 子句,引擎入口走 TableSchema.storageFormat)。 */
    private void createParquetTable(String name, List<ColumnMeta> columns) {
        storage.createTable(new TableSchema("public", name, columns)
                .withStorageFormat(StorageFormat.PARQUET));
    }

    @Test
    void defaultIsArrow() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        assertEquals(StorageFormat.ARROW, catalog.getTable("public", "t").storageFormat());
        executor.execute("INSERT INTO t VALUES (1)");
        assertEquals(1, storage.getTable("public", "t").rowCount());
    }

    @Test
    void parquetInsertAndReadBack() {
        createParquetTable("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR)));
        assertEquals(StorageFormat.PARQUET, catalog.getTable("public", "t").storageFormat());

        executor.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')");
        assertEquals(2, storage.getTable("public", "t").rowCount());

        QueryResult result = executor.execute("SELECT id, name FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) result).data();
        assertEquals(2, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        assertEquals(2, ((IntVector) root.getVector("id")).get(1));
        assertEquals("alice", new String(
                ((VarCharVector) root.getVector("name")).get(0), StandardCharsets.UTF_8));
        assertEquals("bob", new String(
                ((VarCharVector) root.getVector("name")).get(1), StandardCharsets.UTF_8));
        root.close();

        // part 文件用 .parquet 扩展名,不落 .arrow。
        Path tableDir = dataDir.resolve("public").resolve("t");
        try (var stream = Files.list(tableDir)) {
            List<String> names = stream.map(p -> p.getFileName().toString()).toList();
            assertEquals(List.of("part-000001.parquet"), names);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void parquetRoundTripTypes() {
        createParquetTable("t", List.of(
                new ColumnMeta("i", ColumnType.INTEGER),
                new ColumnMeta("b", ColumnType.BIGINT),
                new ColumnMeta("d", ColumnType.DOUBLE),
                new ColumnMeta("s", ColumnType.VARCHAR),
                new ColumnMeta("flag", ColumnType.BOOLEAN),
                new ColumnMeta("tm", ColumnType.TIME),
                new ColumnMeta("bin", ColumnType.VARBINARY)));
        executor.execute("INSERT INTO t VALUES"
                + " (1, 10000000000, 1.5, '字符', TRUE, TIME '10:30:00', X'DEADBEEF'),"
                + " (NULL, NULL, NULL, NULL, NULL, NULL, NULL)");

        QueryResult result = executor.execute("SELECT i, b, d, s, flag, tm, bin FROM t ORDER BY i NULLS LAST");
        VectorSchemaRoot root = ((QueryResult.Rows) result).data();
        assertEquals(2, root.getRowCount());

        IntVector i = (IntVector) root.getVector("i");
        BigIntVector b = (BigIntVector) root.getVector("b");
        Float8Vector d = (Float8Vector) root.getVector("d");
        VarCharVector s = (VarCharVector) root.getVector("s");
        BitVector flag = (BitVector) root.getVector("flag");
        TimeMilliVector tm = (TimeMilliVector) root.getVector("tm");
        VarBinaryVector bin = (VarBinaryVector) root.getVector("bin");

        assertEquals(1, i.get(0));
        assertEquals(10000000000L, b.get(0));
        assertEquals(1.5, d.get(0), 1e-9);
        assertEquals("字符", new String(s.get(0), StandardCharsets.UTF_8));
        assertEquals(1, flag.get(0));
        assertEquals(10 * 3600_000 + 30 * 60_000, tm.get(0));
        assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, bin.get(0));

        // 第二行全 NULL,回读后保持 NULL。
        assertTrue(i.isNull(1));
        assertTrue(b.isNull(1));
        assertTrue(d.isNull(1));
        assertTrue(s.isNull(1));
        assertTrue(flag.isNull(1));
        assertTrue(tm.isNull(1));
        assertTrue(bin.isNull(1));
        root.close();
    }
}
