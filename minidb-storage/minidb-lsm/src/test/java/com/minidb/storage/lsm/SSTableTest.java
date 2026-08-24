package com.minidb.storage.lsm;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;
import com.minidb.storage.parquet.ParquetPartFormat;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableTest {
    private final TableSchema schema = new TableSchema("public", "t",
            List.of(new ColumnMeta("id", ColumnType.INTEGER), new ColumnMeta("name", ColumnType.VARCHAR)),
            List.of("id"), List.of(), List.of());
    private RootAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void writeAndReadRoundTrip(@TempDir Path dir) throws Exception {
        // 准备有序的 MemTable 数据（MemTable key 是 String 类型）
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "alice"}));
        mt.put(List.of(2), new RowValue(RowValue.INSERT, new Object[]{2, "bob"}));
        mt.put(List.of(3), new RowValue(RowValue.INSERT, new Object[]{3, "carol"}));

        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 0, schema,
                new ArrowPartFormat(), allocator, 10);
        long count = writer.writeFromMemTable(mt);
        assertEquals(3, count);

        SSTableReader reader = new SSTableReader(sstFile, schema,
                new ArrowPartFormat(), allocator);
        SSTable sst = reader.metadata();
        assertEquals(0, sst.level());
        assertEquals(3, sst.rowCount());
        // decodeKey 将 "1"/"3" 解析为 Integer
        assertEquals(List.of(1), sst.minKey());
        assertEquals(List.of(3), sst.maxKey());

        // 读回所有行
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = reader.scan();
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            for (int i = 0; i < batch.getRowCount(); i++) {
                rows.add(new Object[]{
                        batch.getVector(0).getObject(i),
                        batch.getVector(1).getObject(i).toString()});
            }
        }
        it.close();
        reader.close();

        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(2, rows.get(1)[0]);
        assertEquals("bob", rows.get(1)[1]);
        assertEquals(3, rows.get(2)[0]);
        assertEquals("carol", rows.get(2)[1]);
    }

    @Test
    void parquetFormatRoundTrip(@TempDir Path dir) throws Exception {
        // Parquet 格式的 SSTable 块编解码(SSTableWriter.writeToBytes /
        // SSTableReader 内存读)——默认存储格式即 parquet,是 LSM 的主路径。
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "alice"}));
        mt.put(List.of(2), new RowValue(RowValue.INSERT, new Object[]{2, "bob"}));

        Path sstFile = dir.resolve("sst-L0-000002.sst");
        ParquetPartFormat format = new ParquetPartFormat();
        SSTableWriter writer = new SSTableWriter(sstFile, 0, schema, format, allocator, 10);
        assertEquals(2, writer.writeFromMemTable(mt));

        SSTableReader reader = new SSTableReader(sstFile, schema, format, allocator);
        assertEquals(2, reader.metadata().rowCount());
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = reader.scan();
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            for (int i = 0; i < batch.getRowCount(); i++) {
                rows.add(new Object[]{
                        batch.getVector(0).getObject(i),
                        batch.getVector(1).getObject(i).toString()});
            }
        }
        it.close();
        reader.close();

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(2, rows.get(1)[0]);
        assertEquals("bob", rows.get(1)[1]);
    }

    @Test
    void bloomFilterRejectsMissingKey(@TempDir Path dir) throws Exception {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "a"}));

        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 0, schema,
                new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(mt);

        SSTableReader reader = new SSTableReader(sstFile, schema,
                new ArrowPartFormat(), allocator);
        SSTable sst = reader.metadata();
        // 整数 key 1 经 encodeKey 零填充为 "00000000000000000001"，bloom 用同一编码
        assertTrue(sst.bloom().mightContain(SSTableWriter.encodeKey(List.of(1))));
        assertFalse(sst.bloom().mightContain("999".getBytes()));
        reader.close();
    }

    @Test
    void keyRangeCheck(@TempDir Path dir) throws Exception {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(10), new RowValue(RowValue.INSERT, new Object[]{10, "a"}));
        mt.put(List.of(20), new RowValue(RowValue.INSERT, new Object[]{20, "b"}));

        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 0, schema,
                new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(mt);

        SSTableReader reader = new SSTableReader(sstFile, schema,
                new ArrowPartFormat(), allocator);
        SSTable sst = reader.metadata();
        // 重叠检测（decodeKey 将 "10"/"20" 解析为 Integer）
        assertTrue(sst.overlaps(List.of(5), List.of(15)));   // 部分重叠
        assertTrue(sst.overlaps(List.of(10), List.of(20)));  // 完全重叠
        assertFalse(sst.overlaps(List.of(1), List.of(5)));   // 完全不重叠(左)
        assertFalse(sst.overlaps(List.of(30), List.of(40))); // 完全不重叠(右)
        reader.close();
    }

    @Test
    void allColumnTypesRoundTrip(@TempDir Path dir) throws Exception {
        TableSchema multiSchema = new TableSchema("public", "mt",
                List.of(
                        new ColumnMeta("pk", ColumnType.INTEGER),
                        new ColumnMeta("c_smallint", ColumnType.SMALLINT),
                        new ColumnMeta("c_bigint", ColumnType.BIGINT),
                        new ColumnMeta("c_float", ColumnType.FLOAT),
                        new ColumnMeta("c_double", ColumnType.DOUBLE),
                        new ColumnMeta("c_decimal", ColumnType.DECIMAL, 10, 2, true),
                        new ColumnMeta("c_bool", ColumnType.BOOLEAN),
                        new ColumnMeta("c_date", ColumnType.DATE),
                        new ColumnMeta("c_time", ColumnType.TIME),
                        new ColumnMeta("c_ts", ColumnType.TIMESTAMP),
                        new ColumnMeta("c_varchar", ColumnType.VARCHAR),
                        new ColumnMeta("c_char", ColumnType.CHAR),
                        new ColumnMeta("c_binary", ColumnType.VARBINARY)),
                List.of("pk"), List.of(), List.of());

        MemTable mt = new MemTable(multiSchema, 1024 * 1024);
        byte[] binVal = new byte[]{0x01, 0x02, 0x03};
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[]{
                1, (short) 100, 9999999999L, 3.14f, 2.718281828,
                new BigDecimal("123.45"), true, 19740, // 19740 = 2024-01-17
                43200000, // 12:00:00 in millis
                1705478400000L, // 2024-01-17 12:00:00 UTC
                "hello", "x", binVal}));

        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 0, multiSchema,
                new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(mt);

        SSTableReader reader = new SSTableReader(sstFile, multiSchema,
                new ArrowPartFormat(), allocator);
        BatchIterator it = reader.scan();
        assertTrue(it.hasNext());
        VectorSchemaRoot batch = it.next();
        assertEquals(1, batch.getRowCount());

        assertEquals(1, batch.getVector(0).getObject(0));                    // pk
        assertEquals((short) 100, batch.getVector(1).getObject(0));          // c_smallint
        assertEquals(9999999999L, batch.getVector(2).getObject(0));          // c_bigint
        assertEquals(3.14f, (float) batch.getVector(3).getObject(0), 0.01f); // c_float
        assertEquals(2.718281828, (double) batch.getVector(4).getObject(0), 1e-9); // c_double
        assertEquals(new BigDecimal("123.45"), batch.getVector(5).getObject(0)); // c_decimal
        assertEquals(true, batch.getVector(6).getObject(0));                    // c_bool
        assertEquals(19740, batch.getVector(7).getObject(0));                // c_date
        // Arrow TimeMilliVector/TimeStampMilliVector.getObject() 返回 LocalTime/LocalDateTime
        assertNotNull(batch.getVector(8).getObject(0));                  // c_time
        assertNotNull(batch.getVector(9).getObject(0));                  // c_ts
        assertEquals("hello", textToString(batch.getVector(10).getObject(0))); // c_varchar
        assertEquals("x", textToString(batch.getVector(11).getObject(0)));   // c_char
        assertArrayEquals(binVal, (byte[]) batch.getVector(12).getObject(0)); // c_binary

        it.close();
        reader.close();
    }

    private static String textToString(Object val) {
        return val instanceof Text t ? t.toString() : val.toString();
    }
}