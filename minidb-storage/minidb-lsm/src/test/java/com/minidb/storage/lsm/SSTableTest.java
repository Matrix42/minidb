package com.minidb.storage.lsm;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;
import java.nio.file.Path;
import java.util.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableTest {
    private final TableSchema schema = new TableSchema("public", "t",
            List.of(new ColumnMeta("id", ColumnType.INTEGER), new ColumnMeta("name", ColumnType.VARCHAR)),
            List.of("id"), List.of(), List.of());
    private final RootAllocator allocator = new RootAllocator();

    @Test
    void writeAndReadRoundTrip(@TempDir Path dir) throws Exception {
        // 准备有序的 MemTable 数据（MemTable key 是 String 类型）
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of("1"), new RowValue(RowValue.INSERT, new Object[]{1, "alice"}));
        mt.put(List.of("2"), new RowValue(RowValue.INSERT, new Object[]{2, "bob"}));
        mt.put(List.of("3"), new RowValue(RowValue.INSERT, new Object[]{3, "carol"}));

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
    void bloomFilterRejectsMissingKey(@TempDir Path dir) throws Exception {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of("1"), new RowValue(RowValue.INSERT, new Object[]{1, "a"}));

        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 0, schema,
                new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(mt);

        SSTableReader reader = new SSTableReader(sstFile, schema,
                new ArrowPartFormat(), allocator);
        SSTable sst = reader.metadata();
        // key 编码后 "1"→"00000000000000000001",bloom 用 encodeKey 的字节
        assertTrue(sst.bloom().mightContain(SSTableWriter.encodeKey(List.of("1"))));
        assertFalse(sst.bloom().mightContain("999".getBytes()));
        reader.close();
    }

    @Test
    void keyRangeCheck(@TempDir Path dir) throws Exception {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of("10"), new RowValue(RowValue.INSERT, new Object[]{10, "a"}));
        mt.put(List.of("20"), new RowValue(RowValue.INSERT, new Object[]{20, "b"}));

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
}