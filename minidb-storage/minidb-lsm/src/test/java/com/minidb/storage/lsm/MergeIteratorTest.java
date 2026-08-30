package com.minidb.storage.lsm;

import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MergeIteratorTest {
    private final TableSchema schema =
            new TableSchema(
                    "public",
                    "t",
                    List.of(
                            new ColumnMeta("id", ColumnType.INTEGER),
                            new ColumnMeta("name", ColumnType.VARCHAR)),
                    List.of("id"),
                    List.of(),
                    List.of());
    private final RootAllocator allocator = new RootAllocator();

    @Test
    void memTableOnly(@TempDir Path dir) {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[] {1, "a"}));
        mt.put(List.of(2), new RowValue(RowValue.INSERT, new Object[] {2, "b"}));

        SSTableManager mgr = new SSTableManager();
        MergeIterator mi =
                new MergeIterator(List.of(mt), mgr, schema, new ArrowPartFormat(), allocator);
        List<Object[]> rows = collect(mi);
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals(2, rows.get(1)[0]);
    }

    @Test
    void memTableOverridesSSTable(@TempDir Path dir) throws Exception {
        // 先写一个 SSTable (key=1: "old")
        MemTable oldMt = new MemTable(schema, 1024 * 1024);
        oldMt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[] {1, "old"}));
        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer =
                new SSTableWriter(sstFile, 0, schema, new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(oldMt);

        SSTableManager mgr = new SSTableManager();
        SSTableReader reader = new SSTableReader(sstFile, schema, new ArrowPartFormat(), allocator);
        SSTable sst = reader.metadata();
        reader.close();
        mgr.addLevel0(
                new SSTable(
                        sstFile,
                        0,
                        sst.seq(),
                        sst.minKey(),
                        sst.maxKey(),
                        sst.rowCount(),
                        sst.bloom()));

        // MemTable 更新 key=1
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(1), new RowValue(RowValue.UPDATE, new Object[] {1, "new"}));

        MergeIterator mi =
                new MergeIterator(List.of(mt), mgr, schema, new ArrowPartFormat(), allocator);
        List<Object[]> rows = collect(mi);
        assertEquals(1, rows.size());
        assertEquals("new", rows.get(0)[1]);
    }

    @Test
    void deleteTombstoneRemovesRow(@TempDir Path dir) throws Exception {
        // SSTable 有 key=1
        MemTable oldMt = new MemTable(schema, 1024 * 1024);
        oldMt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[] {1, "a"}));
        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer =
                new SSTableWriter(sstFile, 0, schema, new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(oldMt);

        SSTableManager mgr = new SSTableManager();
        SSTableReader reader = new SSTableReader(sstFile, schema, new ArrowPartFormat(), allocator);
        SSTable sst = reader.metadata();
        reader.close();
        mgr.addLevel0(
                new SSTable(
                        sstFile,
                        0,
                        sst.seq(),
                        sst.minKey(),
                        sst.maxKey(),
                        sst.rowCount(),
                        sst.bloom()));

        // MemTable 删除 key=1
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(1), new RowValue(RowValue.DELETE, null));

        MergeIterator mi =
                new MergeIterator(List.of(mt), mgr, schema, new ArrowPartFormat(), allocator);
        List<Object[]> rows = collect(mi);
        assertTrue(rows.isEmpty());
    }

    @Test
    void multiBatchSstStreamsAcrossBlocks(@TempDir Path dir) throws Exception {
        // 10000 行超过单 block 容量 → SstSource 流式换批;验证换批后 key 槽复用
        // (lastKey 拷贝)与 values 数组独立性,以及跨批的 key 覆盖仍正确。
        MemTable oldMt = new MemTable(schema, 1024 * 1024);
        int n = 10_000;
        for (int i = 0; i < n; i++) {
            oldMt.put(List.of(i), new RowValue(RowValue.INSERT, new Object[] {i, "v" + i}));
        }
        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer =
                new SSTableWriter(sstFile, 0, schema, new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(oldMt);

        SSTableManager mgr = new SSTableManager();
        SSTableReader reader = new SSTableReader(sstFile, schema, new ArrowPartFormat(), allocator);
        SSTable sst = reader.metadata();
        reader.close();
        mgr.addLevel0(
                new SSTable(
                        sstFile,
                        0,
                        sst.seq(),
                        sst.minKey(),
                        sst.maxKey(),
                        sst.rowCount(),
                        sst.bloom()));

        // MemTable 覆盖文件中间某 key,验证流式合并跨批正确
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(5000), new RowValue(RowValue.UPDATE, new Object[] {5000, "overridden"}));

        MergeIterator mi =
                new MergeIterator(List.of(mt), mgr, schema, new ArrowPartFormat(), allocator);
        List<Object[]> rows = collect(mi);
        assertEquals(n, rows.size());
        assertEquals("v0", rows.get(0)[1]);
        assertEquals("overridden", rows.get(5000)[1], "跨批的 key 覆盖应正确");
        assertEquals("v9999", rows.get(n - 1)[1]);
    }

    private List<Object[]> collect(MergeIterator mi) {
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = mi.scan();
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            for (int i = 0; i < batch.getRowCount(); i++) {
                Object[] row = new Object[batch.getFieldVectors().size()];
                for (int c = 0; c < row.length; c++) {
                    Object val = batch.getVector(c).getObject(i);
                    // Arrow VarCharVector.getObject 返回 Text，转为 String
                    if (val instanceof Text) {
                        val = val.toString();
                    }
                    row[c] = val;
                }
                rows.add(row);
            }
        }
        it.close();
        return rows;
    }
}
