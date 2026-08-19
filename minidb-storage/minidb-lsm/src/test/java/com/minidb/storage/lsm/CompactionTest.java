package com.minidb.storage.lsm;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;
import java.nio.file.Path;
import java.util.*;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactionTest {
    private final TableSchema schema = new TableSchema("public", "t",
            List.of(new ColumnMeta("id", ColumnType.INTEGER), new ColumnMeta("name", ColumnType.VARCHAR)),
            List.of("id"), List.of(), List.of());
    private final RootAllocator allocator = new RootAllocator();

    @Test
    void compactL0toL1(@TempDir Path dir) throws Exception {
        ArrowPartFormat fmt = new ArrowPartFormat();
        SSTableManager mgr = new SSTableManager();

        // 写 3 个 L0 SSTable（模拟多次 flush）
        for (int batch = 0; batch < 3; batch++) {
            MemTable mt = new MemTable(schema, 1024 * 1024);
            for (int i = batch * 10; i < (batch + 1) * 10; i++) {
                mt.put(List.of(String.valueOf(i)), new RowValue(RowValue.INSERT, new Object[]{i, "val" + i}));
            }
            long seq = mgr.nextSeq();
            Path file = dir.resolve("sst-L0-" + String.format("%06d", seq) + ".sst");
            SSTableWriter writer = new SSTableWriter(file, 0, schema, fmt, allocator, 10);
            writer.writeFromMemTable(mt);
            SSTableReader reader = new SSTableReader(file, schema, fmt, allocator);
            SSTable sst = reader.metadata();
            reader.close();
            mgr.addLevel0(new SSTable(file, 0, seq, sst.minKey(), sst.maxKey(),
                    sst.rowCount(), sst.bloom()));
        }

        assertEquals(3, mgr.levelFiles(0).size());

        // Compaction: L0 → L1
        Compaction compaction = new Compaction();
        compaction.compactLevel0To1(mgr, schema, fmt, allocator, dir, 64 * 1024);

        // L0 应该为空，L1 有文件
        assertEquals(0, mgr.levelFiles(0).size());
        assertTrue(mgr.levelFiles(1).size() >= 1);

        // 验证数据完整性：L1 的文件应该包含所有 30 行
        long totalRows = 0;
        for (SSTable sst : mgr.levelFiles(1)) {
            totalRows += sst.rowCount();
        }
        assertEquals(30, totalRows);
    }

    @Test
    void dedupAcrossLevels(@TempDir Path dir) throws Exception {
        ArrowPartFormat fmt = new ArrowPartFormat();
        SSTableManager mgr = new SSTableManager();

        // L0: key=1,2,3
        MemTable mt1 = new MemTable(schema, 1024 * 1024);
        mt1.put(List.of("1"), new RowValue(RowValue.INSERT, new Object[]{1, "v1"}));
        mt1.put(List.of("2"), new RowValue(RowValue.INSERT, new Object[]{2, "v2"}));
        mt1.put(List.of("3"), new RowValue(RowValue.INSERT, new Object[]{3, "v3"}));
        writeSST(mgr, 0, mt1, dir, fmt);

        // L0: key=2 更新
        MemTable mt2 = new MemTable(schema, 1024 * 1024);
        mt2.put(List.of("2"), new RowValue(RowValue.UPDATE, new Object[]{2, "v2-new"}));
        writeSST(mgr, 0, mt2, dir, fmt);

        assertEquals(2, mgr.levelFiles(0).size());

        Compaction compaction = new Compaction();
        compaction.compactLevel0To1(mgr, schema, fmt, allocator, dir, 64 * 1024);

        // 应该去重：只有 3 行，key=2 的值是 "v2-new"
        long totalRows = 0;
        for (SSTable sst : mgr.levelFiles(1)) {
            totalRows += sst.rowCount();
        }
        assertEquals(3, totalRows);
    }

    private void writeSST(SSTableManager mgr, int level, MemTable mt,
                           Path dir, PartFormat fmt) {
        long seq = mgr.nextSeq();
        Path file = dir.resolve("sst-L" + level + "-" + String.format("%06d", seq) + ".sst");
        SSTableWriter writer = new SSTableWriter(file, level, schema, fmt, allocator, 10);
        writer.writeFromMemTable(mt);
        SSTableReader reader = new SSTableReader(file, schema, fmt, allocator);
        SSTable sst = reader.metadata();
        reader.close();
        if (level == 0) {
            mgr.addLevel0(new SSTable(file, 0, seq, sst.minKey(), sst.maxKey(),
                    sst.rowCount(), sst.bloom()));
        } else {
            mgr.addLevelN(level, List.of(new SSTable(file, level, seq,
                    sst.minKey(), sst.maxKey(), sst.rowCount(), sst.bloom())));
        }
    }
}