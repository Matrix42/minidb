package com.minidb.storage.lsm;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 双缓冲 flush(A6):写路径满表 swap 出、后台异步落盘。验证:
 * ① swap 后未 flush 时读路径(scan/getByKey/rowCount)覆盖待落盘表——否则丢数据;
 * ② flushNextPending 落盘 + 删 WAL 段后读路径仍正确;
 * ③ TRUNCATE 作废挂起的 flush;
 * ④ 真实后台执行器端到端;
 * ⑤ 重启恢复 WAL 多段(swap 未 flush 即 crash)。
 */
class LSMTableAsyncFlushTest {
    private final TableSchema schema = new TableSchema("public", "t",
            List.of(new ColumnMeta("id", ColumnType.INTEGER), new ColumnMeta("name", ColumnType.VARCHAR)),
            List.of("id"), List.of(), List.of());
    private final RootAllocator allocator = new RootAllocator();

    /** 不执行的假执行器:swap 后 flush 时机由测试手动控制。 */
    private static final class NoOpFlushExecutor extends LSMBackgroundExecutor {
        NoOpFlushExecutor() {
            super(4, 1L << 40, 1000);
        }

        @Override
        public void flushAsync(LSMTable table) {
            // 故意不执行,让 pending 堆积
        }
    }

    @Test
    void swappedTablesVisibleToReadPath(@TempDir Path dir) throws Exception {
        LSMTable table = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
        table.setFlushExecutor(new NoOpFlushExecutor());
        writeRows(table, 1, 3);
        writeRows(table, 10, 3);
        assertTrue(table.pendingFlushCount() > 0, "写满后应 swap 出待落盘表");
        assertTrue(Files.exists(dir.resolve("wal-0.log")), "swap 后 WAL 段应保留(未 flush)");

        // 读路径必须覆盖待落盘表(数据未落盘仍可见)
        assertEquals(6, table.rowCount());
        assertEquals("v10", table.getByKey(List.of(10)).values()[1].toString());
        assertNull(table.getByKey(List.of(999)));
        assertEquals(6, rows(table).size());
        table.close();
    }

    @Test
    void flushNextPendingPersistsAndDropsSegments(@TempDir Path dir) throws Exception {
        LSMTable table = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
        table.setFlushExecutor(new NoOpFlushExecutor());
        writeRows(table, 1, 3);
        writeRows(table, 10, 3);
        table.flushNextPending();

        assertEquals(0, table.pendingFlushCount());
        assertTrue(table.partCount() > 0, "flush 后应有 SSTable 落盘");
        assertTrue(Files.notExists(dir.resolve("wal-0.log")), "flush 后对应 WAL 段应删除");
        assertEquals(6, table.rowCount());
        assertEquals(6, rows(table).size());
        assertEquals("v12", table.getByKey(List.of(12)).values()[1].toString());
        table.close();
    }

    @Test
    void clearPartsAbandonsPendingFlush(@TempDir Path dir) throws Exception {
        LSMTable table = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
        table.setFlushExecutor(new NoOpFlushExecutor());
        writeRows(table, 1, 3);
        table.clearParts();
        assertEquals(0, table.pendingFlushCount());

        // 作废的 flush 任务不应复活已清空的数据
        table.flushNextPending();
        assertEquals(0, table.partCount(), "作废 flush 不得产生 SSTable");
        assertTrue(rows(table).isEmpty());
        table.close();
    }

    @Test
    void asyncFlushEndToEnd(@TempDir Path dir) throws Exception {
        try (LSMBackgroundExecutor ex = new LSMBackgroundExecutor(4, 1L << 40, 1000)) {
            ex.start();
            LSMTable table = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
            table.setFlushExecutor(ex);
            writeRows(table, 1, 3);
            writeRows(table, 10, 3);

            // 等后台 flush 完成(写路径不阻塞,数据由后台线程落盘)
            long deadline = System.currentTimeMillis() + 5000;
            while (table.pendingFlushCount() > 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(0, table.pendingFlushCount(), "后台 flush 应在超时前完成");
            assertEquals(6, table.rowCount());
            assertEquals(6, rows(table).size());
            assertEquals("v10", table.getByKey(List.of(10)).values()[1].toString());
            table.close();
        }
    }

    @Test
    void restartRecoversMultipleWalSegments(@TempDir Path dir) throws Exception {
        LSMTable table1 = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
        table1.setFlushExecutor(new NoOpFlushExecutor());
        writeRows(table1, 1, 3);
        writeRows(table1, 10, 3);
        // 模拟 crash:swap 未 flush 即关闭进程(不 close,旧 channel 未释放)

        LSMTable table2 = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
        assertEquals(6, table2.rowCount(), "恢复应重放所有 WAL 段");
        assertEquals("v10", table2.getByKey(List.of(10)).values()[1].toString());
        assertEquals(6, rows(table2).size());
        table2.close();
    }

    private void writeRows(LSMTable table, int start, int n) {
        VectorSchemaRoot root = table.newBatchRoot();
        root.allocateNew();
        root.setRowCount(n);
        for (int i = 0; i < n; i++) {
            int id = start + i;
            ((org.apache.arrow.vector.IntVector) root.getVector(0)).setSafe(i, id);
            ((org.apache.arrow.vector.VarCharVector) root.getVector(1)).setSafe(i, ("v" + id).getBytes());
        }
        table.writePart(root, TableHandle.Operation.INSERT);
        root.close();
    }

    private List<Object[]> rows(LSMTable table) {
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = table.scan();
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            for (int i = 0; i < batch.getRowCount(); i++) {
                Object[] row = new Object[batch.getFieldVectors().size()];
                for (int c = 0; c < row.length; c++) {
                    Object val = batch.getVector(c).getObject(i);
                    if (val instanceof org.apache.arrow.vector.util.Text) {
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
