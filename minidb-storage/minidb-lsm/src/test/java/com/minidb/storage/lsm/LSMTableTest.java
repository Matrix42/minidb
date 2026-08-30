package com.minidb.storage.lsm;

import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class LSMTableTest {
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
    void writeAndScan(@TempDir Path dir) throws Exception {
        LSMTable table =
                new LSMTable(
                        schema,
                        new ArrowPartFormat(),
                        allocator,
                        dir,
                        64 * 1024 * 1024); // 大 MemTable 避免 flush

        // INSERT
        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        ((org.apache.arrow.vector.IntVector) batch.getVector(0)).setSafe(0, 1);
        ((org.apache.arrow.vector.VarCharVector) batch.getVector(1)).setSafe(0, "alice".getBytes());
        ((org.apache.arrow.vector.IntVector) batch.getVector(0)).setSafe(1, 2);
        ((org.apache.arrow.vector.VarCharVector) batch.getVector(1)).setSafe(1, "bob".getBytes());
        batch.setRowCount(2);
        table.writePart(batch, TableHandle.Operation.INSERT);
        batch.close();

        // Scan
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = table.scan();
        while (it.hasNext()) {
            VectorSchemaRoot b = it.next();
            for (int i = 0; i < b.getRowCount(); i++) {
                rows.add(
                        new Object[] {
                            b.getVector(0).getObject(i), b.getVector(1).getObject(i).toString()
                        });
            }
        }
        it.close();

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(2, rows.get(1)[0]);
        assertEquals("bob", rows.get(1)[1]);

        table.close();
    }

    @Test
    void updateAndDelete(@TempDir Path dir) throws Exception {
        LSMTable table =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);

        // INSERT key=1
        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        ((org.apache.arrow.vector.IntVector) batch.getVector(0)).setSafe(0, 1);
        ((org.apache.arrow.vector.VarCharVector) batch.getVector(1)).setSafe(0, "old".getBytes());
        batch.setRowCount(1);
        table.writePart(batch, TableHandle.Operation.INSERT);
        batch.close();

        // UPDATE key=1
        VectorSchemaRoot batch2 = table.newBatchRoot();
        batch2.allocateNew();
        ((org.apache.arrow.vector.IntVector) batch2.getVector(0)).setSafe(0, 1);
        ((org.apache.arrow.vector.VarCharVector) batch2.getVector(1)).setSafe(0, "new".getBytes());
        batch2.setRowCount(1);
        table.writePart(batch2, TableHandle.Operation.UPDATE);
        batch2.close();

        // Scan should see "new"
        List<Object[]> rows = collect(table);
        assertEquals(1, rows.size());
        assertEquals("new", rows.get(0)[1]);

        // DELETE key=1
        VectorSchemaRoot batch3 = table.newBatchRoot();
        batch3.allocateNew();
        ((org.apache.arrow.vector.IntVector) batch3.getVector(0)).setSafe(0, 1);
        batch3.setRowCount(1);
        table.writePart(batch3, TableHandle.Operation.DELETE);
        batch3.close();

        // Scan should be empty
        rows = collect(table);
        assertTrue(rows.isEmpty());

        table.close();
    }

    @Test
    void rowCount(@TempDir Path dir) throws Exception {
        LSMTable table =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);

        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        for (int i = 0; i < 5; i++) {
            ((org.apache.arrow.vector.IntVector) batch.getVector(0)).setSafe(i, i);
            ((org.apache.arrow.vector.VarCharVector) batch.getVector(1))
                    .setSafe(i, ("v" + i).getBytes());
        }
        batch.setRowCount(5);
        table.writePart(batch, TableHandle.Operation.INSERT);
        batch.close();

        assertEquals(5, table.rowCount());
        table.close();
    }

    @Test
    void flushAndRecover(@TempDir Path dir) throws Exception {
        // 小 MemTable 阈值，强制 flush
        LSMTable table = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);

        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        for (int i = 0; i < 20; i++) {
            ((org.apache.arrow.vector.IntVector) batch.getVector(0)).setSafe(i, i);
            ((org.apache.arrow.vector.VarCharVector) batch.getVector(1))
                    .setSafe(i, ("v" + i).getBytes());
        }
        batch.setRowCount(20);
        table.writePart(batch, TableHandle.Operation.INSERT);
        batch.close();
        table.close();

        // 重新打开
        LSMTable table2 =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);
        List<Object[]> rows = collect(table2);
        assertEquals(20, rows.size());
        table2.close();
    }

    @Test
    void emptyProjectionPreservesRowCount(@TempDir Path dir) throws Exception {
        // 空投影(COUNT(*) 等不引用任何列时)由 projectColumns 包装为 0 列 root,
        // 行数必须保留,否则聚合算子读到 0 行。
        LSMTable table =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);

        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        for (int i = 0; i < 3; i++) {
            ((org.apache.arrow.vector.IntVector) batch.getVector(0)).setSafe(i, i);
            ((org.apache.arrow.vector.VarCharVector) batch.getVector(1))
                    .setSafe(i, ("v" + i).getBytes());
        }
        batch.setRowCount(3);
        table.writePart(batch, TableHandle.Operation.INSERT);
        batch.close();

        try (BatchIterator it = table.scan(new int[0])) {
            assertTrue(it.hasNext(), "空投影应产生至少一个批");
            VectorSchemaRoot b = it.next();
            assertEquals(0, b.getFieldVectors().size(), "空投影应输出 0 列");
            assertEquals(3, b.getRowCount(), "空投影批的行数应保留");
        }
        assertEquals(3, rowCountOf(table.scan(new int[0])), "空投影不应丢行");
        assertEquals(3, rowCountOf(table.scan(new int[] {0})), "单列投影不应丢行");

        table.close();
    }

    private static long rowCountOf(BatchIterator it) {
        long total = 0;
        try {
            while (it.hasNext()) {
                total += it.next().getRowCount();
            }
            return total;
        } finally {
            it.close();
        }
    }

    private List<Object[]> collect(LSMTable table) {
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = table.scan();
        while (it.hasNext()) {
            VectorSchemaRoot b = it.next();
            for (int i = 0; i < b.getRowCount(); i++) {
                Object[] row = new Object[b.getFieldVectors().size()];
                for (int c = 0; c < row.length; c++) {
                    Object val = b.getVector(c).getObject(i);
                    // VarCharVector.getObject() returns Text, not String
                    row[c] =
                            val instanceof org.apache.arrow.vector.util.Text t ? t.toString() : val;
                }
                rows.add(row);
            }
        }
        it.close();
        return rows;
    }
}
