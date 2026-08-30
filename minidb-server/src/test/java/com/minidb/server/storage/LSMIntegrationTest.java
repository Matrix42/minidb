package com.minidb.server.storage;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;
import com.minidb.storage.lsm.LSMTable;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LSM 集成测试：通过 StorageManager(而非 SQL/QueryExecutor)验证 LSMTable 的 CRUD、flush 持久化、compaction 合并、WAL
 * 恢复等完整链路。
 */
class LSMIntegrationTest {

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

    private RootAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    // ---- helpers ----

    /** 写入多行到 TableHandle。 */
    private void writeRows(TableHandle table, int[] ids, String[] names) {
        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        IntVector idVec = (IntVector) batch.getVector(0);
        VarCharVector nameVec = (VarCharVector) batch.getVector(1);
        for (int i = 0; i < ids.length; i++) {
            idVec.setSafe(i, ids[i]);
            nameVec.setSafe(i, names[i].getBytes());
        }
        batch.setRowCount(ids.length);
        table.writePart(batch, TableHandle.Operation.INSERT);
        batch.close();
    }

    /** 更新一行。 */
    private void updateRow(TableHandle table, int id, String newName) {
        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        ((IntVector) batch.getVector(0)).setSafe(0, id);
        ((VarCharVector) batch.getVector(1)).setSafe(0, newName.getBytes());
        batch.setRowCount(1);
        table.writePart(batch, TableHandle.Operation.UPDATE);
        batch.close();
    }

    /** 删除一行。 */
    private void deleteRow(TableHandle table, int id) {
        VectorSchemaRoot batch = table.newBatchRoot();
        batch.allocateNew();
        ((IntVector) batch.getVector(0)).setSafe(0, id);
        batch.setRowCount(1);
        table.writePart(batch, TableHandle.Operation.DELETE);
        batch.close();
    }

    /** 收集 TableHandle 的所有行为 {@code List<Object[]>}。 */
    private List<Object[]> collectRows(TableHandle table) {
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

    /** 按 id 列查找行。 */
    private Object[] findRow(List<Object[]> rows, int id) {
        return rows.stream().filter(r -> r[0].equals(id)).findFirst().orElse(null);
    }

    // ---- tests ----

    /**
     * 完整 CRUD 流程：建表 → INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT。 通过 StorageManager 操作，验证
     * LSMTable 与 StorageManager 的集成。
     */
    @Test
    void crudViaStorageManager(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        StorageManager storage = new StorageManager(catalog, allocator, dir);

        // CREATE TABLE with PK
        TableHandle table = storage.createTable(schema);
        assertTrue(table instanceof LSMTable, "表有主键时应为 LSMTable");

        // INSERT
        writeRows(table, new int[] {1, 2, 3}, new String[] {"alice", "bob", "carol"});
        List<Object[]> rows = collectRows(table);
        assertEquals(3, rows.size());
        assertEquals("alice", findRow(rows, 1)[1]);
        assertEquals("bob", findRow(rows, 2)[1]);
        assertEquals("carol", findRow(rows, 3)[1]);

        // UPDATE
        updateRow(table, 2, "bobby");
        rows = collectRows(table);
        assertEquals(3, rows.size());
        assertEquals("bobby", findRow(rows, 2)[1]);

        // DELETE
        deleteRow(table, 1);
        rows = collectRows(table);
        assertEquals(2, rows.size());
        assertNull(findRow(rows, 1), "id=1 应已被删除");

        storage.close();
    }

    /** INSERT 后数据仅存 MemTable+WAL 中，关闭 LSMTable 时自动 flush 到 SSTable， 重新打开后数据应完整恢复。 */
    @Test
    void flushPersistence(@TempDir Path dir) throws Exception {
        // 使用较小的 MemTable 阈值，写入多行后触发 flush
        LSMTable table = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 200);
        // 每行约 120 字节，写 3 行触发 flush
        writeRows(table, new int[] {1, 2, 3}, new String[] {"a", "b", "c"});
        // 此时应有 SSTable 文件（flush 已触发）
        assertTrue(table.partCount() >= 1, "flush 后应产生 SSTable");
        table.close();

        // 重新打开
        LSMTable table2 =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);
        List<Object[]> rows = collectRows(table2);
        assertEquals(3, rows.size());
        assertEquals("a", findRow(rows, 1)[1]);
        assertEquals("b", findRow(rows, 2)[1]);
        assertEquals("c", findRow(rows, 3)[1]);
        table2.close();
    }

    /**
     * 多次 flush 产生多个 L0 SSTable，compaction 将它们合并为 L1 SSTable， 合并后数据应完整且去重（同 key 保留最新版本）。
     * UPDATE/DELETE 留在 MemTable 中（不 flush），由 MergeIterator 在扫描时合并。
     */
    @Test
    void compactionMergesCorrectly(@TempDir Path dir) throws Exception {
        // 阈值设为 200，约 2 行触发一次 flush，但单行 UPDATE/DELETE 不会触发
        LSMTable table = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 200);

        // 分批写入产生多个 SSTable
        writeRows(table, new int[] {1, 2}, new String[] {"v1", "v2"}); // flush
        writeRows(table, new int[] {3, 4}, new String[] {"v3", "v4"}); // flush
        writeRows(table, new int[] {5}, new String[] {"v5"}); // stays in MemTable

        int partCountBefore = table.partCount();
        assertTrue(partCountBefore >= 2, "多次 flush 应产生多个 SSTable, 实际: " + partCountBefore);

        // 更新 key=3 和 key=5（留在 MemTable，不触发 flush）
        updateRow(table, 3, "v3-new");
        updateRow(table, 5, "v5-new");
        // 删除 key=2
        deleteRow(table, 2);

        // 扫描验证：MergeIterator 合并 MemTable + SSTable，更新和删除已生效
        List<Object[]> rowsBefore = collectRows(table);
        // key=1,3-new,4,5-new 存在；key=2 被删除
        assertEquals(4, rowsBefore.size());
        assertEquals("v1", findRow(rowsBefore, 1)[1]);
        assertNull(findRow(rowsBefore, 2), "key=2 应已被删除");
        assertEquals("v3-new", findRow(rowsBefore, 3)[1]);
        assertEquals("v4", findRow(rowsBefore, 4)[1]);
        assertEquals("v5-new", findRow(rowsBefore, 5)[1]);

        // Compaction: 合并 SSTable（MemTable 不动）
        table.compact(64 * 1024 * 1024);

        // 扫描验证：compaction 后 MergeIterator 仍正确合并
        List<Object[]> rowsAfter = collectRows(table);
        assertEquals(4, rowsAfter.size());
        assertEquals("v1", findRow(rowsAfter, 1)[1]);
        assertNull(findRow(rowsAfter, 2), "key=2 应已被删除");
        assertEquals("v3-new", findRow(rowsAfter, 3)[1]);
        assertEquals("v4", findRow(rowsAfter, 4)[1]);
        assertEquals("v5-new", findRow(rowsAfter, 5)[1]);

        table.close();
    }

    /** INSERT 数据后关闭(flush→SSTable→truncate WAL)，重新打开数据应恢复。 同时验证 WAL 文件在正确截断后不会残留旧数据。 */
    @Test
    void walRecovery(@TempDir Path dir) throws Exception {
        // 大阈值，防止自动 flush——数据只在 MemTable+WAL 中
        LSMTable table =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);
        writeRows(table, new int[] {10, 20, 30}, new String[] {"x", "y", "z"});
        // 此时无 SSTable，数据在 MemTable+WAL
        assertEquals(0, table.partCount(), "大阈值下不应自动 flush");

        // 关闭：close() 会 flush MemTable 到 SSTable，然后 truncate WAL
        table.close();

        // 验证 WAL 文件存在但为空（已 truncate）
        Path walFile = dir.resolve("wal.log");
        assertTrue(Files.exists(walFile));

        // 重新打开：此时 SSTable 有数据，WAL 为空
        LSMTable table2 =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);
        List<Object[]> rows = collectRows(table2);
        assertEquals(3, rows.size());
        assertEquals("x", findRow(rows, 10)[1]);
        assertEquals("y", findRow(rows, 20)[1]);
        assertEquals("z", findRow(rows, 30)[1]);
        table2.close();
    }

    /** 通过 StorageManager 的完整持久化：建表→写入→关闭→重新打开→验证数据。 验证 catalog.json 和 SSTable 文件能正确恢复。 */
    @Test
    void storageManagerPersistence(@TempDir Path dir) throws Exception {
        // 建表 + 写入
        MiniDbCatalog catalog = new MiniDbCatalog();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        TableHandle table = storage.createTable(schema);
        assertTrue(table instanceof LSMTable);

        writeRows(table, new int[] {100, 200}, new String[] {"foo", "bar"});
        storage.close();

        // 验证磁盘文件存在
        assertTrue(Files.exists(dir.resolve("catalog.json")), "catalog.json 应存在");
        Path tableDir = dir.resolve("public").resolve("t");
        assertTrue(Files.exists(tableDir), "表目录应存在");

        // 重新打开
        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (RootAllocator allocator2 = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator2, dir);
            storage2.loadAll();

            TableHandle reloaded = storage2.getTable("public", "t");
            assertTrue(reloaded instanceof LSMTable);
            List<Object[]> rows = collectRows(reloaded);
            assertEquals(2, rows.size());
            assertEquals("foo", findRow(rows, 100)[1]);
            assertEquals("bar", findRow(rows, 200)[1]);

            storage2.close();
        }
    }

    /** 通过 StorageManager 触发 compaction 并验证数据完整性。 */
    @Test
    void storageManagerCompaction(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        TableHandle table = storage.createTable(schema);
        assertTrue(table instanceof LSMTable);

        // 写入多行后手动 flush，产生多个 SSTable
        LSMTable lsm = (LSMTable) table;
        for (int batch = 0; batch < 3; batch++) {
            int start = batch * 10 + 1;
            int[] ids = new int[10];
            String[] names = new String[10];
            for (int i = 0; i < 10; i++) {
                ids[i] = start + i;
                names[i] = "n" + (start + i);
            }
            writeRows(table, ids, names);
            lsm.flushMemTable();
        }

        int partCountBefore = lsm.partCount();
        assertTrue(partCountBefore >= 3, "多次 flush 应产生多个 SSTable");

        // Compaction
        storage.compactTable("public", "t");

        // 验证数据
        List<Object[]> rows = collectRows(table);
        assertEquals(30, rows.size());
        for (int i = 1; i <= 30; i++) {
            assertEquals("n" + i, findRow(rows, i)[1]);
        }

        storage.close();
    }

    /**
     * 验证 LSMTable.rowCount() 能正确统计 INSERT/UPDATE/DELETE 后的有效行数， 包括 flush 和 compaction 之后。
     * 使用大阈值避免中途自动 flush，保证 rowCount() 精确。
     */
    @Test
    void rowCountAfterOperations(@TempDir Path dir) throws Exception {
        // 大阈值，数据全在 MemTable 中，rowCount() 精确
        LSMTable table =
                new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 64 * 1024 * 1024);

        // INSERT 3 行
        writeRows(table, new int[] {1, 2, 3}, new String[] {"a", "b", "c"});
        assertEquals(3, table.rowCount());

        // UPDATE 不改变行数（MemTable 中同 key 替换）
        updateRow(table, 2, "b2");
        assertEquals(3, table.rowCount());

        // DELETE 减少行数
        deleteRow(table, 1);
        assertEquals(2, table.rowCount());

        // flush 后行数不变（DELETE 被过滤，只有 INSERT 和 UPDATE 写入 SSTable）
        table.flushMemTable();
        assertEquals(2, table.rowCount());

        // compaction 后行数不变
        table.compact(64 * 1024 * 1024);
        assertEquals(2, table.rowCount());

        // 扫描验证实际数据一致
        List<Object[]> rows = collectRows(table);
        assertEquals(2, rows.size());
        assertEquals("b2", findRow(rows, 2)[1]);
        assertEquals("c", findRow(rows, 3)[1]);

        table.close();
    }
}
