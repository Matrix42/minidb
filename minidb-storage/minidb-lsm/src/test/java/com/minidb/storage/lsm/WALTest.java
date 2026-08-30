package com.minidb.storage.lsm;

import com.minidb.storage.common.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WALTest {
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

    @Test
    void appendAndRecover(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, schema);
        wal.append(List.of(1), new RowValue(RowValue.INSERT, new Object[] {1, "a"}));
        wal.append(List.of(2), new RowValue(RowValue.INSERT, new Object[] {2, "b"}));
        wal.close();

        WAL wal2 = new WAL(walFile, schema);
        List<WAL.Entry> entries = wal2.recover();
        assertEquals(2, entries.size());
        assertEquals(1, entries.get(0).key().get(0)); // 二进制编码保类型:Integer 而非 String
        assertEquals(RowValue.INSERT, entries.get(0).value().kind());
        assertEquals("a", entries.get(0).value().values()[1]);
        wal2.close();
    }

    @Test
    void truncateClearsFile(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, schema);
        wal.append(List.of(1), new RowValue(RowValue.INSERT, new Object[] {1, "a"}));
        wal.truncate();
        wal.close();

        WAL wal2 = new WAL(walFile, schema);
        assertTrue(wal2.recover().isEmpty());
        wal2.close();
    }

    @Test
    void emptyFileRecoversEmpty(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        Files.createFile(walFile);
        WAL wal = new WAL(walFile, schema);
        assertTrue(wal.recover().isEmpty());
        wal.close();
    }

    @Test
    void deleteTombstone(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, schema);
        wal.append(List.of(1), new RowValue(RowValue.DELETE, null));
        wal.close();

        WAL wal2 = new WAL(walFile, schema);
        List<WAL.Entry> entries = wal2.recover();
        assertEquals(1, entries.size());
        assertEquals(RowValue.DELETE, entries.get(0).value().kind());
        assertNull(entries.get(0).value().values());
        wal2.close();
    }

    @Test
    void rotateSegmentsAndRecoverInOrder(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, schema);
        wal.append(List.of(1), new RowValue(RowValue.INSERT, new Object[] {1, "a"}));
        int gen0 = wal.rotate(); // wal.log → wal-0.log,新当前段
        wal.append(List.of(2), new RowValue(RowValue.INSERT, new Object[] {2, "b"}));
        int gen1 = wal.rotate(); // → wal-1.log
        wal.append(List.of(3), new RowValue(RowValue.INSERT, new Object[] {3, "c"}));
        wal.close();
        assertTrue(Files.exists(dir.resolve("wal-0.log")));
        assertTrue(Files.exists(dir.resolve("wal-1.log")));

        // 恢复:旧段按代号升序 + 当前段(数据全量、顺序正确)
        WAL wal2 = new WAL(walFile, schema);
        List<WAL.Entry> entries = wal2.recover();
        assertEquals(3, entries.size());
        assertEquals(1, entries.get(0).key().get(0));
        assertEquals(2, entries.get(1).key().get(0));
        assertEquals(3, entries.get(2).key().get(0));
        wal2.close();

        // drop 后不再恢复该段
        wal2.dropSegment(gen0);
        WAL wal3 = new WAL(walFile, schema);
        List<WAL.Entry> afterDrop = wal3.recover();
        assertEquals(2, afterDrop.size());
        assertEquals(2, afterDrop.get(0).key().get(0));
        assertEquals(3, afterDrop.get(1).key().get(0));
        wal3.close();
    }

    @Test
    void truncateAllRemovesSegments(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, schema);
        wal.append(List.of(1), new RowValue(RowValue.INSERT, new Object[] {1, "a"}));
        wal.rotate();
        wal.append(List.of(2), new RowValue(RowValue.INSERT, new Object[] {2, "b"}));
        wal.truncateAll();
        wal.close();

        assertTrue(Files.notExists(dir.resolve("wal-0.log")));
        WAL wal2 = new WAL(walFile, schema);
        assertTrue(wal2.recover().isEmpty());
        wal2.close();
    }
}
