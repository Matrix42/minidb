package com.minidb.storage.lsm;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.storage.common.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WALTest {
    private final TableSchema schema = new TableSchema("public", "t",
            List.of(new ColumnMeta("id", ColumnType.INTEGER), new ColumnMeta("name", ColumnType.VARCHAR)),
            List.of("id"), List.of(), List.of());

    @Test
    void appendAndRecover(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, schema);
        wal.append(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "a"}));
        wal.append(List.of(2), new RowValue(RowValue.INSERT, new Object[]{2, "b"}));
        wal.close();

        WAL wal2 = new WAL(walFile, schema);
        List<WAL.Entry> entries = wal2.recover();
        assertEquals(2, entries.size());
        assertEquals("1", entries.get(0).key().get(0));
        assertEquals(RowValue.INSERT, entries.get(0).value().kind());
        assertEquals("a", entries.get(0).value().values()[1]);
        wal2.close();
    }

    @Test
    void truncateClearsFile(@TempDir Path dir) throws Exception {
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, schema);
        wal.append(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "a"}));
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
}