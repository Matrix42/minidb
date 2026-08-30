package com.minidb.storage.lsm;

import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 崩溃恢复原子性:未提交事务的数据在重启后必须被丢弃,绝不能因无参 recover() 提前 flush 而被永久固化进 SSTable(bug #4)。 */
class TransactionRecoveryTest {
    private final TableSchema schema =
            new TableSchema(
                    "public",
                    "t",
                    java.util.List.of(
                            new ColumnMeta("id", ColumnType.INTEGER),
                            new ColumnMeta("name", ColumnType.VARCHAR)),
                    java.util.List.of("id"),
                    java.util.List.of(),
                    java.util.List.of());
    private final RootAllocator allocator = new RootAllocator();

    @Test
    void uncommittedTxDataNotFlushedToSstableOnRecovery(@TempDir Path dir) throws Exception {
        long uncommittedTxId = 7L;
        // 小阈值:保证一行就触发 needsFlush,暴露「重放全量 WAL 后误 flush」的缺陷。
        LSMTable table1 = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
        VectorSchemaRoot root = table1.newBatchRoot();
        root.allocateNew();
        ((IntVector) root.getVector(0)).setSafe(0, 1);
        ((VarCharVector) root.getVector(1)).setSafe(0, "uncommitted".getBytes());
        root.setRowCount(1);
        table1.writePart(root, TableHandle.Operation.INSERT, uncommittedTxId);
        root.close();
        // 模拟 crash:事务未提交,进程直接退出(不 close table1)。

        // 重启:构造器先无参 recover(),再由 StorageManager.loadAll 调 recover(committedTxIds)。
        LSMTable table2 = new LSMTable(schema, new ArrowPartFormat(), allocator, dir, 100);
        // txId=7 未提交,不在 committedTxIds 里 → 数据必须被丢弃。
        table2.recover(Set.of());
        assertEquals(0, table2.rowCount(), "未提交事务的数据不得在崩溃恢复后可见(原子性)");
        table2.close();
    }
}
