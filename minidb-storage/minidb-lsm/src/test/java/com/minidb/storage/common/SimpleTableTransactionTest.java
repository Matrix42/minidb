package com.minidb.storage.common;

import com.minidb.storage.arrow.ArrowPartFormat;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SimpleTable 事务语义(置于 lsm 模块内,因它依赖 minidb-arrow 提供 ArrowPartFormat):
 * ① 事务内读自己的写入(增量 INSERT 与整表 rewrite 快照);其他事务仍读 base(隔离);
 * ② 未提交 rewrite 回滚后 base 不变(DELETE/UPDATE 不再让其他事务看到空表)。
 */
class SimpleTableTransactionTest {
    private final TableSchema schema = new TableSchema("public", "t",
            List.of(new ColumnMeta("id", ColumnType.INTEGER),
                    new ColumnMeta("v", ColumnType.INTEGER)),
            List.of("id"), List.of(), List.of());
    private final RootAllocator allocator = new RootAllocator();

    private static void insert(SimpleTable t, int... vs) {
        VectorSchemaRoot root = t.newBatchRoot();
        root.allocateNew();
        for (int i = 0; i < vs.length; i++) {
            ((org.apache.arrow.vector.IntVector) root.getVector(0)).setSafe(i, i);
            ((org.apache.arrow.vector.IntVector) root.getVector(1)).setSafe(i, vs[i]);
        }
        root.setRowCount(vs.length);
        t.writePart(root, TableHandle.Operation.INSERT);
        root.close();
    }

    private static long rowCount(BatchIterator it) {
        long n = 0;
        while (it.hasNext()) {
            n += it.next().getRowCount();
        }
        it.close();
        return n;
    }

    @Test
    void transactionReadsOwnInsertAndOthersStayIsolated(@TempDir Path dir) throws Exception {
        SimpleTable t = new SimpleTable(schema, allocator, dir, new ArrowPartFormat());
        insert(t, 1, 2); // committed base: 2 行

        long tx = 5;
        // 事务内增量 INSERT
        VectorSchemaRoot root = t.newBatchRoot();
        root.allocateNew();
        ((org.apache.arrow.vector.IntVector) root.getVector(0)).setSafe(0, 100);
        ((org.apache.arrow.vector.IntVector) root.getVector(1)).setSafe(0, 300);
        root.setRowCount(1);
        t.writePart(root, TableHandle.Operation.INSERT, tx);
        root.close();

        // 自己可见(2+1),其他事务仍只读 base(2)
        assertEquals(3, rowCount(t.scan(0, tx)), "事务内必须读到自己的 INSERT");
        assertEquals(2, rowCount(t.scan(0, 999)), "其他事务隔离:不应看到未提交 INSERT");
        t.close();
    }

    @Test
    void concurrentTransactionsDoNotSeeUncommittedRewrite(@TempDir Path dir) throws Exception {
        SimpleTable t = new SimpleTable(schema, allocator, dir, new ArrowPartFormat());
        insert(t, 1, 2, 3); // 3 行: id=0..2

        long txA = 10;
        // 事务 A 做删除 id=0 的 rewrite:读 base 删一行后写新快照。
        List<VectorSchemaRoot> kept = new ArrayList<>();
        try (BatchIterator it = t.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                VectorSchemaRoot nb = t.newBatchRoot();
                nb.allocateNew();
                int k = 0;
                for (int r = 0; r < b.getRowCount(); r++) {
                    if (((org.apache.arrow.vector.IntVector) b.getVector(0)).get(r) == 0) {
                        continue; // 删 id=0
                    }
                    for (int c = 0; c < b.getFieldVectors().size(); c++) {
                        nb.getVector(c).copyFromSafe(r, k, b.getVector(c));
                    }
                    k++;
                }
                if (k > 0) {
                    nb.setRowCount(k);
                    kept.add(nb);
                } else {
                    nb.close();
                }
            }
        }
        t.markRewrite(txA);
        for (VectorSchemaRoot nb : kept) {
            t.writePart(nb, TableHandle.Operation.INSERT, txA);
            nb.close();
        }

        // 事务 A 自己看到 2 行;事务 B 仍看到 3 行(隔离)
        assertEquals(2, rowCount(t.scan(0, txA)), "rewrite 事务应读自己的新快照");
        assertEquals(3, rowCount(t.scan(0, 999)), "其他事务不得看到未提交的 rewrite(空表 bug)");

        // 回滚后 base 恢复 3 行
        t.rollbackTx(txA);
        assertEquals(3, rowCount(t.scan()), "回滚后 base 不变");
        t.close();
    }
}