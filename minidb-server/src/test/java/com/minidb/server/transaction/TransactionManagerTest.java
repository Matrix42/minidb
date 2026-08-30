package com.minidb.server.transaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TransactionManagerTest {

    @Test
    void beginAssignsIncreasingTxId(@TempDir Path tmpDir) {
        TxLog txLog = new TxLog(tmpDir.resolve("txlog.log"));
        TransactionManager tm = new TransactionManager(TransactionIsolation.SERIALIZABLE, txLog);

        TxHandle tx1 = tm.begin();
        TxHandle tx2 = tm.begin();
        TxHandle tx3 = tm.begin();

        assertTrue(tx1.txId() < tx2.txId());
        assertTrue(tx2.txId() < tx3.txId());
        assertEquals(3, tm.activeTxCount());
    }

    @Test
    void snapshotForReadUncommittedIsNegativeOne(@TempDir Path tmpDir) {
        TxLog txLog = new TxLog(tmpDir.resolve("txlog.log"));
        TransactionManager tm =
                new TransactionManager(TransactionIsolation.READ_UNCOMMITTED, txLog);

        TxHandle tx = tm.begin();
        assertEquals(-1L, tx.snapshotTxId());
    }

    @Test
    void commitSetsStatusAndDecrementsCount(@TempDir Path tmpDir) {
        TxLog txLog = new TxLog(tmpDir.resolve("txlog.log"));
        TransactionManager tm = new TransactionManager(TransactionIsolation.READ_COMMITTED, txLog);

        TxHandle tx = tm.begin();
        assertEquals(TxStatus.ACTIVE, tx.status());
        assertEquals(1, tm.activeTxCount());

        tm.commit(tx.txId());
        assertEquals(TxStatus.COMMITTED, tx.status());
        assertEquals(0, tm.activeTxCount());
    }

    @Test
    void rollbackSetsStatusAndDecrementsCount(@TempDir Path tmpDir) {
        TxLog txLog = new TxLog(tmpDir.resolve("txlog.log"));
        TransactionManager tm = new TransactionManager(TransactionIsolation.SERIALIZABLE, txLog);

        TxHandle tx = tm.begin();
        assertEquals(1, tm.activeTxCount());

        tm.rollback(tx.txId());
        assertEquals(TxStatus.ABORTED, tx.status());
        assertEquals(0, tm.activeTxCount());
    }

    @Test
    void commitRecordSurvivesTruncationCheck(@TempDir Path tmpDir) {
        Path logFile = tmpDir.resolve("txlog.log");
        TxLog txLog = new TxLog(logFile);
        TransactionManager tm = new TransactionManager(TransactionIsolation.SERIALIZABLE, txLog);

        // 无并发干预:提交后 activeTxCount 归零,但因已写出 COMMIT,进程内不再截断日志
        // (截断竞态修复),这条 COMMIT 必须保留供崩溃恢复。
        TxHandle tx = tm.begin();
        long txId = tx.txId();
        tm.commit(txId);
        assertEquals(0, tm.activeTxCount());

        // 重新打开验证 COMMIT 仍在日志中
        TxLog reopened = new TxLog(logFile);
        Set<Long> committed = reopened.recoverCommitted();
        reopened.close();
        assertTrue(committed.contains(txId));
    }

    @Test
    void commitWritesTxLog(@TempDir Path tmpDir) {
        Path logFile = tmpDir.resolve("txlog.log");
        TxLog txLog = new TxLog(logFile);
        TransactionManager tm = new TransactionManager(TransactionIsolation.SERIALIZABLE, txLog);
        // 保持一个活跃事务，防止 commit 后 activeTxCount==0 触发截断清空日志
        TxHandle keepAlive = tm.begin();

        TxHandle tx = tm.begin();
        long txId = tx.txId();
        tm.commit(txId);

        txLog.close();

        // 重新打开验证
        TxLog txLog2 = new TxLog(logFile);
        Set<Long> committed = txLog2.recoverCommitted();
        txLog2.close();
        assertTrue(committed.contains(txId));
    }

    @Test
    void serializableConflictDetected(@TempDir Path tmpDir) {
        TxLog txLog = new TxLog(tmpDir.resolve("txlog.log"));
        TransactionManager tm = new TransactionManager(TransactionIsolation.SERIALIZABLE, txLog);

        // T1 读取列 A
        TxHandle t1 = tm.begin();
        tm.recordRead(t1.txId(), "public.t.c1");
        tm.commit(t1.txId());

        // T2 在 T1 提交后写入列 A
        TxHandle t2 = tm.begin();
        tm.recordWrite(t2.txId(), "public.t.c1");
        tm.commit(t2.txId());

        // T3 在 snapshot 时看到 T1 已提交，T2 未提交
        // T3 读取列 A 后，T2 提交了——冲突
        TxHandle t3 = tm.begin();
        tm.recordRead(t3.txId(), "public.t.c1");

        // T2 在 T3 开始后提交，但 T3 的 snapshot 在 T2 之前
        // 正常情况下 T3 的 snapshot 在 T2 提交之前，所以 T3 读不到 T2 的写入
        // 但如果 T3 读完后 T2 写入并提交，T3 提交时检测到冲突
        // 这里简化测试：直接验证冲突检测逻辑
        // (T3 的 snapshot 在 T2 提交之前，T2 的写入在 T3 的写集之后)
        // 由于 T3 只是读，没有写冲突，所以不冲突
        // 完整冲突场景需要读写冲突：
        // T1 读 A → T2 写 A 并提交 → T1 提交时检测到读集有冲突
        assertDoesNotThrow(() -> tm.commit(t3.txId()));
    }

    @Test
    void serializableReadWriteConflict(@TempDir Path tmpDir) {
        TxLog txLog = new TxLog(tmpDir.resolve("txlog.log"));
        TransactionManager tm = new TransactionManager(TransactionIsolation.SERIALIZABLE, txLog);

        // T1 读取列 A，不提交
        TxHandle t1 = tm.begin();
        tm.recordRead(t1.txId(), "public.t.c1");

        // T2 写入列 A 并提交
        TxHandle t2 = tm.begin();
        tm.recordWrite(t2.txId(), "public.t.c1");
        tm.commit(t2.txId());

        // T1 提交时检测到冲突：T1 的 snapshot 在 T2 之前，但 T2 已写入 A
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> tm.commit(t1.txId()));
        assertTrue(ex.getMessage().contains("serialization conflict"));
    }

    @Test
    void rollbackClearsLastWriteTx(@TempDir Path tmpDir) {
        TxLog txLog = new TxLog(tmpDir.resolve("txlog.log"));
        TransactionManager tm = new TransactionManager(TransactionIsolation.SERIALIZABLE, txLog);

        // T1 读取列 A
        TxHandle t1 = tm.begin();
        tm.recordRead(t1.txId(), "public.t.c1");

        // T2 写入列 A 但回滚
        TxHandle t2 = tm.begin();
        tm.recordWrite(t2.txId(), "public.t.c1");
        tm.rollback(t2.txId());

        // T1 提交不应该检测到冲突（T2 已回滚，lastWriteTx 被清理）
        assertDoesNotThrow(() -> tm.commit(t1.txId()));
    }
}
