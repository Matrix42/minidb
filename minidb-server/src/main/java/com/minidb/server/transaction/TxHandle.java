package com.minidb.server.transaction;

import java.util.concurrent.atomic.AtomicReference;

public class TxHandle {
    private final long txId;
    private volatile long snapshotTxId;
    private final AtomicReference<TxStatus> status;

    public TxHandle(long txId, long snapshotTxId) {
        this.txId = txId;
        this.snapshotTxId = snapshotTxId;
        this.status = new AtomicReference<>(TxStatus.ACTIVE);
    }

    public long txId() { return txId; }
    public long snapshotTxId() { return snapshotTxId; }
    public TxStatus status() { return status.get(); }

    /**
     * READ_COMMITTED 级别：每语句执行前刷新快照。
     * 只在 ACTIVE 状态下有效。
     */
    public void refreshSnapshot(long newSnapshotTxId) {
        if (status.get() == TxStatus.ACTIVE) {
            this.snapshotTxId = newSnapshotTxId;
        }
    }

    /**
     * 标记为已提交。只在 ACTIVE → COMMITTED 转换时成功。
     * @return true 如果转换成功，false 如果状态不是 ACTIVE
     */
    public boolean markCommitted() {
        return status.compareAndSet(TxStatus.ACTIVE, TxStatus.COMMITTED);
    }

    /**
     * 标记为已回滚。只在 ACTIVE → ABORTED 转换时成功。
     * @return true 如果转换成功，false 如果状态不是 ACTIVE
     */
    public boolean markAborted() {
        return status.compareAndSet(TxStatus.ACTIVE, TxStatus.ABORTED);
    }
}