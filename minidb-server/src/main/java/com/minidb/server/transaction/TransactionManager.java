package com.minidb.server.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {

    private final AtomicLong nextTxId = new AtomicLong(1);
    private final ConcurrentHashMap<Long, TxHandle> txHandles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, TxStatus> txStatuses = new ConcurrentHashMap<>();
    private final TransactionIsolation isolationLevel;
    private final TxLog txLog;
    private final AtomicInteger activeTxCount = new AtomicInteger(0);

    // Serializable 专用
    private final ConcurrentHashMap<String, Long> lastWriteTx = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, TxAccessSet> accessSets = new ConcurrentHashMap<>();

    public TransactionManager(TransactionIsolation isolationLevel, TxLog txLog) {
        this.isolationLevel = isolationLevel;
        this.txLog = txLog;
    }

    public TransactionIsolation isolationLevel() {
        return isolationLevel;
    }

    /** 获取最近已提交的事务 ID（用于快照计算）。txId=0 表示无已提交事务。 */
    public long latestCommittedTxId() {
        // 最新 COMMITTED 的 txId = nextTxId - 1 减去还处于 ACTIVE/ABORTED 的
        // 简化：遍历找到最大的 COMMITTED txId
        long maxCommitted = 0;
        for (var entry : txStatuses.entrySet()) {
            if (entry.getValue() == TxStatus.COMMITTED && entry.getKey() > maxCommitted) {
                maxCommitted = entry.getKey();
            }
        }
        return maxCommitted;
    }

    public TxHandle begin() {
        long txId = nextTxId.getAndIncrement();
        long snapshotTxId = computeSnapshot(txId);
        TxHandle handle = new TxHandle(txId, snapshotTxId);
        txHandles.put(txId, handle);
        txStatuses.put(txId, TxStatus.ACTIVE);
        activeTxCount.incrementAndGet();
        return handle;
    }

    private long computeSnapshot(long txId) {
        return switch (isolationLevel) {
            case READ_UNCOMMITTED -> -1L;
            case READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE -> latestCommittedTxId();
        };
    }

    /**
     * 提交事务：冲突检测 → 写全局日志 → 标记 COMMITTED。
     * 调用方负责在各表上调用 commitTx(txId) 完成数据合并。
     */
    public void commit(long txId) {
        TxHandle handle = txHandles.get(txId);
        if (handle == null || handle.status() != TxStatus.ACTIVE) {
            throw new IllegalStateException("transaction " + txId + " is not active");
        }

        // Serializable 冲突检测
        if (isolationLevel == TransactionIsolation.SERIALIZABLE) {
            checkSerializableConflict(txId);
        }

        // 写全局事务日志（决定性步骤）
        txLog.append(txId, TxLog.STATUS_COMMIT);

        // 标记状态
        handle.markCommitted();
        txStatuses.put(txId, TxStatus.COMMITTED);

        // 清理
        accessSets.remove(txId);
        activeTxCount.decrementAndGet();

        // 截断检查
        tryTruncateTxLog();
    }

    /** 回滚事务：标记 ABORTED。调用方负责在各表上调用 rollbackTx(txId)。 */
    public void rollback(long txId) {
        TxHandle handle = txHandles.get(txId);
        if (handle == null || handle.status() != TxStatus.ACTIVE) {
            throw new IllegalStateException("transaction " + txId + " is not active");
        }

        handle.markAborted();
        txStatuses.put(txId, TxStatus.ABORTED);
        accessSets.remove(txId);
        activeTxCount.decrementAndGet();
        tryTruncateTxLog();
    }

    public TxStatus statusOf(long txId) {
        TxStatus status = txStatuses.get(txId);
        return status != null ? status : TxStatus.COMMITTED; // 未知 = 保守视作已提交
    }

    public int activeTxCount() {
        return activeTxCount.get();
    }

    // ---- Serializable 冲突检测 ----

    public void recordRead(long txId, String key) {
        if (isolationLevel != TransactionIsolation.SERIALIZABLE) return;
        TxAccessSet access = accessSets.computeIfAbsent(txId,
                k -> new TxAccessSet(txHandles.get(txId).snapshotTxId()));
        access.readSet.add(key);
    }

    public void recordWrite(long txId, String key) {
        if (isolationLevel != TransactionIsolation.SERIALIZABLE) return;
        lastWriteTx.put(key, txId);
        TxAccessSet access = accessSets.computeIfAbsent(txId,
                k -> new TxAccessSet(txHandles.get(txId).snapshotTxId()));
        access.writeSet.add(key);
    }

    private void checkSerializableConflict(long txId) {
        TxAccessSet access = accessSets.get(txId);
        if (access == null) return;

        for (String col : access.readSet) {
            Long writerTx = lastWriteTx.get(col);
            if (writerTx != null && writerTx != txId && access.snapshotTxId < writerTx) {
                throw new IllegalStateException(
                        "serialization conflict: transaction " + txId
                                + " read " + col + " but transaction " + writerTx
                                + " wrote it after snapshot " + access.snapshotTxId);
            }
        }
    }

    private void tryTruncateTxLog() {
        if (activeTxCount.get() == 0) {
            txLog.truncate();
        }
    }
}