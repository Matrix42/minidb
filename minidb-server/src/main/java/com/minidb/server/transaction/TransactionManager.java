package com.minidb.server.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManager {

    private final AtomicLong nextTxId = new AtomicLong(1);
    private final ConcurrentHashMap<Long, TxHandle> txHandles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, TxStatus> txStatuses = new ConcurrentHashMap<>();
    private final TransactionIsolation isolationLevel;
    private final TxLog txLog;
    private final AtomicInteger activeTxCount = new AtomicInteger(0);
    // 截断串行化锁：COMMIT 记录的 append 与 activeTxCount 的 decrement/truncate 判定必须
    // 原子发生。否则并发提交下,一个事务刚 append 完 COMMIT、尚未 decrement,另一个路径
    // 看到的 activeTxCount 已归零并把日志截断,已提交记录随之丢失。
    private final ReentrantLock truncateLock = new ReentrantLock();
    // 自上次截断以来是否有提交发生。一旦有 COMMIT 写入日志,后续 commit/rollback 归零时
    // 一律不再进程内截断(保证已提交决定的记录不丢);真正的截断交由 StorageManager.loadAll
    // 在启动恢复完成后统一执行。
    private volatile boolean committedSinceLastTruncate = false;

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

    /**
     * 计算事务开始时的快照点。
     *
     * @param txId 当前事务 ID（预留，未来可能用于基于 txId 的精确快照计算）
     */
    private long computeSnapshot(long txId) {
        return switch (isolationLevel) {
            case READ_UNCOMMITTED -> -1L;
            case READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE -> latestCommittedTxId();
        };
    }

    /** 提交事务：冲突检测 → 写全局日志 → 标记 COMMITTED。 调用方负责在各表上调用 commitTx(txId) 完成数据合并。 */
    public void commit(long txId) {
        TxHandle handle = txHandles.get(txId);
        if (handle == null || handle.status() != TxStatus.ACTIVE) {
            throw new IllegalStateException("transaction " + txId + " is not active");
        }

        // 截断串行化：append COMMIT 与 decrement/truncate 在同一临界区，阻断
        // 「append 后被并发路径截断」的竞态（见 truncateLock 字段说明）。
        truncateLock.lock();
        try {
            // Serializable 冲突检测
            if (isolationLevel == TransactionIsolation.SERIALIZABLE) {
                checkSerializableConflict(txId);
            }

            // 写全局事务日志（决定性步骤）
            txLog.append(txId, TxLog.STATUS_COMMIT);
            committedSinceLastTruncate = true;

            // 标记状态
            handle.markCommitted();
            txStatuses.put(txId, TxStatus.COMMITTED);

            // 清理
            accessSets.remove(txId);
            // 事务句柄不再被任何后续路径引用(recordRead/recordWrite 仅在 ACTIVE 期调用),
            // 移除避免长期运行累积泄漏;txStatuses 保留 COMMITTED 供 latestCommittedTxId 用。
            txHandles.remove(txId);
            activeTxCount.decrementAndGet();

            // 截断检查（随锁串行化，避免与 append/decrement 交错）
            tryTruncateTxLog();
        } finally {
            truncateLock.unlock();
        }
    }

    /** 回滚事务：标记 ABORTED。调用方负责在各表上调用 rollbackTx(txId)。 */
    public void rollback(long txId) {
        TxHandle handle = txHandles.get(txId);
        if (handle == null || handle.status() != TxStatus.ACTIVE) {
            throw new IllegalStateException("transaction " + txId + " is not active");
        }

        // 清理 lastWriteTx，防止已回滚事务的写入影响后续冲突检测
        TxAccessSet access = accessSets.remove(txId);
        if (access != null) {
            for (String key : access.writeSet) {
                lastWriteTx.remove(key, txId);
            }
        }

        handle.markAborted();
        txStatuses.put(txId, TxStatus.ABORTED);
        // 移除事务句柄(见 commit 的同类注释);txStatuses 保留 ABORTED 供 statusOf 用。
        txHandles.remove(txId);
        truncateLock.lock();
        try {
            activeTxCount.decrementAndGet();
            tryTruncateTxLog();
        } finally {
            truncateLock.unlock();
        }
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
        TxAccessSet access =
                accessSets.computeIfAbsent(
                        txId, k -> new TxAccessSet(txHandles.get(txId).snapshotTxId()));
        access.readSet.add(key);
    }

    public void recordWrite(long txId, String key) {
        if (isolationLevel != TransactionIsolation.SERIALIZABLE) return;
        lastWriteTx.put(key, txId);
        TxAccessSet access =
                accessSets.computeIfAbsent(
                        txId, k -> new TxAccessSet(txHandles.get(txId).snapshotTxId()));
        access.writeSet.add(key);
    }

    private void checkSerializableConflict(long txId) {
        TxAccessSet access = accessSets.get(txId);
        if (access == null) return;

        // 读写冲突：本事务读过的列被快照之后开始的事务写过。
        for (String col : access.readSet) {
            Long writerTx = lastWriteTx.get(col);
            if (writerTx != null && writerTx != txId && access.snapshotTxId < writerTx) {
                throw new IllegalStateException(
                        "serialization conflict: transaction "
                                + txId
                                + " read "
                                + col
                                + " but transaction "
                                + writerTx
                                + " wrote it after snapshot "
                                + access.snapshotTxId);
            }
        }
        // 写写冲突:本事务写过的列被快照之后开始/提交的事务写过——txId 单调递增,
        // writerTx > snapshotTxId 意即对方在本事务快照之后提交或仍活跃(lost update)。
        for (String col : access.writeSet) {
            Long writerTx = lastWriteTx.get(col);
            if (writerTx != null && writerTx != txId && access.snapshotTxId < writerTx) {
                throw new IllegalStateException(
                        "serialization conflict: transaction "
                                + txId
                                + " wrote "
                                + col
                                + " but transaction "
                                + writerTx
                                + " also wrote it after snapshot "
                                + access.snapshotTxId);
            }
        }
    }

    private void tryTruncateTxLog() {
        // 一旦有提交记录入日志就停止进程内截断,把权威截断留给启动恢复(StorageManager.loadAll),
        // 避免并发提交/回滚把 COMMIT 记录截掉(见 truncateLock 字段说明)。
        if (activeTxCount.get() == 0 && !committedSinceLastTruncate) {
            txLog.truncate();
        }
    }
}
