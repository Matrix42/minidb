package com.minidb.server.transaction;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Serializable 隔离级别的读写集，用于冲突检测。 */
public class TxAccessSet {
    final long snapshotTxId;
    final Set<String> readSet = ConcurrentHashMap.newKeySet();
    final Set<String> writeSet = ConcurrentHashMap.newKeySet();

    TxAccessSet(long snapshotTxId) {
        this.snapshotTxId = snapshotTxId;
    }
}