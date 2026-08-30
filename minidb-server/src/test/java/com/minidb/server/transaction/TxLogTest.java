package com.minidb.server.transaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TxLogTest {

    @Test
    void appendAndRecoverCommitted(@TempDir Path tmpDir) throws Exception {
        Path logFile = tmpDir.resolve("txlog.log");
        TxLog txLog = new TxLog(logFile);

        txLog.append(1L, TxLog.STATUS_COMMIT);
        txLog.append(3L, TxLog.STATUS_ABORT);
        txLog.append(5L, TxLog.STATUS_COMMIT);
        txLog.close();

        // 重新打开，恢复
        TxLog txLog2 = new TxLog(logFile);
        Set<Long> committed = txLog2.recoverCommitted();
        txLog2.close();

        assertEquals(Set.of(1L, 5L), committed);
        // txId=3 是 ABORT，不在 committed 集合中
    }

    @Test
    void truncateClearsAll(@TempDir Path tmpDir) throws Exception {
        Path logFile = tmpDir.resolve("txlog.log");
        TxLog txLog = new TxLog(logFile);

        txLog.append(1L, TxLog.STATUS_COMMIT);
        txLog.append(2L, TxLog.STATUS_COMMIT);
        txLog.truncate();
        txLog.close();

        TxLog txLog2 = new TxLog(logFile);
        Set<Long> committed = txLog2.recoverCommitted();
        txLog2.close();
        assertTrue(committed.isEmpty());
    }

    @Test
    void emptyLogReturnsEmptySet(@TempDir Path tmpDir) throws Exception {
        Path logFile = tmpDir.resolve("txlog.log");
        TxLog txLog = new TxLog(logFile);
        Set<Long> committed = txLog.recoverCommitted();
        txLog.close();
        assertTrue(committed.isEmpty());
    }
}
