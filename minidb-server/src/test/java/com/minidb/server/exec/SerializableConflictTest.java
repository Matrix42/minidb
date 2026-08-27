package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.transaction.TransactionManager;
import com.minidb.server.transaction.TxHandle;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SERIALIZABLE 隔离级别端到端冲突检测:写入路径必须把写集登记到
 * {@link TransactionManager}(recordWrite),否则 lastWriteTx 恒空,读写/写写冲突全部漏检。
 */
class SerializableConflictTest {

    @TempDir
    Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;
    TransactionManager tm;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
        tm = storage.transactionManager();
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    /** 执行 SELECT 并物化/释放结果。 */
    private void query(String sql, TxHandle tx) {
        QueryResult result = executor.executeCursor(sql, "public", tx);
        if (result instanceof QueryResult.Rows rows) {
            rows.data().close();
        } else if (result instanceof QueryResult.Cursor cursor) {
            VectorSchemaRoot root = cursor.handle().materialize();
            root.close();
        }
    }

    @Test
    void readWriteConflictDetected() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10)");

        // T1 读取 v(记入读集),不提交
        TxHandle t1 = tm.begin();
        query("SELECT v FROM t WHERE id = 1", t1);

        // T2 写 v 并提交(写集必须被登记,否则 lastWriteTx 恒空,冲突漏检)
        TxHandle t2 = tm.begin();
        executor.executeCursor("UPDATE t SET v = 20 WHERE id = 1", "public", t2);
        tm.commit(t2.txId());

        // T1 提交时检测到读写冲突
        assertThrows(IllegalStateException.class, () -> tm.commit(t1.txId()));
    }

    @Test
    void writeWriteConflictDetected() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10)");

        // T1 写 v(记入写集)
        TxHandle t1 = tm.begin();
        executor.executeCursor("UPDATE t SET v = 11 WHERE id = 1", "public", t1);

        // T2 写 v 并提交,先占住 lastWriteTx
        TxHandle t2 = tm.begin();
        executor.executeCursor("UPDATE t SET v = 22 WHERE id = 1", "public", t2);
        tm.commit(t2.txId());

        // T1 后提交:与 T2 同写一列,写写冲突
        assertThrows(IllegalStateException.class, () -> tm.commit(t1.txId()));
    }
}