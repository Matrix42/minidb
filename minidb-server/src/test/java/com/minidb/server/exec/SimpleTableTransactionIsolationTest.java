package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.transaction.TransactionManager;
import com.minidb.server.transaction.TxHandle;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SimpleTable(无主键表)事务隔离 + 读自身写入(bug #5):事务 A 的 DELETE/UPDATE 改写 全表时,事务 B 不得看到空表;事务 A 自己仍能看到自身变更。
 */
class SimpleTableTransactionIsolationTest {

    @TempDir Path dataDir;
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

    private static int[] ids(QueryResult result) {
        VectorSchemaRoot root =
                result instanceof QueryResult.Rows rows
                        ? rows.data()
                        : ((QueryResult.Cursor) result).handle().materialize();
        IntVector v = (IntVector) root.getVector("id");
        int[] out = new int[v.getValueCount()];
        for (int i = 0; i < out.length; i++) {
            out[i] = v.get(i);
        }
        root.close();
        return out;
    }

    @Test
    void simpleTableDeleteInTransactionIsIsolatedAndRollsBack() {
        // 无主键 → SimpleTable
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");

        TxHandle txA = tm.begin();
        executor.executeCursor("DELETE FROM t WHERE id = 1", "public", txA);

        // A 自己看到删除后的 2 行
        int[] own = ids(executor.executeCursor("SELECT id FROM t ORDER BY id", "public", txA));
        assertEquals(
                java.util.Arrays.toString(new int[] {2, 3}),
                java.util.Arrays.toString(own),
                "事务 A 应看到自己的 DELETE");

        // 事务 B 仍看到完整的 3 行(不得是空表);表级 SERIALIZABLE 冲突检测下,
        // B 提交可能因与 A 的并发写冲突而被中止(合法),故此处只验证隔离读取,回滚即可。
        TxHandle txB = tm.begin();
        int[] other = ids(executor.executeCursor("SELECT id FROM t ORDER BY id", "public", txB));
        assertEquals(
                java.util.Arrays.toString(new int[] {1, 2, 3}),
                java.util.Arrays.toString(other),
                "未提交 DELETE 不得让其他事务看到空表");
        tm.rollback(txB.txId());

        // 回滚后 base 恢复
        tm.rollback(txA.txId());
        int[] after = ids(executor.execute("SELECT id FROM t ORDER BY id"));
        assertEquals(
                java.util.Arrays.toString(new int[] {1, 2, 3}),
                java.util.Arrays.toString(after),
                "回滚后 base 不变");
    }
}
