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

/** 事务内读自己的写入(ACID 一致性):BEGIN 后 INSERT/UPDATE/DELETE,同一事务的 后续 SELECT 必须能看到这些未提交的变更(bug #1)。 */
class TransactionReadOwnWritesTest {

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
    void transactionReadsOwnInserts() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        executor.execute("INSERT INTO t VALUES (1)");

        TxHandle tx = tm.begin();
        // 事务内 INSERT
        executor.executeCursor("INSERT INTO t VALUES (2)", "public", tx);
        // 同一事务内 SELECT 应看到自己的写入
        int[] rows = ids(executor.executeCursor("SELECT id FROM t ORDER BY id", "public", tx));
        assertEquals(
                java.util.Arrays.toString(new int[] {1, 2}),
                java.util.Arrays.toString(rows),
                "事务内必须读到自己的 INSERT");
    }

    @Test
    void transactionReadsOwnUpdatesAndDeletes() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10), (2, 20)");

        TxHandle tx = tm.begin();
        executor.executeCursor("UPDATE t SET v = 99 WHERE id = 1", "public", tx);
        executor.executeCursor("DELETE FROM t WHERE id = 2", "public", tx);
        int[] rows = ids(executor.executeCursor("SELECT id FROM t ORDER BY id", "public", tx));
        assertEquals(
                java.util.Arrays.toString(new int[] {1}),
                java.util.Arrays.toString(rows),
                "事务内必须读到自己的 DELETE");
    }
}
