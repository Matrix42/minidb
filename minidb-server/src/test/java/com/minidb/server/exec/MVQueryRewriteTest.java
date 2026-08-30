package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.TableSchema;
import com.minidb.storage.common.TableType;

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

/** 物化视图查询重写端到端测试:用户查询与 MV 定义结构一致时,查询应被重写为对 MV 表的扫描。 通过 EXPLAIN 观察计划是否命中 MV 表来验证。 */
class MVQueryRewriteTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        executor = new QueryExecutor(catalog, storage, allocator, new StatsManager(storage));
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void rewritesSpjQueryToMvScan() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t WHERE id > 1");

        // EXPLAIN 应显示对 mv 的扫描而不是对 t 的扫描
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("EXPLAIN SELECT id, name FROM t WHERE id > 1"))
                        .data();
        StringBuilder plan = new StringBuilder();
        for (int i = 0; i < root.getRowCount(); i++) {
            plan.append(root.getVector("operation").getObject(i)).append('\n');
        }
        root.close();

        System.out.println("SPJ rewrite plan:\n" + plan);
        // 物理计划中 mv 被 MiniDbScan 直接扫描
        org.junit.jupiter.api.Assertions.assertTrue(
                plan.toString().contains("mv"), "plan should scan mv, but was:\n" + plan);
    }

    @Test
    void rewritesAggregateQueryToMvScan() {
        executor.execute("CREATE TABLE t (g INTEGER, v INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT g, SUM(v) AS s FROM t GROUP BY g");

        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute("EXPLAIN SELECT g, SUM(v) AS s FROM t GROUP BY g"))
                        .data();
        StringBuilder plan = new StringBuilder();
        for (int i = 0; i < root.getRowCount(); i++) {
            plan.append(root.getVector("operation").getObject(i)).append('\n');
        }
        root.close();

        System.out.println("Aggregate rewrite plan:\n" + plan);
        org.junit.jupiter.api.Assertions.assertTrue(
                plan.toString().contains("mv"), "plan should scan mv, but was:\n" + plan);
    }

    @Test
    void rewrittenQueryReturnsCorrectRows() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t WHERE id > 1");

        // 重写后查询结果与直接查 MV 一致
        VectorSchemaRoot root =
                ((QueryResult.Rows) executor.execute("SELECT id FROM t WHERE id > 1 ORDER BY id"))
                        .data();
        IntVector iv = (IntVector) root.getVector("id");
        assertEquals(2, iv.getValueCount());
        assertEquals(2, iv.get(0));
        assertEquals(3, iv.get(1));
        root.close();
    }

    @Test
    void mvTypeIsMaterializedView() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM t");
        TableSchema ts = catalog.getTable("public", "mv");
        assertEquals(TableType.MATERIALIZED_VIEW, ts.tableType());
    }
}
