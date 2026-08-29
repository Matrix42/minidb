package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SubqueryTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
        executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
        executor.execute("CREATE TABLE b (id INTEGER, aid INTEGER)");
        executor.execute("CREATE TABLE c (x INTEGER, y INTEGER)");
        executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z'), (4, 'w')");
        executor.execute("INSERT INTO b VALUES (1, 1), (2, 2), (3, 2), (4, 3), (5, NULL)");
        executor.execute("INSERT INTO c VALUES (1, 1), (2, NULL), (NULL, 3), (NULL, NULL)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void uncorrelatedInSubquery() {
        // b.aid 去重后 = {1, 2, 3, NULL};NULL 不匹配,故命中 a.id 1,2,3。
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT id FROM a WHERE a.id IN (SELECT b.aid FROM b) ORDER BY id"))
                        .data();
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(3, id.getValueCount());
        assertEquals(1, id.get(0));
        assertEquals(2, id.get(1));
        assertEquals(3, id.get(2));
        root.close();
    }

    @Test
    void correlatedScalarSubquery() {
        // 每个 a.id 统计 b 里 aid 匹配的行数;无匹配的 a.id=4 得 COUNT(*)=0。
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.aid = a.id) AS c "
                                                + "FROM a ORDER BY a.id"))
                        .data();
        IntVector id = (IntVector) root.getVector("id");
        BigIntVector c = (BigIntVector) root.getVector("c");
        assertEquals(4, id.getValueCount());
        for (int i = 0; i < 4; i++) {
            assertEquals(i + 1, id.get(i));
        }
        assertEquals(1, c.get(0)); // a.id=1 匹配 b.aid=1 一次
        assertEquals(2, c.get(1)); // a.id=2 匹配 b.aid=2 两次
        assertEquals(1, c.get(2)); // a.id=3 匹配 b.aid=3 一次
        assertEquals(0, c.get(3)); // a.id=4 无匹配
        root.close();
    }

    @Test
    void isNotDistinctFromPredicate() {
        // 行 (1,1) 与 (NULL,NULL) 满足 null-safe 等值,共 2 行。
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT COUNT(*) AS n FROM c WHERE x IS NOT DISTINCT FROM y"))
                        .data();
        assertEquals(2, ((BigIntVector) root.getVector("n")).get(0));
        root.close();
    }

    @Test
    void isDistinctFromPredicate() {
        // 行 (2,NULL) 与 (NULL,3) 满足 IS DISTINCT FROM(一 null 一非 null),共 2 行。
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT COUNT(*) AS n FROM c WHERE x IS DISTINCT FROM y"))
                        .data();
        assertEquals(2, ((BigIntVector) root.getVector("n")).get(0));
        root.close();
    }

    @Test
    void notInWithNullReturnsEmpty() {
        // b.aid 含 NULL:NOT IN 恒非 TRUE → 结果空集(三值逻辑陷阱)。
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT id FROM a WHERE a.id NOT IN (SELECT b.aid FROM b) ORDER BY id"))
                        .data();
        assertEquals(0, root.getRowCount());
        root.close();
    }

    @Test
    void notInWithoutNull() {
        // 滤掉 NULL 后,b.aid 非空 = {1,2,3},a.id NOT IN → 只有 4。
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT id FROM a WHERE a.id NOT IN "
                                                + "(SELECT b.aid FROM b WHERE b.aid IS NOT NULL) ORDER BY id"))
                        .data();
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(1, id.getValueCount());
        assertEquals(4, id.get(0));
        root.close();
    }
}
