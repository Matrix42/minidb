package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LikeTest {

    @TempDir
    Path dataDir;
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
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES "
                + "(1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'a.b'), (5, NULL)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    private int[] ids(String sql) {
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        IntVector id = (IntVector) root.getVector("id");
        int[] result = new int[id.getValueCount()];
        for (int i = 0; i < result.length; i++) {
            result[i] = id.get(i);
        }
        root.close();
        return result;
    }

    @Test
    void likePrefix() {
        // 'A%' 匹配以 A 开头的
        int[] r = ids("SELECT id FROM t WHERE name LIKE 'A%' ORDER BY id");
        assertEquals(1, r.length);
        assertEquals(1, r[0]);
    }

    @Test
    void likeContains() {
        // '%o%' 匹配含 o 的:Bob、Carol
        int[] r = ids("SELECT id FROM t WHERE name LIKE '%o%' ORDER BY id");
        assertEquals(2, r.length);
        assertEquals(2, r[0]);
        assertEquals(3, r[1]);
    }

    @Test
    void likeSingleChar() {
        // '_ob' 匹配任意单字符 + ob:Bob
        int[] r = ids("SELECT id FROM t WHERE name LIKE '_ob' ORDER BY id");
        assertEquals(1, r.length);
        assertEquals(2, r[0]);
    }

    @Test
    void notLike() {
        // NOT LIKE 'A%' 匹配不以 A 开头的非 NULL:Bob、Carol、a.b
        int[] r = ids("SELECT id FROM t WHERE name NOT LIKE 'A%' ORDER BY id");
        assertEquals(3, r.length);
        assertEquals(2, r[0]);
        assertEquals(3, r[1]);
        assertEquals(4, r[2]);
    }

    @Test
    void likeRegexMetacharacterIsLiteral() {
        // '.' 是字面点,只匹配 'a.b',不匹配其它(正则元字符已转义)
        int[] r = ids("SELECT id FROM t WHERE name LIKE 'a.b' ORDER BY id");
        assertEquals(1, r.length);
        assertEquals(4, r[0]);
    }

    @Test
    void likeNullPropagates() {
        // NULL LIKE ... 结果为 NULL,WHERE 不保留
        int[] r = ids("SELECT id FROM t WHERE name LIKE '%'");
        assertEquals(4, r.length); // 只 4 个非 NULL
    }
}
