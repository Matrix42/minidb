package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * 验证 VarChar 排序用 UTF-8 字节比较(ValueComparators)与 String.compareTo 语义一致:
 * UTF-8 编码保序,字节字典序 == code point 序。中文/emoji 覆盖非 ASCII 与补充字符。
 */
class SortSemanticsTest {

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
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    private String[] sortedNames(String valuesSql) {
        executor.execute("CREATE TABLE t (name VARCHAR)");
        executor.execute("INSERT INTO t VALUES " + valuesSql);
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(
                "SELECT name FROM t ORDER BY name")).data();
        VarCharVector v = (VarCharVector) root.getVector("name");
        String[] out = new String[v.getValueCount()];
        for (int i = 0; i < out.length; i++) {
            out[i] = new String(v.get(i), StandardCharsets.UTF_8);
        }
        root.close();
        return out;
    }

    @Test
    void chineseOrderByCodePoint() {
        // code point 序: 此(U+6B64) < 波(U+6CE2) < 阿(U+963F)
        String[] got = sortedNames("('阿'), ('波'), ('此')");
        assertArrayEquals(new String[]{"此", "波", "阿"}, got);
    }

    @Test
    void emojiSupplementaryOrder() {
        // 补充字符按 code point 序: 😀(U+1F600) < 😁(U+1F601)
        String[] got = sortedNames("('😁'), ('😀')");
        assertArrayEquals(new String[]{"😀", "😁"}, got);
    }

    @Test
    void mixedAsciiAndChinese() {
        // ASCII < 中文(UTF-8 里 ASCII 首字节 0x00-0x7F,中文首字节 >= 0xE0)
        String[] got = sortedNames("('中'), ('a'), ('Z')");
        assertArrayEquals(new String[]{"Z", "a", "中"}, got);
    }
}
