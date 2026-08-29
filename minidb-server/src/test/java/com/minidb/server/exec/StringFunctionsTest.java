package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StringFunctionsTest {

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
        executor.execute("CREATE TABLE t (id INTEGER, s VARCHAR)");
        executor.execute(
                "INSERT INTO t VALUES "
                        + "(1, 'hello'), (2, 'abracadabra'), (3, 'a,b,c'), (4, NULL)");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    /** 对第 id 行求值一个表达式,取 VARCHAR 结果(可为 null)。 */
    private String str(String expr, int id) {
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT " + expr + " AS v FROM t WHERE id = " + id))
                        .data();
        VarCharVector v = (VarCharVector) root.getVector("v");
        String result = v.isNull(0) ? null : new String(v.get(0), StandardCharsets.UTF_8);
        root.close();
        return result;
    }

    /** 对第 id 行求值一个表达式,取 INTEGER 结果(可为 null)。 */
    private Integer integer(String expr, int id) {
        VectorSchemaRoot root =
                ((QueryResult.Rows)
                                executor.execute(
                                        "SELECT " + expr + " AS v FROM t WHERE id = " + id))
                        .data();
        IntVector v = (IntVector) root.getVector("v");
        Integer result = v.isNull(0) ? null : v.get(0);
        root.close();
        return result;
    }

    @Test
    void position() {
        assertEquals(4, integer("POSITION('lo' IN 'hello')", 1));
        assertEquals(5, integer("POSITION('o' IN s)", 1));
        assertEquals(0, integer("POSITION('x' IN 'hello')", 1));
        assertEquals(1, integer("POSITION('' IN 'hello')", 1));
    }

    @Test
    void replace() {
        assertEquals("XbrXcXdXbrX", str("REPLACE('abracadabra', 'a', 'X')", 1));
        assertEquals("brcdbr", str("REPLACE(s, 'a', '')", 2));
        assertEquals("abc", str("REPLACE('abc', '', 'x')", 1));
    }

    @Test
    void leftRight() {
        assertEquals("he", str("LEFT('hello', 2)", 1));
        assertEquals("lo", str("RIGHT('hello', 2)", 1));
        assertEquals("hello", str("LEFT('hello', 10)", 1));
        assertEquals("", str("LEFT('hello', 0)", 1));
        assertEquals("", str("LEFT('hello', -1)", 1));
    }

    @Test
    void repeat() {
        assertEquals("ababab", str("REPEAT('ab', 3)", 1));
        assertEquals("", str("REPEAT('ab', 0)", 1));
        assertEquals("", str("REPEAT('x', -1)", 1));
    }

    @Test
    void reverse() {
        assertEquals("olleh", str("REVERSE('hello')", 1));
        // 按 code point 反转,多字节 emoji 不被拆坏
        assertEquals("b😀a", str("REVERSE('a😀b')", 1));
    }

    @Test
    void lpadRpad() {
        assertEquals("   hi", str("LPAD('hi', 5)", 1));
        assertEquals("hi   ", str("RPAD('hi', 5)", 1));
        assertEquals("000hi", str("LPAD('hi', 5, '0')", 1));
        assertEquals("hi000", str("RPAD('hi', 5, '0')", 1));
        assertEquals("abahi", str("LPAD('hi', 5, 'ab')", 1));
        assertEquals("hel", str("LPAD('hello', 3)", 1));
        assertEquals("", str("RPAD('hi', 5, '')", 1));
    }

    @Test
    void initcap() {
        assertEquals("Hello World", str("INITCAP('hello world')", 1));
        assertEquals("Hello World", str("INITCAP('HELLO world')", 1));
        assertEquals("Foo_Bar Baz", str("INITCAP('foo_bar baz')", 1));
    }

    @Test
    void ascii() {
        assertEquals(65, integer("ASCII('A')", 1));
        assertEquals(97, integer("ASCII('abc')", 1));
        assertEquals(0, integer("ASCII('')", 1));
    }

    @Test
    void chr() {
        assertEquals("A", str("CHR(65)", 1));
        assertEquals("é", str("CHR(233)", 1));
        assertEquals("\0", str("CHR(0)", 1));
    }

    @Test
    void splitPart() {
        assertEquals("b", str("SPLIT_PART('a,b,c', ',', 2)", 1));
        assertEquals("a", str("SPLIT_PART('a,b,c', ',', 1)", 1));
        assertEquals("", str("SPLIT_PART('a,b,c', ',', 5)", 1));
        assertEquals("", str("SPLIT_PART('a,,c', ',', 2)", 1));
        assertEquals("abc", str("SPLIT_PART('abc', '', 1)", 1));
    }

    @Test
    void nullPropagates() {
        assertNull(str("REVERSE(s)", 4));
        assertNull(str("REPLACE(s, 'a', 'b')", 4));
        assertNull(integer("POSITION('a' IN s)", 4));
    }
}
