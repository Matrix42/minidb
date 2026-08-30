package com.minidb.storage.lsm;

import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.common.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 二进制保序 key 编码(A2)回归:整数定长 + 符号位翻转使字节字典序 = 数值序 (负数 < 零 < 正数,含 MIN/MAX),字符串长度前缀保序,复合 key 无分隔符; decode
 * 按 schema 主键列类型还原;WAL 恢复类型保真(数字字符串 key 不被误 parse)。
 */
class KeyEncodingTest {
    private final TableSchema schema =
            new TableSchema(
                    "public",
                    "t",
                    List.of(
                            new ColumnMeta("id", ColumnType.INTEGER),
                            new ColumnMeta("name", ColumnType.VARCHAR)),
                    List.of("id"),
                    List.of(),
                    List.of());
    private final RootAllocator allocator = new RootAllocator();

    @Test
    void integerKeysOrderByBytes() {
        // 字节字典序 = 数值序(符号位翻转保证负数 < 正数)
        assertTrue(byteLess(encodeKey(List.of(-5)), encodeKey(List.of(0))));
        assertTrue(byteLess(encodeKey(List.of(0)), encodeKey(List.of(5))));
        assertTrue(byteLess(encodeKey(List.of(5)), encodeKey(List.of(100))));
        assertTrue(
                byteLess(
                        encodeKey(List.of(Integer.MIN_VALUE)),
                        encodeKey(List.of(Integer.MAX_VALUE))));
        // BIGINT(含 MIN/MAX 边界)
        assertTrue(byteLess(encodeKey(List.of(-10L)), encodeKey(List.of(-9L))));
        assertTrue(byteLess(encodeKey(List.of(9L)), encodeKey(List.of(10L))));
        assertTrue(
                byteLess(encodeKey(List.of(Long.MIN_VALUE)), encodeKey(List.of(Long.MAX_VALUE))));
        // SMALLINT
        assertTrue(byteLess(encodeKey(List.of((short) -3)), encodeKey(List.of((short) 3))));
    }

    @Test
    void stringAndCompositeKeysOrderByBytes() {
        // 长度前缀:前缀关系下短串恒小(字典序一致)
        assertTrue(byteLess(encodeKey(List.of("a")), encodeKey(List.of("b"))));
        assertTrue(byteLess(encodeKey(List.of("ab")), encodeKey(List.of("abc"))));
        assertTrue(byteLess(encodeKey(List.of("z")), encodeKey(List.of("zz"))));
        // 复合 key [int, str]:先比首列,同列再比次列
        assertTrue(byteLess(encodeKey(List.of(1, "a")), encodeKey(List.of(1, "b"))));
        assertTrue(byteLess(encodeKey(List.of(1, "a")), encodeKey(List.of(2, "a"))));
    }

    @Test
    void decodeRoundTripsBySchema() {
        TableSchema multi =
                new TableSchema(
                        "public",
                        "mt",
                        List.of(
                                new ColumnMeta("a", ColumnType.INTEGER),
                                new ColumnMeta("b", ColumnType.BIGINT),
                                new ColumnMeta("c", ColumnType.VARCHAR)),
                        List.of("a", "b", "c"),
                        List.of(),
                        List.of());
        assertEquals(List.of(1, 2L, "x"), decodeKey(encodeKey(List.of(1, 2L, "x")), multi));
        assertEquals(List.of(-7, -8L, "z"), decodeKey(encodeKey(List.of(-7, -8L, "z")), multi));
        assertEquals(
                List.of(Integer.MIN_VALUE, Long.MAX_VALUE, "q"),
                decodeKey(encodeKey(List.of(Integer.MIN_VALUE, Long.MAX_VALUE, "q")), multi));
    }

    @Test
    void negativeKeyRangeScan(@TempDir Path dir) throws Exception {
        // 负整数主键的 range 裁剪正确(依赖字节序保序)。
        // 每行 ~700B 撑出多个 block(64KB),块级裁剪才能生效
        MemTable mt = new MemTable(schema, 1024 * 1024);
        for (int i = -25; i < 25; i++) {
            mt.put(List.of(i), new RowValue(RowValue.INSERT, new Object[] {i, "x".repeat(700)}));
        }
        Path sstFile = dir.resolve("sst-L0-000001.sst");
        SSTableWriter writer =
                new SSTableWriter(sstFile, 0, schema, new ArrowPartFormat(), allocator, 10);
        writer.writeFromMemTable(mt);

        SSTableReader reader = new SSTableReader(sstFile, schema, new ArrowPartFormat(), allocator);
        // [5, 10]:与负值块 [-25, 21) 相交则整块读(超集),但 21..24 所在块必须被裁掉;
        // 负值保序错误会导致二分错位丢行
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = reader.scan(List.of(5), List.of(10));
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            for (int i = 0; i < batch.getRowCount(); i++) {
                rows.add(new Object[] {batch.getVector(0).getObject(i)});
            }
        }
        it.close();
        List<Integer> ids = rows.stream().map(r -> (Integer) r[0]).toList();
        assertTrue(ids.containsAll(List.of(5, 6, 7, 8, 9, 10)), "目标行不得因裁剪丢失: " + ids);
        assertTrue(ids.stream().noneMatch(v -> v >= 21), "不相交块应被裁剪: " + ids);
        reader.close();
    }

    @Test
    void walStringKeyNotParsedToInteger(@TempDir Path dir) throws Exception {
        // 预存 bug 修复:VARCHAR 主键 "123" 经 WAL 恢复后必须是 String,
        // 不得被「试 parse 整数」误转成 Integer(否则与写入侧类型不一致引发 CCE)
        TableSchema strSchema =
                new TableSchema(
                        "public",
                        "t",
                        List.of(
                                new ColumnMeta("id", ColumnType.VARCHAR),
                                new ColumnMeta("v", ColumnType.VARCHAR)),
                        List.of("id"),
                        List.of(),
                        List.of());
        Path walFile = dir.resolve("wal.log");
        WAL wal = new WAL(walFile, strSchema);
        wal.append(List.of("123"), new RowValue(RowValue.INSERT, new Object[] {"123", "x"}));
        wal.close();

        WAL wal2 = new WAL(walFile, strSchema);
        List<WAL.Entry> entries = wal2.recover();
        assertEquals(1, entries.size());
        Object k = entries.get(0).key().get(0);
        assertTrue(k instanceof String, "VARCHAR 主键恢复后必须是 String,实际 " + k.getClass());
        assertEquals("123", k);
        wal2.close();
    }

    /**
     * 无符号字节比较——encodeKey 翻转符号位后,无符号字典序 = 数值序 (0x80+ 是正数域,Java 的 Arrays.compare 按有符号 byte 比较会反序)。
     */
    private static boolean byteLess(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) return cmp < 0;
        }
        return a.length < b.length;
    }

    private static byte[] encodeKey(List<Object> key) {
        return SSTableWriter.encodeKey(key);
    }

    private static List<Object> decodeKey(byte[] bytes, TableSchema schema) {
        return SSTableReader.decodeKey(bytes, schema);
    }
}
