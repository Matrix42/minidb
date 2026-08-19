package com.minidb.storage.lsm;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.storage.common.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MemTableTest {
    private final TableSchema schema = new TableSchema("public", "t",
            List.of(new ColumnMeta("id", ColumnType.INTEGER), new ColumnMeta("name", ColumnType.VARCHAR)),
            List.of("id"), List.of(), List.of());

    @Test
    void putAndGet() {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        List<Object> key = List.of(42);
        mt.put(key, new RowValue(RowValue.INSERT, new Object[]{42, "hello"}));
        assertEquals("hello", mt.get(key).values()[1]);
    }

    @Test
    void getReturnsNullForMissing() {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        assertNull(mt.get(List.of(99)));
    }

    @Test
    void estimatedBytesGrows() {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        long before = mt.estimatedBytes();
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "x"}));
        assertTrue(mt.estimatedBytes() > before);
    }

    @Test
    void needsFlushWhenOverThreshold() {
        MemTable mt = new MemTable(schema, 100);
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "hello world"}));
        for (int i = 2; i < 20; i++) {
            mt.put(List.of(i), new RowValue(RowValue.INSERT, new Object[]{i, "value"}));
        }
        assertTrue(mt.needsFlush());
    }

    @Test
    void iteratorReturnsSortedByKey() {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(3), new RowValue(RowValue.INSERT, new Object[]{3, "c"}));
        mt.put(List.of(1), new RowValue(RowValue.INSERT, new Object[]{1, "a"}));
        mt.put(List.of(2), new RowValue(RowValue.INSERT, new Object[]{2, "b"}));
        var it = mt.iterator();
        assertTrue(it.hasNext());
        assertEquals(1, it.next().getKey().get(0));
        assertEquals(2, it.next().getKey().get(0));
        assertEquals(3, it.next().getKey().get(0));
        assertFalse(it.hasNext());
    }

    @Test
    void deleteTombstone() {
        MemTable mt = new MemTable(schema, 1024 * 1024);
        mt.put(List.of(1), new RowValue(RowValue.DELETE, null));
        RowValue rv = mt.get(List.of(1));
        assertEquals(RowValue.DELETE, rv.kind());
        assertNull(rv.values());
    }
}