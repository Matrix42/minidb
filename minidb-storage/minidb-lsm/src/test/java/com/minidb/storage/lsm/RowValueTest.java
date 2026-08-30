package com.minidb.storage.lsm;

import com.minidb.storage.common.RowValue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RowValueTest {
    @Test
    void insertKind() {
        RowValue rv = new RowValue((byte) 0, new Object[] {"a", 1});
        assertEquals((byte) 0, rv.kind());
        assertArrayEquals(new Object[] {"a", 1}, rv.values());
    }

    @Test
    void deleteHasNullValues() {
        RowValue rv = new RowValue((byte) 2, null);
        assertEquals((byte) 2, rv.kind());
        assertNull(rv.values());
    }

    @Test
    void constants() {
        assertEquals(0, RowValue.INSERT);
        assertEquals(1, RowValue.UPDATE);
        assertEquals(2, RowValue.DELETE);
    }
}
