package com.minidb.storage.common;

public record RowValue(byte kind, Object[] values) {
    public static final byte INSERT = 0;
    public static final byte UPDATE = 1;
    public static final byte DELETE = 2;
}