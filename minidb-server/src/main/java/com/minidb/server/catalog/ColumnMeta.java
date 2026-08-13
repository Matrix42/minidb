package com.minidb.server.catalog;

/**
 * 一列元数据。precision/scale 仅对 DECIMAL/NUMERIC 有意义,其余类型恒
 * {@link #PRECISION_UNSET}/{@link #SCALE_UNSET}。
 */
public record ColumnMeta(String name, ColumnType type, int precision, int scale) {

    public static final int PRECISION_UNSET = -1;
    public static final int SCALE_UNSET = -1;

    public ColumnMeta(String name, ColumnType type) {
        this(name, type, PRECISION_UNSET, SCALE_UNSET);
    }
}
