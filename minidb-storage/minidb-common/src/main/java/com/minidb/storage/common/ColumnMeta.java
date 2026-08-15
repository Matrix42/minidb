package com.minidb.storage.common;

/**
 * 一列元数据。precision/scale 仅对 DECIMAL/NUMERIC 有意义,其余类型恒
 * {@link #PRECISION_UNSET}/{@link #SCALE_UNSET}。nullable 表示是否可为 NULL
 * (列级 NOT NULL 约束)。
 */
public record ColumnMeta(String name, ColumnType type, int precision, int scale, Boolean nullable) {

    public static final int PRECISION_UNSET = -1;
    public static final int SCALE_UNSET = -1;

    public ColumnMeta {
        // 旧 catalog.json 无 nullable 字段,Jackson 反序列化为 null;归一化为 true(列全可空,向后兼容)。
        nullable = nullable == null ? Boolean.TRUE : nullable;
    }

    public ColumnMeta(String name, ColumnType type) {
        this(name, type, PRECISION_UNSET, SCALE_UNSET, true);
    }

    public ColumnMeta(String name, ColumnType type, int precision, int scale) {
        this(name, type, precision, scale, true);
    }

    public ColumnMeta(String name, ColumnType type, boolean nullable) {
        this(name, type, PRECISION_UNSET, SCALE_UNSET, nullable);
    }
}
