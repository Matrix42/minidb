package com.minidb.storage.common;

/** 表的落盘存储格式,决定用哪个 {@code PartFormat} 实现。 */
public enum StorageFormat {
    /** Arrow IPC 文件。 */
    ARROW,
    /** Apache Parquet,默认格式。 */
    PARQUET;

    /** 默认存储格式。 */
    public static final StorageFormat DEFAULT = PARQUET;

    public static StorageFormat fromString(String name) {
        return switch (name.toUpperCase(java.util.Locale.ROOT)) {
            case "ARROW", "IPC" -> ARROW;
            case "PARQUET" -> PARQUET;
            default -> throw new IllegalArgumentException("unknown storage format: " + name);
        };
    }
}
