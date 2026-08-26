package com.minidb.server.transaction;

/**
 * SQL 标准事务隔离级别,支持连字符和蛇形命名两种字符串形式。
 */
public enum TransactionIsolation {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE;

    /**
     * 从字符串解析隔离级别,支持 {@code read-uncommitted} / {@code read_uncommitted} 等变体。
     */
    public static TransactionIsolation fromString(String s) {
        return switch (s.toLowerCase(java.util.Locale.ROOT)) {
            case "read-uncommitted", "read_uncommitted" -> READ_UNCOMMITTED;
            case "read-committed", "read_committed" -> READ_COMMITTED;
            case "repeatable-read", "repeatable_read" -> REPEATABLE_READ;
            case "serializable" -> SERIALIZABLE;
            default -> throw new IllegalArgumentException(
                    "unknown isolation level: " + s + " (supported: read-uncommitted, read-committed, repeatable-read, serializable)");
        };
    }
}