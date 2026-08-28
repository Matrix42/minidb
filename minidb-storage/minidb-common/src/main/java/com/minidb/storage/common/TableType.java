package com.minidb.storage.common;

/** 表存储引擎类型。null 表示自动选择（有主键→LSM，无→Simple）。 */
public enum TableType {
    LSM,
    SIMPLE,
    /** 物化视图：物理存储由查询结果填充，DML 增量刷新。 */
    MATERIALIZED_VIEW
}