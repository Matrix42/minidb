package com.minidb.storage.common;

/** 表存储引擎类型。null 表示自动选择（有主键→LSM，无→Simple）。 */
public enum TableType {
    LSM,
    SIMPLE
}