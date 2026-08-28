package com.minidb.storage.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/** 增量刷新所需的结构信息。用于 IncrementalRefreshEngine 判断刷新路径。 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface MVStructure {

    /** SPJ：SELECT ... FROM 单表 WHERE ... */
    record Spj(
            String querySql,
            List<String> outputColumns) implements MVStructure {
    }

    /** 单表聚合：GROUP BY + SUM/COUNT/AVG/MIN/MAX */
    record Aggregate(
            String querySql,
            List<String> outputColumns,
            List<String> groupByColumns,
            List<AggFunc> aggFuncs) implements MVStructure {
    }

    record AggFunc(String outputColumn, AggType type, String inputColumn) {
    }

    enum AggType { SUM, COUNT, AVG, MIN, MAX }
}