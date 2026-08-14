package com.minidb.server.stats;

import java.io.Serializable;
import java.util.Map;

public record TableStats(Map<String, Histogram> columnHistograms, long rowCount, boolean stale)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 便捷构造:尚未有 rowCount 的旧调用点(StatsManager 里 2 参调用)默认 0。 */
    public TableStats(Map<String, Histogram> columnHistograms, boolean stale) {
        this(columnHistograms, 0L, stale);
    }
}
