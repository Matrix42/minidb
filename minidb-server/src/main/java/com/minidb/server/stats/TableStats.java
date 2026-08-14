package com.minidb.server.stats;

import java.util.Map;

public record TableStats(Map<String, Histogram> columnHistograms, long rowCount, boolean stale) {

    /** 便捷构造:尚未有 rowCount 的旧调用点默认 0。 */
    public TableStats(Map<String, Histogram> columnHistograms, boolean stale) {
        this(columnHistograms, 0L, stale);
    }
}
