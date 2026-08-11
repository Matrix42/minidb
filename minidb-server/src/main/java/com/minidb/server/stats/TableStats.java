package com.minidb.server.stats;

import java.io.Serializable;
import java.util.Map;

public record TableStats(Map<String, Histogram> columnHistograms, boolean stale)
        implements Serializable {
    private static final long serialVersionUID = 1L;
}
