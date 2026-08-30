package com.minidb.server.stats;

import java.util.Map;

public record TableStats(Map<String, Histogram> columnHistograms, long rowCount, boolean stale) {}
