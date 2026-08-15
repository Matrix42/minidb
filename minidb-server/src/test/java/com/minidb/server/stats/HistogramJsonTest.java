package com.minidb.server.stats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.storage.common.ColumnType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistogramJsonTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void histogramJsonRoundTrip() throws Exception {
        Histogram h = new Histogram(ColumnType.INTEGER,
                List.of(new Histogram.Bucket("1", "5", 3), new Histogram.Bucket("6", "10", 2)),
                List.of(new Histogram.McValue("3", 2)),
                8, 1, 10);

        String json = MAPPER.writeValueAsString(h);
        Histogram back = MAPPER.readValue(json, Histogram.class);

        assertEquals(ColumnType.INTEGER, back.type());
        assertEquals(h.buckets(), back.buckets());
        assertEquals(h.mcv(), back.mcv());
        assertEquals(h.distinctCount(), back.distinctCount());
        assertEquals(h.nullCount(), back.nullCount());
        assertEquals(h.totalRows(), back.totalRows());
    }

    @Test
    void emptyHistogramJsonRoundTrip() throws Exception {
        Histogram h = Histogram.empty(ColumnType.VARCHAR);
        Histogram back = MAPPER.readValue(MAPPER.writeValueAsString(h), Histogram.class);
        assertEquals(ColumnType.VARCHAR, back.type());
        assertEquals(0, back.totalRows());
    }

    @Test
    void tableStatsJsonRoundTrip() throws Exception {
        TableStats ts = new TableStats(
                Map.of("id", new Histogram(ColumnType.INTEGER, List.of(),
                        List.of(), 5, 0, 10)),
                10, false);
        TableStats back = MAPPER.readValue(MAPPER.writeValueAsString(ts), TableStats.class);
        assertEquals(10, back.rowCount());
        assertEquals(false, back.stale());
        assertEquals(ts.columnHistograms().keySet(), back.columnHistograms().keySet());
    }
}
