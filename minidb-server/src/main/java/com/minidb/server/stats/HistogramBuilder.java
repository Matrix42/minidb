package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

public final class HistogramBuilder {

    public static final int BUCKET_COUNT = 10;
    public static final int MCV_CAP = 10;

    private HistogramBuilder() {
    }

    public static Histogram build(List<? extends ValueVector> columnBatches, ColumnType type) {
        List<Comparable<?>> values = new ArrayList<>();
        long nullCount = 0;
        for (ValueVector v : columnBatches) {
            for (int i = 0; i < v.getValueCount(); i++) {
                if (v.isNull(i)) {
                    nullCount++;
                    continue;
                }
                values.add(read(v, i, type));
            }
        }
        if (values.isEmpty()) {
            return Histogram.empty();
        }
        values.sort(Comparator.comparing(HistogramBuilder::normalize));
        long totalRows = values.size();
        long distinctCount = distinctCount(values);
        List<Histogram.McValue> mcv = topMcv(values);
        List<Histogram.Bucket> buckets = equiDepth(values, totalRows);
        return new Histogram(buckets, mcv, distinctCount, nullCount, totalRows);
    }

    private static Comparable<?> read(ValueVector v, int i, ColumnType type) {
        return switch (type) {
            case INTEGER -> ((IntVector) v).get(i);
            case BIGINT -> ((BigIntVector) v).get(i);
            case DOUBLE -> ((Float8Vector) v).get(i);
            case VARCHAR -> new String(((VarCharVector) v).get(i));
            case BOOLEAN -> ((BitVector) v).get(i) == 1;
            case DATE -> ((DateDayVector) v).get(i);
            case TIMESTAMP -> ((TimeStampMilliVector) v).get(i);
        };
    }

    private static long distinctCount(List<Comparable<?>> sorted) {
        long d = 1;
        for (int i = 1; i < sorted.size(); i++) {
            if (normalize(sorted.get(i)).compareTo(normalize(sorted.get(i - 1))) != 0) {
                d++;
            }
        }
        return d;
    }

    private static List<Histogram.McValue> topMcv(List<Comparable<?>> values) {
        // Group by the value itself (not normalized) so that McValue stores the
        // ORIGINAL value. Within a single column all values share one type, so
        // identity grouping is equivalent to normalize-based grouping for
        // counting purposes, while preserving Integer/String in the McValue.
        // Histogram.equalitySelectivity normalizes both sides via compareValue,
        // so storing the original type is correct and preserves display fidelity.
        Map<Comparable<?>, Long> freq = values.stream().collect(
                Collectors.groupingBy(v -> v, Collectors.counting()));
        return freq.entrySet().stream()
                .sorted(Map.Entry.<Comparable<?>, Long>comparingByValue().reversed())
                .limit(MCV_CAP)
                .map(e -> new Histogram.McValue(e.getKey(), e.getValue()))
                .toList();
    }

    private static List<Histogram.Bucket> equiDepth(List<Comparable<?>> sorted, long total) {
        List<Histogram.Bucket> buckets = new ArrayList<>();
        int n = sorted.size();
        int bcount = Math.min(BUCKET_COUNT, n);
        if (bcount == 0) {
            return buckets;
        }
        int per = n / bcount;
        int remainder = n % bcount;
        int idx = 0;
        for (int b = 0; b < bcount; b++) {
            int size = per + (b < remainder ? 1 : 0);
            if (size == 0) {
                continue;
            }
            Comparable<?> lower = sorted.get(idx);
            Comparable<?> upper = sorted.get(idx + size - 1);
            buckets.add(new Histogram.Bucket(lower, upper, size));
            idx += size;
        }
        return buckets;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<Object> normalize(Comparable<?> c) {
        if (c instanceof java.math.BigDecimal bd) {
            return (Comparable<Object>) (Comparable) Double.valueOf(bd.doubleValue());
        }
        if (c instanceof Integer i) {
            return (Comparable<Object>) (Comparable) Double.valueOf(i.doubleValue());
        }
        if (c instanceof Long l) {
            return (Comparable<Object>) (Comparable) Double.valueOf(l.doubleValue());
        }
        return (Comparable<Object>) c;
    }
}
