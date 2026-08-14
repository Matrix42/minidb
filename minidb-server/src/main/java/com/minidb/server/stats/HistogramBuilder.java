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
            return Histogram.empty(type);
        }
        values.sort(Comparator.comparing(v -> normalize(v, type)));
        long totalRows = values.size();
        long distinctCount = distinctCount(values, type);
        List<Histogram.McValue> mcv = topMcv(values);
        List<Histogram.Bucket> buckets = equiDepth(values, totalRows);
        return new Histogram(type, buckets, mcv, distinctCount, nullCount, totalRows);
    }

    private static Comparable<?> read(ValueVector v, int i, ColumnType type) {
        return switch (type) {
            case INTEGER -> Integer.toString(((IntVector) v).get(i));
            case BIGINT -> Long.toString(((BigIntVector) v).get(i));
            case DOUBLE -> Double.toString(((Float8Vector) v).get(i));
            case VARCHAR -> new String(((VarCharVector) v).get(i));
            case BOOLEAN -> Boolean.toString(((BitVector) v).get(i) == 1);
            case DATE -> Integer.toString(((DateDayVector) v).get(i));
            case TIMESTAMP -> Long.toString(((TimeStampMilliVector) v).get(i));
            default -> throw new IllegalArgumentException("histogram: unsupported type " + type);
        };
    }

    private static long distinctCount(List<Comparable<?>> sorted, ColumnType type) {
        long d = 1;
        for (int i = 1; i < sorted.size(); i++) {
            if (normalize(sorted.get(i), type).compareTo(normalize(sorted.get(i - 1), type)) != 0) {
                d++;
            }
        }
        return d;
    }

    private static List<Histogram.McValue> topMcv(List<Comparable<?>> values) {
        // Group by the canonical String value itself (not normalized) so that
        // McValue stores the string as read from the column. Within a single
        // column all values share one type, so identity grouping is equivalent
        // to normalize-based grouping for counting purposes. Histogram
        // equalitySelectivity parses both sides via histValue/normalizeLiteral.
        Map<Comparable<?>, Long> freq = values.stream().collect(
                Collectors.groupingBy(v -> v, Collectors.counting()));
        return freq.entrySet().stream()
                .sorted(Map.Entry.<Comparable<?>, Long>comparingByValue().reversed())
                .limit(MCV_CAP)
                .map(e -> new Histogram.McValue((String) e.getKey(), e.getValue()))
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
            buckets.add(new Histogram.Bucket((String) lower, (String) upper, size));
            idx += size;
        }
        return buckets;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<Object> normalize(Comparable<?> c, ColumnType type) {
        // read() 产出的值恒为 String,按列类型解析为可比较值。
        String s = (String) c;
        return switch (type) {
            case INTEGER, BIGINT, SMALLINT, DOUBLE, REAL, FLOAT, DECIMAL, NUMERIC,
                 DATE, TIME, TIMESTAMP -> (Comparable<Object>) (Comparable) Double.valueOf(Double.parseDouble(s));
            case BOOLEAN -> (Comparable<Object>) (Comparable) Boolean.valueOf(s);
            default -> (Comparable<Object>) (Comparable) s;
        };
    }
}
