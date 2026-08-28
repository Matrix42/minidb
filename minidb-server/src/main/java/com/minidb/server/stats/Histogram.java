package com.minidb.server.stats;

import com.minidb.storage.common.ColumnType;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 * Single-column equi-depth histogram with top-k MCV values.
 *
 * <p>Bucket boundaries and MCV values are stored as canonical {@link String}s
 * (e.g. {@code "1"}, {@code "3.14"}, {@code "a"}) rather than raw
 * {@link Comparable} objects so that the histogram can be (de)serialized to
 * JSON. The column's {@link ColumnType} tells {@link #histValue} how to parse
 * those strings back into comparable values for selectivity estimation.
 *
 * <p>Statistics are persisted as JSON (Jackson) via {@link TableStats}; Java
 * serialization is no longer used.
 */
public record Histogram(
        ColumnType type,
        List<Bucket> buckets,
        List<McValue> mcv,
        long distinctCount,
        long nullCount,
        long totalRows) {

    public static final double DEFAULT_SELECTIVITY = 0.33;

    public record Bucket(String lower, String upper, long rowCount) {
    }

    public record McValue(String value, long frequency) {
    }

    public Histogram {
        buckets = List.copyOf(buckets);
        mcv = List.copyOf(mcv);
    }

    public static Histogram empty(ColumnType type) {
        return new Histogram(type, List.of(), List.of(), 0, 0, 0);
    }

    public double selectivity(RexNode condition, long inputRows) {
        if (condition == null) {
            return DEFAULT_SELECTIVITY;
        }
        if (condition instanceof RexCall call) {
            return selectivityCall(call, inputRows);
        }
        // RexInputRef or RexLiteral alone isn't a predicate we model.
        return DEFAULT_SELECTIVITY;
    }

    private double selectivityCall(RexCall call, long inputRows) {
        SqlKind kind = call.getKind();
        return switch (kind) {
            case AND -> {
                double prod = 1.0;
                for (RexNode op : call.getOperands()) {
                    prod *= selectivity(op, inputRows);
                }
                yield prod;
            }
            case OR -> {
                // inclusion-exclusion over operands (pairwise independent assumption)
                double sel = 0.0;
                for (RexNode op : call.getOperands()) {
                    sel = sel + selectivity(op, inputRows) - sel * selectivity(op, inputRows);
                }
                yield sel;
            }
            case NOT -> 1.0 - selectivity(call.getOperands().get(0), inputRows);
            case EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL,
                 GREATER_THAN, GREATER_THAN_OR_EQUAL ->
                    comparisonSelectivity(call, kind);
            default -> DEFAULT_SELECTIVITY;
        };
    }

    private double comparisonSelectivity(RexCall call, SqlKind kind) {
        List<RexNode> ops = call.getOperands();
        if (ops.size() != 2) {
            return DEFAULT_SELECTIVITY;
        }
        Integer colIndex = inputRefIndex(ops.get(0));
        Comparable<?> literal = rexLiteral(ops.get(1));
        if (colIndex == null && literal == null) {
            // try swapped operand order (literal op col)
            colIndex = inputRefIndex(ops.get(1));
            literal = rexLiteral(ops.get(0));
        }
        if (colIndex == null || literal == null) {
            return DEFAULT_SELECTIVITY;
        }
        if (kind == SqlKind.EQUALS) {
            return equalitySelectivity(literal);
        }
        if (kind == SqlKind.NOT_EQUALS) {
            return 1.0 - equalitySelectivity(literal);
        }
        return rangeSelectivity(literal, kind);
    }

    private double equalitySelectivity(Comparable<?> value) {
        if (totalRows == 0) {
            return DEFAULT_SELECTIVITY;
        }
        for (McValue m : mcv) {
            if (Objects.equals(histValue(m.value()), normalizeLiteral(value))) {
                return (double) m.frequency() / totalRows;
            }
        }
        if (distinctCount == 0) {
            return DEFAULT_SELECTIVITY;
        }
        return 1.0 / distinctCount;
    }

    private double rangeSelectivity(Comparable<?> literal, SqlKind kind) {
        if (totalRows == 0 || buckets.isEmpty()) {
            return DEFAULT_SELECTIVITY;
        }
        // Guard against cross-column type mismatch: the histogram may be for a
        // numeric column while the literal is a String (or vice versa) when a
        // compound filter's condition is evaluated against a single histogram.
        // EXPLAIN is read-only and must not throw ClassCastException.
        Comparable<?> sampleBound = histValue(buckets.get(0).upper());
        Comparable<?> normalizedLiteral = normalizeLiteral(literal);
        if (!typesCompatible(sampleBound, normalizedLiteral)) {
            return DEFAULT_SELECTIVITY;
        }
        long matched = 0;
        boolean lessFamily = kind == SqlKind.LESS_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL;
        for (Bucket b : buckets) {
            if (lessFamily) {
                // whole bucket below literal: upper < literal
                if (histValue(b.upper()).compareTo(normalizeLiteral(literal)) < 0) {
                    matched += b.rowCount();
                } else if (histValue(b.lower()).compareTo(normalizeLiteral(literal)) < 0) {
                    // boundary bucket straddles literal: interpolate by position.
                    // frac = how far into the bucket the literal is (literal - lower),
                    // consistent with spanSize = upper - lower.
                    long span = spanSize(b);
                    if (span > 0) {
                        long frac = numericDelta(histValue(b.lower()), normalizeLiteral(literal));
                        matched += (long) (b.rowCount() * ((double) frac / span));
                    }
                }
            } else { // GREATER_THAN / GREATER_THAN_OR_EQUAL
                // whole bucket above literal: lower > literal
                if (histValue(b.lower()).compareTo(normalizeLiteral(literal)) > 0) {
                    matched += b.rowCount();
                } else if (histValue(b.upper()).compareTo(normalizeLiteral(literal)) > 0) {
                    // frac = how far above the literal the upper is (upper - literal),
                    // consistent with spanSize = upper - lower.
                    long span = spanSize(b);
                    if (span > 0) {
                        long frac = numericDelta(normalizeLiteral(literal), histValue(b.upper()));
                        matched += (long) (b.rowCount() * ((double) frac / span));
                    }
                }
            }
        }
        double sel = (double) matched / totalRows;
        return Math.min(1.0, Math.max(0.0, sel));
    }

    private long spanSize(Bucket b) {
        Comparable<?> lo = histValue(b.lower());
        Comparable<?> hi = histValue(b.upper());
        if (lo instanceof Number n1 && hi instanceof Number n2) {
            long d = n2.longValue() - n1.longValue();
            return d <= 0 ? 1 : d;
        }
        return 1;
    }

    private static long numericDelta(Comparable<Object> a, Comparable<Object> b) {
        if (a instanceof Number na && b instanceof Number nb) {
            return Math.round(nb.doubleValue() - na.doubleValue());
        }
        // non-numeric (VARCHAR etc.): fall back to compareTo sign, span is 1 so frac is ±1/0
        return Integer.signum(b.compareTo(a));
    }

    private static Integer inputRefIndex(RexNode node) {
        if (node instanceof RexInputRef ref) {
            return ref.getIndex();
        }
        return null;
    }

    private static Comparable<?> rexLiteral(RexNode node) {
        if (node instanceof RexLiteral lit) {
            Object v = lit.getValue();
            if (v instanceof Comparable<?> c) {
                // BigDecimal (and all numeric literals) flow through here;
                // normalizeLiteral normalizes BigDecimal/Integer/Long to double.
                return c;
            }
        }
        return null;
    }

    /** 直方图里存的规范字符串 → 可比较值,按列类型解析。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Comparable<Object> histValue(String s) {
        return switch (type) {
            case INTEGER, BIGINT, SMALLINT, DOUBLE, REAL, FLOAT, DECIMAL, NUMERIC,
                 DATE, TIME, TIMESTAMP -> (Comparable<Object>) (Comparable) Double.valueOf(s);
            case VARCHAR, CHAR, NCHAR, NVARCHAR -> (Comparable<Object>) (Comparable) s;
            case BOOLEAN -> (Comparable<Object>) (Comparable) Boolean.valueOf(s);
            default -> (Comparable<Object>) (Comparable) s;
        };
    }

    /** 字面量(RexLiteral 里的 Comparable)→ 可比较值,数值统一归一化到 Double。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<Object> normalizeLiteral(Comparable<?> c) {
        if (c instanceof BigDecimal bd) {
            return (Comparable<Object>) (Comparable) bd.doubleValue();
        }
        if (c instanceof Integer i) {
            return (Comparable<Object>) (Comparable) i.doubleValue();
        }
        if (c instanceof Long l) {
            return (Comparable<Object>) (Comparable) l.doubleValue();
        }
        if (c instanceof Float f) {
            return (Comparable<Object>) (Comparable) f.doubleValue();
        }
        return (Comparable<Object>) c;
    }

    /**
     * Returns true only if the two normalized values can be safely compared.
     * Both must be {@link Number} or neither must be. For the non-numeric case
     * (e.g. String vs String) the kinds must match so that VARCHAR vs Boolean
     * edge cases return false rather than throwing.
     */
    private static boolean typesCompatible(Comparable<?> a, Comparable<?> b) {
        boolean aNum = a instanceof Number;
        boolean bNum = b instanceof Number;
        if (aNum != bNum) {
            return false;
        }
        if (aNum) {
            return true;
        }
        // neither is Number: require same concrete kind to avoid odd edge cases
        return a.getClass() == b.getClass();
    }
}
