package com.minidb.server.stats;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public final class Histogram implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final double DEFAULT_SELECTIVITY = 0.33;

    public record Bucket(Comparable<?> lower, Comparable<?> upper, long rowCount)
            implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public record McValue(Comparable<?> value, long frequency)
            implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private final List<Bucket> buckets;
    private final List<McValue> mcv;
    private final long distinctCount;
    private final long nullCount;
    private final long totalRows;

    public Histogram(List<Bucket> buckets, List<McValue> mcv,
                     long distinctCount, long nullCount, long totalRows) {
        this.buckets = List.copyOf(buckets);
        this.mcv = List.copyOf(mcv);
        this.distinctCount = distinctCount;
        this.nullCount = nullCount;
        this.totalRows = totalRows;
    }

    public static Histogram empty() {
        return new Histogram(List.of(), List.of(), 0, 0, 0);
    }

    public long totalRows() {
        return totalRows;
    }

    public long distinctCount() {
        return distinctCount;
    }

    public long nullCount() {
        return nullCount;
    }

    public List<Bucket> buckets() {
        return buckets;
    }

    public List<McValue> mcv() {
        return mcv;
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
        Comparable<?> literal = literalValue(ops.get(1));
        if (colIndex == null && literal == null) {
            // try swapped operand order (literal op col)
            colIndex = inputRefIndex(ops.get(1));
            literal = literalValue(ops.get(0));
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
            if (Objects.equals(compareValue(m.value()), compareValue(value))) {
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
        long matched = 0;
        boolean lessFamily = kind == SqlKind.LESS_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL;
        for (Bucket b : buckets) {
            if (lessFamily) {
                // whole bucket below literal: upper < literal
                if (compareValue(b.upper()).compareTo(compareValue(literal)) < 0) {
                    matched += b.rowCount();
                } else if (compareValue(b.lower()).compareTo(compareValue(literal)) < 0) {
                    // boundary bucket straddles literal: interpolate by position
                    long span = spanSize(b);
                    if (span > 0) {
                        long frac = compareValue(literal).compareTo(compareValue(b.lower()));
                        matched += (long) (b.rowCount() * ((double) frac / span));
                    }
                }
            } else { // GREATER_THAN / GREATER_THAN_OR_EQUAL
                // whole bucket above literal: lower > literal
                if (compareValue(b.lower()).compareTo(compareValue(literal)) > 0) {
                    matched += b.rowCount();
                } else if (compareValue(b.upper()).compareTo(compareValue(literal)) > 0) {
                    long span = spanSize(b);
                    if (span > 0) {
                        long frac = compareValue(b.upper()).compareTo(compareValue(literal));
                        matched += (long) (b.rowCount() * ((double) frac / span));
                    }
                }
            }
        }
        double sel = (double) matched / totalRows;
        return Math.min(1.0, Math.max(0.0, sel));
    }

    private long spanSize(Bucket b) {
        Comparable<?> lo = compareValue(b.lower());
        Comparable<?> hi = compareValue(b.upper());
        if (lo instanceof Number n1 && hi instanceof Number n2) {
            long d = n2.longValue() - n1.longValue();
            return d <= 0 ? 1 : d;
        }
        return 1;
    }

    private static Integer inputRefIndex(RexNode node) {
        if (node instanceof RexInputRef ref) {
            return ref.getIndex();
        }
        return null;
    }

    private static Comparable<?> literalValue(RexNode node) {
        if (node instanceof RexLiteral lit) {
            Object v = lit.getValue();
            if (v instanceof Comparable<?> c) {
                return c;
            }
            // numbers come back as BigDecimal
            if (v instanceof java.math.BigDecimal bd) {
                return bd;
            }
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<Object> compareValue(Comparable<?> c) {
        // Normalize: BigDecimal/Integer/Long -> double for cross-type compare
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
