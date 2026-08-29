package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.ValueComparators;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Window function evaluation shared by the Project/Calc operators' RexOver path. Input is
 * materialized into a single columnar root (no per-cell boxing); sorting/peer comparison use {@link
 * ValueComparators}, and only the window aggregate's operand is read boxed (its domain dispatch
 * needs the runtime type). RANGE and ROWS frames both use row positions (no peer expansion).
 * Supports aggregates SUM/AVG/COUNT/MIN/MAX, ROW_NUMBER/RANK/ DENSE_RANK, LEAD/LAG (with optional
 * offset and default) and FIRST_VALUE/LAST_VALUE.
 */
public final class WindowFunctions {

    private WindowFunctions() {}

    public static VectorSchemaRoot materialize(RelNode input, ExecContext ctx) {
        return RowVectors.materializeToRoot(input, ctx);
    }

    /**
     * Computes the window value for every row of {@code rows}. The result is indexed by the
     * ORIGINAL row position, so it lines up with the input rows regardless of how the window's
     * ORDER BY reorders them.
     */
    public static List<Object> computeOver(RexOver over, VectorSchemaRoot rows, ExecContext ctx) {
        RexWindow window = over.getWindow();
        // partition key 求值:列引用直接取列,表达式(RexCall)用 interpreter 求值。
        List<ValueVector> partVectors = new ArrayList<>();
        List<ValueVector> computed = new ArrayList<>();
        for (RexNode key : window.partitionKeys) {
            if (key instanceof RexInputRef ref) {
                partVectors.add(rows.getVector(ref.getIndex()));
            } else {
                ValueVector v = ctx.interpreter().eval(key, rows);
                partVectors.add(v);
                computed.add(v);
            }
        }
        // order key 求值:列引用取列,表达式求值。
        List<ValueVector> orderVectors = new ArrayList<>();
        for (RexFieldCollation fc : window.orderKeys) {
            if (fc.left instanceof RexInputRef ref) {
                orderVectors.add(rows.getVector(ref.getIndex()));
            } else {
                ValueVector v = ctx.interpreter().eval(fc.left, rows);
                orderVectors.add(v);
                computed.add(v);
            }
        }
        try {
            Object[] result = new Object[rows.getRowCount()];
            if (rows.getRowCount() == 0) {
                return Arrays.asList(result);
            }
            int inputCols = rows.getFieldVectors().size();
            // Group row indices by partition-key values (LinkedHashMap keeps first-seen order).
            Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();
            for (int rowIdx = 0; rowIdx < rows.getRowCount(); rowIdx++) {
                List<Object> pk = new ArrayList<>(partVectors.size());
                for (ValueVector v : partVectors) {
                    pk.add(RowVectors.readObject(v, rowIdx));
                }
                partitions.computeIfAbsent(pk, k -> new ArrayList<>()).add(rowIdx);
            }
            SqlKind aggKind = over.getAggOperator().kind;
            for (List<Integer> partition : partitions.values()) {
                List<Integer> orderedRows = new ArrayList<>(partition);
                orderedRows.sort(comparator(window, orderVectors));
                for (int rowPos = 0; rowPos < orderedRows.size(); rowPos++) {
                    int originalRowIdx = orderedRows.get(rowPos);
                    result[originalRowIdx] =
                            computeRow(
                                    over, aggKind, window, orderVectors, orderedRows, rowPos, rows);
                }
            }
            return Arrays.asList(result);
        } finally {
            for (ValueVector v : computed) {
                v.close();
            }
        }
    }

    private static Object computeRow(
            RexOver over,
            SqlKind aggKind,
            RexWindow window,
            List<ValueVector> orderVectors,
            List<Integer> orderedRows,
            int position,
            VectorSchemaRoot rows) {
        switch (aggKind) {
            case ROW_NUMBER:
                return (long) (position + 1);
            case RANK:
            case DENSE_RANK:
                {
                    int rank = 0;
                    int denseRank = 0;
                    for (int p = 0; p <= position; p++) {
                        if (p == 0 || !peers(orderVectors, orderedRows, p - 1, p)) {
                            rank = p + 1;
                            denseRank++;
                        }
                    }
                    return aggKind == SqlKind.RANK ? (long) rank : (long) denseRank;
                }
            case LEAD:
            case LAG:
                {
                    int offset = 1;
                    if (over.getOperands().size() >= 2) {
                        offset = literalInt(over.getOperands().get(1));
                    }
                    Object defaultValue = null;
                    if (over.getOperands().size() >= 3) {
                        defaultValue = literalValue(over.getOperands().get(2));
                    }
                    int targetPos = position + (aggKind == SqlKind.LEAD ? offset : -offset);
                    if (targetPos >= 0 && targetPos < orderedRows.size()) {
                        return operandOf(over, rows, orderedRows.get(targetPos));
                    }
                    return defaultValue;
                }
            case FIRST_VALUE:
            case LAST_VALUE:
                {
                    int[] frame = frameBounds(window, position, orderedRows.size());
                    int boundPos = aggKind == SqlKind.FIRST_VALUE ? frame[0] : frame[1];
                    return operandOf(over, rows, orderedRows.get(boundPos));
                }
            case SUM:
            case AVG:
            case COUNT:
            case MIN:
            case MAX:
                return aggregateOver(over, aggKind, window, orderedRows, position, rows);
            default:
                throw new UnsupportedOperationException(
                        "window function not supported: " + aggKind);
        }
    }

    private static Object aggregateOver(
            RexOver over,
            SqlKind aggKind,
            RexWindow window,
            List<Integer> orderedRows,
            int position,
            VectorSchemaRoot rows) {
        int[] frame = frameBounds(window, position, orderedRows.size());
        boolean isCountStar = over.getOperands().isEmpty();
        boolean isDecimal = over.getType().getSqlTypeName() == SqlTypeName.DECIMAL;
        boolean isFloating = isFloating(over.getType().getSqlTypeName());
        double doubleSum = 0;
        long longSum = 0;
        BigDecimal decimalSum = null;
        long count = 0;
        Object bestValue = null;
        for (int i = frame[0]; i <= frame[1]; i++) {
            Object value = isCountStar ? null : operandOf(over, rows, orderedRows.get(i));
            if (!isCountStar && value == null) {
                continue;
            }
            count++;
            switch (aggKind) {
                case SUM:
                case AVG:
                    if (isDecimal) {
                        decimalSum =
                                (decimalSum == null ? BigDecimal.ZERO : decimalSum)
                                        .add((BigDecimal) value);
                    } else if (isFloating) {
                        doubleSum += ((Number) value).doubleValue();
                    } else {
                        longSum += ((Number) value).longValue();
                    }
                    break;
                case MIN:
                case MAX:
                    if (bestValue == null
                            || (aggKind == SqlKind.MIN
                                    ? compareValues(value, bestValue) < 0
                                    : compareValues(value, bestValue) > 0)) {
                        bestValue = value;
                    }
                    break;
                default:
                    break;
            }
        }
        switch (aggKind) {
            case COUNT:
                return count;
            case SUM:
                return count == 0
                        ? null
                        : isDecimal ? decimalSum : isFloating ? doubleSum : longSum;
            case AVG:
                if (count == 0) {
                    return null;
                }
                if (isDecimal) {
                    return decimalSum.divide(
                            BigDecimal.valueOf(count), java.math.MathContext.DECIMAL128);
                }
                // AVG(整数) 返回 double,避免整数除法截断
                return isFloating ? doubleSum / count : (double) longSum / count;
            case MIN:
            case MAX:
                return bestValue;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Reads the window aggregate's operand as a boxed value (single cell; needed for domain
     * dispatch).
     */
    private static Object operandOf(RexOver over, VectorSchemaRoot rows, int rowIdx) {
        if (over.getOperands().isEmpty()) {
            return null; // COUNT(*)
        }
        RexNode operand = over.getOperands().get(0);
        if (operand instanceof RexInputRef ref) {
            return RowVectors.readObject(rows.getVector(ref.getIndex()), rowIdx);
        }
        return literalValue(operand);
    }

    private static Object literalValue(RexNode node) {
        if (node instanceof RexLiteral literal) {
            Object value = literal.getValue();
            if (value instanceof BigDecimal decimal) {
                return decimal.longValue();
            }
            return value;
        }
        throw new UnsupportedOperationException("window offset/default must be literal: " + node);
    }

    private static int literalInt(RexNode node) {
        Object value = literalValue(node);
        return value instanceof Number n ? n.intValue() : 0;
    }

    /**
     * True when the two rows at positions {@code posA}/{@code posB} are peers (equal on every
     * window order key; nulls treated as equal to nulls only).
     */
    private static boolean peers(
            List<ValueVector> orderVectors, List<Integer> orderedRows, int posA, int posB) {
        for (ValueVector v : orderVectors) {
            int rowA = orderedRows.get(posA);
            int rowB = orderedRows.get(posB);
            boolean nullA = v.isNull(rowA);
            boolean nullB = v.isNull(rowB);
            if (nullA || nullB) {
                if (nullA != nullB) {
                    return false;
                }
            } else if (ValueComparators.compare(v, rowA, v, rowB) != 0) {
                return false;
            }
        }
        return true;
    }

    /** Frame bounds as row positions in the ordered partition (inclusive). */
    private static int[] frameBounds(RexWindow window, int position, int size) {
        int lower = boundOffset(window.getLowerBound(), position, -1, size);
        int upper = boundOffset(window.getUpperBound(), position, 1, size);
        return new int[] {Math.max(0, lower), Math.min(size - 1, upper)};
    }

    private static int boundOffset(RexWindowBound bound, int position, int sign, int size) {
        if (bound.isUnboundedPreceding()) {
            return 0;
        }
        if (bound.isUnboundedFollowing()) {
            return size - 1;
        }
        if (bound.isCurrentRow()) {
            return position;
        }
        int offset = literalInt(bound.getOffset());
        return position + sign * offset; // PRECEDING: sign=-1, FOLLOWING: sign=+1
    }

    /**
     * Orders row indices by the window's ORDER BY keys; row index is only a stable tiebreaker among
     * peers (preserves input order).
     */
    private static Comparator<Integer> comparator(
            RexWindow window, List<ValueVector> orderVectors) {
        Comparator<Integer> comparator = null;
        for (int k = 0; k < window.orderKeys.size(); k++) {
            RexFieldCollation fieldCollation = window.orderKeys.get(k);
            ValueVector v = orderVectors.get(k);
            boolean descending =
                    fieldCollation.getDirection() == RelFieldCollation.Direction.DESCENDING
                            || fieldCollation.getDirection()
                                    == RelFieldCollation.Direction.STRICTLY_DESCENDING;
            Comparator<Integer> fieldComparator =
                    (a, b) -> {
                        boolean nullA = v.isNull(a);
                        boolean nullB = v.isNull(b);
                        if (nullA || nullB) {
                            if (nullA && nullB) {
                                return 0;
                            }
                            return nullA ? 1 : -1; // nulls last
                        }
                        return ValueComparators.compare(v, a, v, b);
                    };
            if (descending) {
                fieldComparator = fieldComparator.reversed();
            }
            comparator =
                    comparator == null
                            ? fieldComparator
                            : comparator.thenComparing(fieldComparator);
        }
        return comparator == null
                ? Comparator.comparingInt(i -> i)
                : comparator.thenComparingInt(i -> i);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareValues(Object left, Object right) {
        return ((Comparable) left).compareTo(right);
    }

    private static boolean isFloating(SqlTypeName typeName) {
        return typeName == SqlTypeName.DOUBLE
                || typeName == SqlTypeName.FLOAT
                || typeName == SqlTypeName.REAL;
    }
}
