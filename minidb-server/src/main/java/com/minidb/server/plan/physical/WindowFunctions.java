package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

/**
 * Window function evaluation shared by the Project/Calc operators' RexOver
 * path. Rows are normalized {@code Object[]}; the window's partition keys and
 * order keys are resolved against the input row type. RANGE and ROWS frames
 * both use row positions (no peer expansion). Supports aggregates
 * SUM/AVG/COUNT/MIN/MAX, ROW_NUMBER/RANK/DENSE_RANK, LEAD/LAG (with optional
 * offset and default) and FIRST_VALUE/LAST_VALUE.
 */
public final class WindowFunctions {

    private WindowFunctions() {
    }

    public static List<Object[]> materialize(RelNode input, ExecContext ctx) {
        List<Object[]> rows = new ArrayList<>();
        BatchIterator iterator = ((MiniDbRel) input).execute(ctx);
        try {
            while (iterator.hasNext()) {
                VectorSchemaRoot batch = iterator.next();
                for (int rowIdx = 0; rowIdx < batch.getRowCount(); rowIdx++) {
                    Object[] row = new Object[batch.getFieldVectors().size()];
                    for (int colIdx = 0; colIdx < row.length; colIdx++) {
                        row[colIdx] = RowVectors.readObject(batch.getVector(colIdx), rowIdx);
                    }
                    rows.add(row);
                }
            }
        } finally {
            iterator.close();
        }
        return rows;
    }

    /** Computes the window value for every row of {@code rows}.
     *  The result is indexed by the ORIGINAL row position, so it lines up with
     *  the input rows regardless of how the window's ORDER BY reorders them. */
    public static List<Object> computeOver(RexOver over, List<Object[]> rows) {
        RexWindow window = over.getWindow();
        List<Integer> partitionKeyCols = new ArrayList<>();
        for (RexNode key : window.partitionKeys) {
            partitionKeyCols.add(((RexInputRef) key).getIndex());
        }
        Object[] result = new Object[rows.size()];
        if (rows.isEmpty()) {
            return Arrays.asList(result);
        }
        int inputCols = rows.get(0).length;
        // Group row indices by partition-key values (LinkedHashMap keeps first-seen order).
        Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();
        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            List<Object> partitionKey = new ArrayList<>(partitionKeyCols.size());
            for (int keyCol : partitionKeyCols) {
                partitionKey.add(rows.get(rowIdx)[keyCol]);
            }
            partitions.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(rowIdx);
        }
        SqlKind aggKind = over.getAggOperator().kind;
        for (List<Integer> partition : partitions.values()) {
            List<Integer> orderedRows = new ArrayList<>(partition);
            orderedRows.sort(comparator(window, rows));
            for (int rowPos = 0; rowPos < orderedRows.size(); rowPos++) {
                int originalRowIdx = orderedRows.get(rowPos);
                result[originalRowIdx] = computeRow(over, aggKind, window, orderedRows, rowPos, rows, inputCols);
            }
        }
        return Arrays.asList(result);
    }

    private static Object computeRow(RexOver over, SqlKind aggKind, RexWindow window,
                                     List<Integer> orderedRows, int position,
                                     List<Object[]> rows, int inputCols) {
        switch (aggKind) {
            case ROW_NUMBER:
                return (long) (position + 1);
            case RANK:
            case DENSE_RANK: {
                int rank = 0;
                int denseRank = 0;
                for (int p = 0; p <= position; p++) {
                    if (p == 0 || !peers(window, rows, orderedRows, p - 1, p)) {
                        rank = p + 1;
                        denseRank++;
                    }
                }
                return aggKind == SqlKind.RANK ? (long) rank : (long) denseRank;
            }
            case LEAD:
            case LAG: {
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
                    return operandOf(over, rows.get(orderedRows.get(targetPos)), inputCols);
                }
                return defaultValue;
            }
            case FIRST_VALUE:
            case LAST_VALUE: {
                int[] frame = frameBounds(window, position, orderedRows.size());
                int boundPos = aggKind == SqlKind.FIRST_VALUE ? frame[0] : frame[1];
                return operandOf(over, rows.get(orderedRows.get(boundPos)), inputCols);
            }
            case SUM:
            case AVG:
            case COUNT:
            case MIN:
            case MAX:
                return aggregateOver(over, aggKind, window, orderedRows, position, rows, inputCols);
            default:
                throw new UnsupportedOperationException(
                        "window function not supported: " + aggKind);
        }
    }

    private static Object aggregateOver(RexOver over, SqlKind aggKind, RexWindow window,
                                        List<Integer> orderedRows, int position,
                                        List<Object[]> rows, int inputCols) {
        int[] frame = frameBounds(window, position, orderedRows.size());
        boolean isCountStar = over.getOperands().isEmpty();
        boolean isFloating = isFloating(over.getType().getSqlTypeName());
        double doubleSum = 0;
        long longSum = 0;
        long count = 0;
        Object bestValue = null;
        for (int i = frame[0]; i <= frame[1]; i++) {
            Object value = isCountStar ? null : operandOf(over, rows.get(orderedRows.get(i)), inputCols);
            if (!isCountStar && value == null) {
                continue;
            }
            count++;
            switch (aggKind) {
                case SUM:
                case AVG:
                    if (isFloating) {
                        doubleSum += ((Number) value).doubleValue();
                    } else {
                        longSum += ((Number) value).longValue();
                    }
                    break;
                case MIN:
                case MAX:
                    if (bestValue == null || (aggKind == SqlKind.MIN
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
                return count == 0 ? null : isFloating ? doubleSum : longSum;
            case AVG:
                return count == 0 ? null : isFloating ? doubleSum / count : longSum / count;
            case MIN:
            case MAX:
                return bestValue;
            default:
                throw new IllegalStateException();
        }
    }

    private static Object operandOf(RexOver over, Object[] row, int inputCols) {
        if (over.getOperands().isEmpty()) {
            return null; // COUNT(*)
        }
        RexNode operand = over.getOperands().get(0);
        if (operand instanceof RexInputRef ref) {
            return row[ref.getIndex()];
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
        throw new UnsupportedOperationException(
                "window offset/default must be literal: " + node);
    }

    private static int literalInt(RexNode node) {
        Object value = literalValue(node);
        return value instanceof Number n ? n.intValue() : 0;
    }

    /** True when the two rows at positions {@code posA}/{@code posB} are peers
     *  (equal on every window order key; nulls treated as equal to nulls only). */
    private static boolean peers(RexWindow window, List<Object[]> rows,
                                 List<Integer> orderedRows, int posA, int posB) {
        for (RexFieldCollation fieldCollation : window.orderKeys) {
            Object leftVal = rows.get(orderedRows.get(posA))[((RexInputRef) fieldCollation.left).getIndex()];
            Object rightVal = rows.get(orderedRows.get(posB))[((RexInputRef) fieldCollation.left).getIndex()];
            if (leftVal == null || rightVal == null) {
                if (leftVal != rightVal) {
                    return false;
                }
            } else if (compareValues(leftVal, rightVal) != 0) {
                return false;
            }
        }
        return true;
    }

    /** Frame bounds as row positions in the ordered partition (inclusive). */
    private static int[] frameBounds(RexWindow window, int position, int size) {
        int lower = boundOffset(window.getLowerBound(), position, -1, size);
        int upper = boundOffset(window.getUpperBound(), position, 1, size);
        return new int[]{Math.max(0, lower), Math.min(size - 1, upper)};
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

    /** Orders row indices by the window's ORDER BY keys; row index is only a
     *  stable tiebreaker among peers (preserves input order). */
    private static Comparator<Integer> comparator(RexWindow window, List<Object[]> rows) {
        Comparator<Integer> comparator = null;
        for (RexFieldCollation fieldCollation : window.orderKeys) {
            int fieldIndex = ((RexInputRef) fieldCollation.left).getIndex();
            boolean descending = fieldCollation.getDirection() == RelFieldCollation.Direction.DESCENDING
                    || fieldCollation.getDirection() == RelFieldCollation.Direction.STRICTLY_DESCENDING;
            Comparator<Integer> fieldComparator = (a, b) -> {
                Object leftVal = rows.get(a)[fieldIndex];
                Object rightVal = rows.get(b)[fieldIndex];
                if (leftVal == null && rightVal == null) {
                    return 0;
                }
                if (leftVal == null) {
                    return 1; // nulls last
                }
                if (rightVal == null) {
                    return -1;
                }
                return compareValues(leftVal, rightVal);
            };
            if (descending) {
                fieldComparator = fieldComparator.reversed();
            }
            comparator = comparator == null ? fieldComparator : comparator.thenComparing(fieldComparator);
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
        return typeName == SqlTypeName.DOUBLE || typeName == SqlTypeName.FLOAT
                || typeName == SqlTypeName.REAL || typeName == SqlTypeName.DECIMAL;
    }

}
