package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
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
 * Window function evaluation shared by the Project operator's RexOver path.
 * Rows are normalized {@code Object[]}; the window's partition keys and order
 * keys are resolved against the input row type. RANGE and ROWS frames both use
 * row positions (no peer expansion). Supports aggregates SUM/AVG/COUNT/MIN/MAX,
 * ROW_NUMBER/RANK/DENSE_RANK, LEAD/LAG (with optional offset and default) and
 * FIRST_VALUE/LAST_VALUE.
 */
public final class WindowFunctions {

    private WindowFunctions() {
    }

    public static List<Object[]> materialize(RelNode input, ExecContext ctx) {
        List<Object[]> rows = new ArrayList<>();
        BatchIterator it = ((MiniDbRel) input).execute(ctx);
        try {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                for (int r = 0; r < batch.getRowCount(); r++) {
                    Object[] row = new Object[batch.getFieldVectors().size()];
                    for (int c = 0; c < row.length; c++) {
                        row[c] = readObject(batch.getVector(c), r);
                    }
                    rows.add(row);
                }
            }
        } finally {
            it.close();
        }
        return rows;
    }

    /** Computes the window value for every row of {@code rows}. */
    public static List<Object> computeOver(RexOver over, List<Object[]> rows) {
        RexWindow win = over.getWindow();
        List<Integer> partKeys = new ArrayList<>();
        for (RexNode k : win.partitionKeys) {
            partKeys.add(((RexInputRef) k).getIndex());
        }
        List<Object> out = new ArrayList<>(rows.size());
        if (rows.isEmpty()) {
            return out;
        }
        int cols = rows.get(0).length;
        Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            List<Object> key = new ArrayList<>(partKeys.size());
            for (int k : partKeys) {
                key.add(rows.get(i)[k]);
            }
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
        }
        SqlKind kind = over.getAggOperator().kind;
        for (List<Integer> part : partitions.values()) {
            List<Integer> ordered = new ArrayList<>(part);
            ordered.sort(comparator(win, rows));
            int[] vals = new int[ordered.size()];
            for (int p = 0; p < ordered.size(); p++) {
                vals[p] = ordered.get(p);
            }
            for (int p = 0; p < vals.length; p++) {
                out.add(ordered.get(p), computeRow(over, kind, win, ordered, p, rows, cols));
            }
        }
        return out;
    }

    private static Object computeRow(RexOver over, SqlKind kind, RexWindow win,
                                     List<Integer> ordered, int pos,
                                     List<Object[]> rows, int cols) {
        switch (kind) {
            case ROW_NUMBER:
                return (long) (pos + 1);
            case RANK:
            case DENSE_RANK: {
                int rank = 0;
                int dense = 0;
                for (int p = 0; p <= pos; p++) {
                    if (p == 0 || !peers(win, rows, ordered, p - 1, p)) {
                        rank = p + 1;
                        dense++;
                    }
                }
                return kind == SqlKind.RANK ? (long) rank : (long) dense;
            }
            case LEAD:
            case LAG: {
                int offset = 1;
                if (over.getOperands().size() >= 2) {
                    offset = literalInt(over.getOperands().get(1));
                }
                Object def = null;
                if (over.getOperands().size() >= 3) {
                    def = literalValue(over.getOperands().get(2));
                }
                int q = pos + (kind == SqlKind.LEAD ? offset : -offset);
                if (q >= 0 && q < ordered.size()) {
                    return operandOf(over, rows.get(ordered.get(q)), cols);
                }
                return def;
            }
            case FIRST_VALUE:
            case LAST_VALUE: {
                int[] f = frameBounds(win, pos, ordered.size());
                Object[] row = rows.get(ordered.get(kind == SqlKind.FIRST_VALUE ? f[0] : f[1]));
                return operandOf(over, row, cols);
            }
            case SUM:
            case AVG:
            case COUNT:
            case MIN:
            case MAX:
                return aggregateOver(over, kind, win, ordered, pos, rows, cols);
            default:
                throw new UnsupportedOperationException(
                        "window function not supported: " + kind);
        }
    }

    private static Object aggregateOver(RexOver over, SqlKind kind, RexWindow win,
                                        List<Integer> ordered, int pos,
                                        List<Object[]> rows, int cols) {
        int[] f = frameBounds(win, pos, ordered.size());
        boolean countStar = over.getOperands().isEmpty();
        boolean floating = isFloating(over.getType().getSqlTypeName());
        double dsum = 0;
        long lsum = 0;
        long cnt = 0;
        Object best = null;
        for (int i = f[0]; i <= f[1]; i++) {
            Object v = countStar ? null : operandOf(over, rows.get(ordered.get(i)), cols);
            if (!countStar && v == null) {
                continue;
            }
            cnt++;
            switch (kind) {
                case SUM:
                case AVG:
                    if (floating) {
                        dsum += ((Number) v).doubleValue();
                    } else {
                        lsum += ((Number) v).longValue();
                    }
                    break;
                case MIN:
                case MAX:
                    if (best == null || (kind == SqlKind.MIN
                            ? compareValues(v, best) < 0
                            : compareValues(v, best) > 0)) {
                        best = v;
                    }
                    break;
                default:
                    break;
            }
        }
        switch (kind) {
            case COUNT:
                return cnt;
            case SUM:
                return cnt == 0 ? null : floating ? dsum : lsum;
            case AVG:
                return cnt == 0 ? null : floating ? dsum / cnt : lsum / cnt;
            case MIN:
            case MAX:
                return best;
            default:
                throw new IllegalStateException();
        }
    }

    private static Object operandOf(RexOver over, Object[] row, int cols) {
        if (over.getOperands().isEmpty()) {
            return null; // COUNT(*)
        }
        RexNode op = over.getOperands().get(0);
        if (op instanceof RexInputRef ref) {
            return row[ref.getIndex()];
        }
        return literalValue(op);
    }

    private static Object literalValue(RexNode node) {
        if (node instanceof RexLiteral lit) {
            Object v = lit.getValue();
            if (v instanceof BigDecimal bd) {
                return bd.longValue();
            }
            return v;
        }
        throw new UnsupportedOperationException(
                "window offset/default must be literal: " + node);
    }

    private static int literalInt(RexNode node) {
        Object v = literalValue(node);
        return v instanceof Number n ? n.intValue() : 0;
    }

    private static boolean peers(RexWindow win, List<Object[]> rows,
                                 List<Integer> ordered, int a, int b) {
        for (RexFieldCollation fc : win.orderKeys) {
            Object x = rows.get(ordered.get(a))[((RexInputRef) fc.left).getIndex()];
            Object y = rows.get(ordered.get(b))[((RexInputRef) fc.left).getIndex()];
            if (x == null || y == null) {
                if (x != y) {
                    return false;
                }
            } else if (compareValues(x, y) != 0) {
                return false;
            }
        }
        return true;
    }

    /** Frame bounds as row positions in the ordered partition (inclusive). */
    private static int[] frameBounds(RexWindow win, int pos, int size) {
        int lo = boundOffset(win.getLowerBound(), pos, -1, size);
        int hi = boundOffset(win.getUpperBound(), pos, 1, size);
        return new int[]{Math.max(0, lo), Math.min(size - 1, hi)};
    }

    private static int boundOffset(RexWindowBound bound, int pos, int sign, int size) {
        if (bound.isUnboundedPreceding()) {
            return 0;
        }
        if (bound.isUnboundedFollowing()) {
            return size - 1;
        }
        if (bound.isCurrentRow()) {
            return pos;
        }
        int offset = literalInt(bound.getOffset());
        return pos + sign * offset; // PRECEDING: sign=-1, FOLLOWING: sign=+1
    }

    private static Comparator<Integer> comparator(RexWindow win,
                                                  List<Object[]> rows) {
        Comparator<Integer> cmp = Comparator.comparingInt(i -> i);
        for (RexFieldCollation fc : win.orderKeys) {
            int field = ((RexInputRef) fc.left).getIndex();
            boolean desc = fc.getDirection() == RelFieldCollation.Direction.DESCENDING
                    || fc.getDirection() == RelFieldCollation.Direction.STRICTLY_DESCENDING;
            Comparator<Integer> one = (a, b) -> {
                Object x = rows.get(a)[field];
                Object y = rows.get(b)[field];
                if (x == null && y == null) {
                    return 0;
                }
                if (x == null) {
                    return 1; // nulls last
                }
                if (y == null) {
                    return -1;
                }
                return compareValues(x, y);
            };
            if (desc) {
                one = one.reversed();
            }
            cmp = cmp.thenComparing(one);
        }
        return cmp;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareValues(Object a, Object b) {
        return ((Comparable) a).compareTo(b);
    }

    private static boolean isFloating(SqlTypeName t) {
        return t == SqlTypeName.DOUBLE || t == SqlTypeName.FLOAT
                || t == SqlTypeName.REAL || t == SqlTypeName.DECIMAL;
    }

    private static Object readObject(ValueVector v, int row) {
        if (v.isNull(row)) {
            return null;
        }
        if (v instanceof IntVector iv) {
            return iv.get(row);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(row);
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (v instanceof VarCharVector vv) {
            return new String(vv.get(row), StandardCharsets.UTF_8);
        }
        if (v instanceof BitVector bv) {
            return bv.get(row);
        }
        if (v instanceof DateDayVector dv) {
            return dv.get(row);
        }
        if (v instanceof TimeStampMilliVector tv) {
            return tv.get(row);
        }
        throw new UnsupportedOperationException(
                "cannot window column type: " + v.getMinorType());
    }
}
