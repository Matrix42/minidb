package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

/**
 * Join with three strategies:
 * <ul>
 *   <li>HASH — equi-join only; builds a hash table on the left input keyed by
 *       the equi columns, probes with the right input.</li>
 *   <li>SORT_MERGE — equi-join only; sorts both sides by the key columns and
 *       merges equal-key groups.</li>
 *   <li>NESTED_LOOP — any condition; evaluates the full RexNode condition per
 *       candidate pair (correctness over speed).</li>
 * </ul>
 * AUTO picks HASH for pure equi-joins (all AND leaves are {@code lcol = rcol}),
 * otherwise NESTED_LOOP. NULL keys never match in equi-joins (SQL semantics).
 * Outer joins preserve unmatched rows with NULLs on the other side. Rows are
 * normalized to {@code Object[]}; output is a single batch.
 */
public class MiniDbJoin extends Join implements MiniDbRel {

    public enum Strategy { AUTO, HASH, SORT_MERGE, NESTED_LOOP }

    private final Strategy strategy;

    public MiniDbJoin(RelOptCluster cluster, RelTraitSet traitSet,
                      RelNode left, RelNode right, RexNode condition,
                      JoinRelType joinType, Strategy strategy) {
        super(cluster, traitSet, left, right, condition, Set.of(), joinType);
        this.strategy = strategy;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType, strategy);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        JoinRelType type = getJoinType();
        if (type == JoinRelType.SEMI || type == JoinRelType.ANTI) {
            throw new UnsupportedOperationException("semi/anti join not supported");
        }
        List<Object[]> leftRows = materialize(getLeft(), ctx);
        List<Object[]> rightRows = materialize(getRight(), ctx);
        JoinInfo info = analyzeCondition();
        Strategy s = strategy;
        if (s == Strategy.AUTO) {
            s = info.isEqui() ? Strategy.HASH : Strategy.NESTED_LOOP;
        }
        List<Object[]> out = switch (s) {
            case HASH -> hashJoin(leftRows, rightRows, info, type);
            case SORT_MERGE -> sortMergeJoin(leftRows, rightRows, info, type);
            case NESTED_LOOP -> nestedLoopJoin(leftRows, rightRows, ctx);
            default -> throw new IllegalStateException("unhandled strategy " + s);
        };
        VectorSchemaRoot root = buildOutput(out, ctx);

        boolean[] done = {false};
        return new BatchIterator() {
            @Override
            public boolean hasNext() {
                return !done[0];
            }

            @Override
            public VectorSchemaRoot next() {
                done[0] = true;
                return root;
            }

            @Override
            public void close() {
                root.close();
            }
        };
    }

    // ---- materialization ----

    private List<Object[]> materialize(RelNode input, ExecContext ctx) {
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
                "cannot join column type: " + v.getMinorType());
    }

    // ---- strategies ----

    private List<Object[]> hashJoin(List<Object[]> left, List<Object[]> right,
                                    JoinInfo info, JoinRelType type) {
        List<Integer> lk = info.leftKeys;
        List<Integer> rk = info.rightKeys;
        Map<List<Object>, List<Integer>> hash = new HashMap<>();
        for (int i = 0; i < left.size(); i++) {
            if (containsNull(left.get(i), lk)) {
                continue;
            }
            hash.computeIfAbsent(keyOf(left.get(i), lk), k -> new ArrayList<>()).add(i);
        }
        boolean leftPreserved = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean rightPreserved = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] leftMatched = new boolean[left.size()];
        Object[] nullLeft = new Object[left.get(0).length];
        Object[] nullRight = new Object[right.get(0).length];
        List<Object[]> out = new ArrayList<>();
        for (int j = 0; j < right.size(); j++) {
            List<Integer> matches;
            if (containsNull(right.get(j), rk)) {
                matches = null;
            } else {
                matches = hash.get(keyOf(right.get(j), rk));
            }
            if (matches != null) {
                for (int i : matches) {
                    out.add(concat(left.get(i), right.get(j)));
                    leftMatched[i] = true;
                }
            } else if (rightPreserved) {
                out.add(concat(nullLeft, right.get(j)));
            }
        }
        if (leftPreserved) {
            for (int i = 0; i < left.size(); i++) {
                if (!leftMatched[i]) {
                    out.add(concat(left.get(i), nullRight));
                }
            }
        }
        return out;
    }

    private List<Object[]> sortMergeJoin(List<Object[]> left, List<Object[]> right,
                                         JoinInfo info, JoinRelType type) {
        List<Integer> lk = info.leftKeys;
        List<Integer> rk = info.rightKeys;
        List<Integer> lorder = sortedIndices(left, lk);
        List<Integer> rorder = sortedIndices(right, rk);
        boolean leftPreserved = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean rightPreserved = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] leftMatched = new boolean[left.size()];
        boolean[] rightMatched = new boolean[right.size()];
        Object[] nullLeft = new Object[left.get(0).length];
        Object[] nullRight = new Object[right.get(0).length];
        List<Object[]> out = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < lorder.size() && j < rorder.size()) {
            int li = lorder.get(i);
            int rj = rorder.get(j);
            boolean ln = containsNull(left.get(li), lk);
            boolean rn = containsNull(right.get(rj), rk);
            if (ln || rn) {
                if (ln && rn) {
                    i++;
                    j++;
                } else if (ln) {
                    if (leftPreserved) {
                        out.add(concat(left.get(li), nullRight));
                    }
                    i++;
                } else {
                    if (rightPreserved) {
                        out.add(concat(nullLeft, right.get(rj)));
                    }
                    j++;
                }
                continue;
            }
            int cmp = compareKeys(left.get(li), lk, right.get(rj), rk);
            if (cmp < 0) {
                if (leftPreserved) {
                    out.add(concat(left.get(li), nullRight));
                }
                i++;
            } else if (cmp > 0) {
                if (rightPreserved) {
                    out.add(concat(nullLeft, right.get(rj)));
                }
                j++;
            } else {
                int i2 = i;
                while (i2 < lorder.size()
                        && !containsNull(left.get(lorder.get(i2)), lk)
                        && compareKeys(left.get(lorder.get(i2)), lk,
                        right.get(rj), rk) == 0) {
                    i2++;
                }
                int j2 = j;
                while (j2 < rorder.size()
                        && !containsNull(right.get(rorder.get(j2)), rk)
                        && compareKeys(right.get(rorder.get(j2)), rk,
                        left.get(li), lk) == 0) {
                    j2++;
                }
                for (int a = i; a < i2; a++) {
                    for (int b = j; b < j2; b++) {
                        int la = lorder.get(a);
                        int rb = rorder.get(b);
                        out.add(concat(left.get(la), right.get(rb)));
                        leftMatched[la] = true;
                        rightMatched[rb] = true;
                    }
                }
                i = i2;
                j = j2;
            }
        }
        while (i < lorder.size()) {
            if (leftPreserved) {
                out.add(concat(left.get(lorder.get(i)), nullRight));
            }
            i++;
        }
        while (j < rorder.size()) {
            if (rightPreserved) {
                out.add(concat(nullLeft, right.get(rorder.get(j))));
            }
            j++;
        }
        return out;
    }

    private List<Object[]> nestedLoopJoin(List<Object[]> left, List<Object[]> right,
                                          ExecContext ctx) {
        JoinRelType type = getJoinType();
        boolean leftPreserved = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean rightPreserved = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] leftMatched = new boolean[left.size()];
        boolean[] rightMatched = new boolean[right.size()];
        Object[] nullLeft = new Object[left.get(0).length];
        Object[] nullRight = new Object[right.get(0).length];
        int ncols = nullLeft.length + nullRight.length;
        VectorSchemaRoot probe = buildProbeRoot(ncols, ctx);
        List<Object[]> out = new ArrayList<>();
        try {
            for (int i = 0; i < left.size(); i++) {
                for (int j = 0; j < right.size(); j++) {
                    writeProbeRow(probe, left.get(i), right.get(j));
                    ValueVector cond = ctx.interpreter().eval(getCondition(), probe);
                    try {
                        boolean hit = !cond.isNull(0)
                                && ((BitVector) cond).get(0) == 1;
                        if (hit) {
                            out.add(concat(left.get(i), right.get(j)));
                            leftMatched[i] = true;
                            rightMatched[j] = true;
                        }
                    } finally {
                        cond.close();
                    }
                }
            }
        } finally {
            probe.close();
        }
        if (leftPreserved) {
            for (int i = 0; i < left.size(); i++) {
                if (!leftMatched[i]) {
                    out.add(concat(left.get(i), nullRight));
                }
            }
        }
        if (rightPreserved) {
            for (int j = 0; j < right.size(); j++) {
                if (!rightMatched[j]) {
                    out.add(concat(nullLeft, right.get(j)));
                }
            }
        }
        return out;
    }

    private VectorSchemaRoot buildProbeRoot(int ncols, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(1);
            v.allocateNew();
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    private void writeProbeRow(VectorSchemaRoot probe, Object[] l, Object[] r) {
        List<FieldVector> vectors = probe.getFieldVectors();
        for (int c = 0; c < l.length; c++) {
            writeObject(vectors.get(c), 0, l[c]);
        }
        for (int c = 0; c < r.length; c++) {
            writeObject(vectors.get(l.length + c), 0, r[c]);
        }
        probe.setRowCount(1);
    }

    // ---- helpers ----

    private static boolean containsNull(Object[] row, List<Integer> keys) {
        for (int k : keys) {
            if (row[k] == null) {
                return true;
            }
        }
        return false;
    }

    private static List<Object> keyOf(Object[] row, List<Integer> keys) {
        List<Object> key = new ArrayList<>(keys.size());
        for (int k : keys) {
            key.add(row[k]);
        }
        return key;
    }

    private static Object[] concat(Object[] l, Object[] r) {
        Object[] out = new Object[l.length + r.length];
        System.arraycopy(l, 0, out, 0, l.length);
        System.arraycopy(r, 0, out, l.length, r.length);
        return out;
    }

    private static List<Integer> sortedIndices(List<Object[]> rows, List<Integer> keys) {
        List<Integer> order = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            order.add(i);
        }
        order.sort(Comparator.comparingInt((Integer i) -> nullFlag(rows.get(i), keys))
                .thenComparing((Integer a, Integer b) ->
                        compareKeys(rows.get(a), keys, rows.get(b), keys)));
        return order;
    }

    private static int nullFlag(Object[] row, List<Integer> keys) {
        return containsNull(row, keys) ? 1 : 0; // null keys sort last
    }

    private static int compareKeys(Object[] a, List<Integer> ak,
                                   Object[] b, List<Integer> bk) {
        for (int k = 0; k < ak.size(); k++) {
            Object x = a[ak.get(k)];
            Object y = b[bk.get(k)];
            if (x == null || y == null) {
                if (x == null && y == null) {
                    continue;
                }
                return x == null ? 1 : -1;
            }
            int c = compareValues(x, y);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareValues(Object a, Object b) {
        return ((Comparable) a).compareTo(b);
    }

    private VectorSchemaRoot buildOutput(List<Object[]> rows, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(rows.size());
            v.allocateNew();
        }
        for (int r = 0; r < rows.size(); r++) {
            Object[] row = rows.get(r);
            for (int c = 0; c < row.length; c++) {
                writeObject(vectors.get(c), r, row[c]);
            }
        }
        for (FieldVector v : vectors) {
            v.setValueCount(rows.size());
        }
        // of() after setValueCount: rowCount derives from first vector's valueCount
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    private static void writeObject(FieldVector out, int row, Object o) {
        if (o == null) {
            out.setNull(row);
            return;
        }
        if (out instanceof IntVector iv) {
            iv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof BigIntVector bv) {
            bv.setSafe(row, ((Number) o).longValue());
        } else if (out instanceof Float8Vector fv) {
            fv.setSafe(row, ((Number) o).doubleValue());
        } else if (out instanceof VarCharVector vv) {
            vv.setSafe(row, o.toString().getBytes(StandardCharsets.UTF_8));
        } else if (out instanceof BitVector bv) {
            bv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof DateDayVector dv) {
            dv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof TimeStampMilliVector tv) {
            tv.setSafe(row, ((Number) o).longValue());
        } else {
            throw new UnsupportedOperationException(
                    "cannot write value to " + out.getMinorType());
        }
    }
}
