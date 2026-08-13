package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

/**
 * Join base class. Subclasses implement one strategy (MiniDbHashJoin,
 * MiniDbSortMergeJoin, MiniDbNestedLoopJoin); this class owns materialization
 * of both inputs, output building, and the single-batch lazy iterator.
 * Rows are normalized to Object[]; output is a single batch.
 */
public abstract class MiniDbJoin extends Join implements MiniDbRel {

    protected MiniDbJoin(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode left, RelNode right, RexNode condition,
                         JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, Set.of(), joinType);
    }

    @Override
    public final BatchIterator execute(ExecContext ctx) {
        JoinRelType type = getJoinType();
        if (type == JoinRelType.SEMI || type == JoinRelType.ANTI) {
            throw new UnsupportedOperationException("semi/anti join not supported");
        }
        List<Object[]> leftRows = materialize(getLeft(), ctx);
        List<Object[]> rightRows = materialize(getRight(), ctx);
        JoinInfo info = analyzeCondition();
        List<Object[]> out = joinRows(leftRows, rightRows, info, type, ctx);
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

    /** Strategy-specific join implementation. */
    protected abstract List<Object[]> joinRows(
            List<Object[]> left, List<Object[]> right,
            JoinInfo info, JoinRelType type, ExecContext ctx);

    // analyzeCondition() inherited from Join (Calcite provides it public).
    // ---- shared helpers (verbatim from original MiniDbJoin.java) ----

    protected final List<Object[]> materialize(RelNode input, ExecContext ctx) {
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

    protected static Object readObject(ValueVector v, int row) {
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

    protected VectorSchemaRoot buildOutput(List<Object[]> rows, ExecContext ctx) {
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

    protected static void writeObject(FieldVector out, int row, Object o) {
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

    protected static boolean containsNull(Object[] row, List<Integer> keys) {
        for (int k : keys) {
            if (row[k] == null) {
                return true;
            }
        }
        return false;
    }

    protected static List<Object> keyOf(Object[] row, List<Integer> keys) {
        List<Object> key = new ArrayList<>(keys.size());
        for (int k : keys) {
            key.add(row[k]);
        }
        return key;
    }

    protected static Object[] concat(Object[] l, Object[] r) {
        Object[] out = new Object[l.length + r.length];
        System.arraycopy(l, 0, out, 0, l.length);
        System.arraycopy(r, 0, out, l.length, r.length);
        return out;
    }

    protected static List<Integer> sortedIndices(List<Object[]> rows, List<Integer> keys) {
        List<Integer> order = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            order.add(i);
        }
        order.sort(Comparator.comparingInt((Integer i) -> nullFlag(rows.get(i), keys))
                .thenComparing((Integer a, Integer b) ->
                        compareKeys(rows.get(a), keys, rows.get(b), keys)));
        return order;
    }

    protected static int nullFlag(Object[] row, List<Integer> keys) {
        return containsNull(row, keys) ? 1 : 0; // null keys sort last
    }

    protected static int compareKeys(Object[] a, List<Integer> ak,
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
    protected static int compareValues(Object a, Object b) {
        return ((Comparable) a).compareTo(b);
    }

    /** True if any collation covers {@code keys} as an ascending prefix. */
    public static boolean coversKeys(List<RelCollation> collations, List<Integer> keys) {
        for (RelCollation c : collations) {
            List<RelFieldCollation> fcs = c.getFieldCollations();
            if (fcs.size() < keys.size()) {
                continue;
            }
            boolean ok = true;
            for (int i = 0; i < keys.size(); i++) {
                RelFieldCollation fc = fcs.get(i);
                RelFieldCollation.Direction d = fc.getDirection();
                if (fc.getFieldIndex() != keys.get(i)
                        || (d != RelFieldCollation.Direction.ASCENDING
                            && d != RelFieldCollation.Direction.STRICTLY_ASCENDING)) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return true;
            }
        }
        return false;
    }
}
