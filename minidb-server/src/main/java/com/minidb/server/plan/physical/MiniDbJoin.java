package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
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

    protected static Object readObject(ValueVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        if (vector instanceof SmallIntVector sv) {
            return sv.get(row);
        }
        if (vector instanceof IntVector iv) {
            return iv.get(row);
        }
        if (vector instanceof BigIntVector bv) {
            return bv.get(row);
        }
        if (vector instanceof Float4Vector fv) {
            return fv.get(row);
        }
        if (vector instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (vector instanceof DecimalVector dv) {
            return dv.getObject(row);
        }
        if (vector instanceof VarCharVector vv) {
            return new String(vv.get(row), StandardCharsets.UTF_8);
        }
        if (vector instanceof BitVector bv) {
            return bv.get(row);
        }
        if (vector instanceof DateDayVector dv) {
            return dv.get(row);
        }
        if (vector instanceof TimeMilliVector tv) {
            return tv.get(row);
        }
        if (vector instanceof TimeStampMilliVector tv) {
            return tv.get(row);
        }
        if (vector instanceof VarBinaryVector bv) {
            return bv.get(row);
        }
        throw new UnsupportedOperationException(
                "cannot join column type: " + vector.getMinorType());
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
        if (out instanceof SmallIntVector sv) {
            sv.setSafe(row, ((Number) o).shortValue());
        } else if (out instanceof IntVector iv) {
            iv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof BigIntVector bv) {
            bv.setSafe(row, ((Number) o).longValue());
        } else if (out instanceof Float4Vector fv) {
            fv.setSafe(row, ((Number) o).floatValue());
        } else if (out instanceof Float8Vector fv) {
            fv.setSafe(row, ((Number) o).doubleValue());
        } else if (out instanceof DecimalVector dv) {
            dv.setSafe(row, (BigDecimal) o);
        } else if (out instanceof VarCharVector vv) {
            vv.setSafe(row, o.toString().getBytes(StandardCharsets.UTF_8));
        } else if (out instanceof BitVector bv) {
            bv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof DateDayVector dv) {
            dv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof TimeMilliVector tv) {
            tv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof TimeStampMilliVector tv) {
            tv.setSafe(row, ((Number) o).longValue());
        } else if (out instanceof VarBinaryVector bv) {
            bv.setSafe(row, (byte[]) o);
        } else {
            throw new UnsupportedOperationException(
                    "cannot write value to " + out.getMinorType());
        }
    }

    /** True if any of the join-key columns in {@code row} is null. */
    protected static boolean hasNullKey(Object[] row, List<Integer> keyCols) {
        for (int colIdx : keyCols) {
            if (row[colIdx] == null) {
                return true;
            }
        }
        return false;
    }

    /** The values of the join-key columns of {@code row}, as a hashable key. */
    protected static List<Object> buildKey(Object[] row, List<Integer> keyCols) {
        List<Object> key = new ArrayList<>(keyCols.size());
        for (int colIdx : keyCols) {
            key.add(row[colIdx]);
        }
        return key;
    }

    protected static Object[] concat(Object[] leftRow, Object[] rightRow) {
        Object[] out = new Object[leftRow.length + rightRow.length];
        System.arraycopy(leftRow, 0, out, 0, leftRow.length);
        System.arraycopy(rightRow, 0, out, leftRow.length, rightRow.length);
        return out;
    }

    /** Row indices of {@code rows} ordered by the key columns, nulls last. */
    protected static List<Integer> sortedIndices(List<Object[]> rows, List<Integer> keyCols) {
        List<Integer> order = new ArrayList<>(rows.size());
        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            order.add(rowIdx);
        }
        order.sort(Comparator.comparingInt((Integer rowIdx) -> nullKeyFlag(rows.get(rowIdx), keyCols))
                .thenComparing((Integer a, Integer b) ->
                        compareKeys(rows.get(a), keyCols, rows.get(b), keyCols)));
        return order;
    }

    /** 1 when the row has a null key, 0 otherwise — lets null-keyed rows sort last. */
    protected static int nullKeyFlag(Object[] row, List<Integer> keyCols) {
        return hasNullKey(row, keyCols) ? 1 : 0;
    }

    protected static int compareKeys(Object[] leftRow, List<Integer> leftKeyCols,
                                     Object[] rightRow, List<Integer> rightKeyCols) {
        for (int k = 0; k < leftKeyCols.size(); k++) {
            Object leftVal = leftRow[leftKeyCols.get(k)];
            Object rightVal = rightRow[rightKeyCols.get(k)];
            if (leftVal == null || rightVal == null) {
                if (leftVal == null && rightVal == null) {
                    continue;
                }
                return leftVal == null ? 1 : -1;
            }
            int cmp = compareValues(leftVal, rightVal);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static int compareValues(Object left, Object right) {
        return ((Comparable) left).compareTo(right);
    }

    /** True if any collation covers {@code keys} as an ascending prefix.
     *  Null collations (e.g. a table with no declared ordering) are treated
     *  as covering nothing. */
    public static boolean coversKeys(List<RelCollation> collations, List<Integer> keys) {
        if (collations == null || keys == null) {
            return false;
        }
        for (RelCollation collation : collations) {
            List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
            if (fieldCollations.size() < keys.size()) {
                continue;
            }
            boolean covers = true;
            for (int i = 0; i < keys.size(); i++) {
                RelFieldCollation fieldCollation = fieldCollations.get(i);
                RelFieldCollation.Direction direction = fieldCollation.getDirection();
                if (fieldCollation.getFieldIndex() != keys.get(i)
                        || (direction != RelFieldCollation.Direction.ASCENDING
                            && direction != RelFieldCollation.Direction.STRICTLY_ASCENDING)) {
                    covers = false;
                    break;
                }
            }
            if (covers) {
                return true;
            }
        }
        return false;
    }
}
