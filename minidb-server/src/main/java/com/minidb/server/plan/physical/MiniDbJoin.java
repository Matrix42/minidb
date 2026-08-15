package com.minidb.server.plan.physical;

import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.exec.ValueComparators;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
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
 * MiniDbSortMergeJoin, MiniDbNestedLoopJoin); this class owns columnar
 * materialization of both inputs (into a single {@link VectorSchemaRoot} each,
 * no per-cell boxing), output building, and the single-batch lazy iterator.
 * Join strategies work on row indices and columnar keys, never on Object[].
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
        VectorSchemaRoot left = materializeColumns(getLeft(), ctx);
        VectorSchemaRoot right = materializeColumns(getRight(), ctx);
        try {
            JoinInfo info = analyzeCondition();
            List<int[]> pairs = joinPairs(left, right, info, type, ctx);
            VectorSchemaRoot out = buildOutput(left, right, pairs, ctx);
            left.close();
            right.close();
            boolean[] done = {false};
            return new BatchIterator() {
                @Override
                public boolean hasNext() {
                    return !done[0];
                }

                @Override
                public VectorSchemaRoot next() {
                    done[0] = true;
                    return out;
                }

                @Override
                public void close() {
                    out.close();
                }
            };
        } catch (RuntimeException e) {
            left.close();
            right.close();
            throw e;
        }
    }

    /**
     * Strategy-specific join. Returns output row pairs {@code {leftIdx, rightIdx}},
     * where -1 means the null-padded side (outer joins preserve unmatched rows).
     */
    protected abstract List<int[]> joinPairs(VectorSchemaRoot left, VectorSchemaRoot right,
                                             JoinInfo info, JoinRelType type, ExecContext ctx);

    /** Column count of the left input (from its row type, not the data). */
    protected final int leftColumnCount() {
        return getLeft().getRowType().getFieldCount();
    }

    /** Column count of the right input; see {@link #leftColumnCount()}. */
    protected final int rightColumnCount() {
        return getRight().getRowType().getFieldCount();
    }

    /** Pulls every batch of {@code input} into a single owned root, no per-cell boxing. */
    private VectorSchemaRoot materializeColumns(RelNode input, ExecContext ctx) {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        int total = 0;
        BatchIterator it = ((MiniDbRel) input).execute(ctx);
        while (it.hasNext()) {
            VectorSchemaRoot b = it.next();
            batches.add(b);
            total += b.getRowCount();
        }
        VectorSchemaRoot merged;
        if (batches.isEmpty()) {
            merged = emptyRoot(input, ctx);
        } else {
            merged = VectorSchemaRoot.create(batches.get(0).getSchema(), ctx.allocator());
            merged.allocateNew();
            int dst = 0;
            for (VectorSchemaRoot batch : batches) {
                for (int i = 0; i < batch.getRowCount(); i++) {
                    RowCopier.copyRow(batch, i, merged, dst++);
                }
            }
            merged.setRowCount(dst);
        }
        // close input only AFTER copying: Filter/Project own their batches.
        it.close();
        return merged;
    }

    private static VectorSchemaRoot emptyRoot(RelNode input, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : input.getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(0);
            v.allocateNew();
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    /** Writes join output pairs back into a columnar root (null side = setNull). */
    private VectorSchemaRoot buildOutput(VectorSchemaRoot left, VectorSchemaRoot right,
                                         List<int[]> pairs, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        int total = pairs.size();
        for (FieldVector v : vectors) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }
        int leftCols = leftColumnCount();
        int rightCols = rightColumnCount();
        for (int r = 0; r < total; r++) {
            int[] pair = pairs.get(r);
            for (int c = 0; c < leftCols; c++) {
                if (pair[0] >= 0) {
                    RowCopier.copyRow(left.getVector(c), pair[0], vectors.get(c), r);
                } else {
                    vectors.get(c).setNull(r);
                }
            }
            for (int c = 0; c < rightCols; c++) {
                if (pair[1] >= 0) {
                    RowCopier.copyRow(right.getVector(c), pair[1], vectors.get(leftCols + c), r);
                } else {
                    vectors.get(leftCols + c).setNull(r);
                }
            }
        }
        for (FieldVector v : vectors) {
            v.setValueCount(total);
        }
        // of() after setValueCount: rowCount derives from first vector's valueCount.
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    /** True if any of the join-key columns in {@code row} is null. */
    protected static boolean hasNullKey(VectorSchemaRoot root, int row, List<Integer> keyCols) {
        for (int colIdx : keyCols) {
            if (root.getVector(colIdx).isNull(row)) {
                return true;
            }
        }
        return false;
    }

    /** Row indices of {@code root} ordered by the key columns, nulls last. */
    protected static List<Integer> sortedIndices(VectorSchemaRoot root, List<Integer> keyCols) {
        List<Integer> order = new ArrayList<>(root.getRowCount());
        for (int rowIdx = 0; rowIdx < root.getRowCount(); rowIdx++) {
            order.add(rowIdx);
        }
        order.sort(Comparator.comparingInt((Integer rowIdx) -> nullKeyFlag(root, rowIdx, keyCols))
                .thenComparing((Integer a, Integer b) ->
                        compareKeys(root, a, keyCols, root, b, keyCols)));
        return order;
    }

    /** 1 when the row has a null key, 0 otherwise — lets null-keyed rows sort last. */
    protected static int nullKeyFlag(VectorSchemaRoot root, int row, List<Integer> keyCols) {
        return hasNullKey(root, row, keyCols) ? 1 : 0;
    }

    protected static int compareKeys(VectorSchemaRoot left, int leftRow, List<Integer> leftKeyCols,
                                     VectorSchemaRoot right, int rightRow, List<Integer> rightKeyCols) {
        for (int k = 0; k < leftKeyCols.size(); k++) {
            ValueVector lv = left.getVector(leftKeyCols.get(k));
            ValueVector rv = right.getVector(rightKeyCols.get(k));
            boolean leftNull = lv.isNull(leftRow);
            boolean rightNull = rv.isNull(rightRow);
            if (leftNull || rightNull) {
                if (leftNull && rightNull) {
                    continue;
                }
                return leftNull ? 1 : -1;
            }
            int cmp = ValueComparators.compare(lv, leftRow, rv, rightRow);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    protected static List<Integer> identity(int n) {
        List<Integer> order = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            order.add(i);
        }
        return order;
    }

    protected static int[] toIntArray(List<Integer> cols) {
        int[] arr = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            arr[i] = cols.get(i);
        }
        return arr;
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
