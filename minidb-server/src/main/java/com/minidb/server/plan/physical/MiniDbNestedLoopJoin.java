package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
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
 * Nested-loop join: for every (left, right) candidate pair it builds a
 * one-row joined vector and evaluates the full RexNode condition on it.
 * Correctness over speed — works for any condition, not just equi-joins.
 */
public class MiniDbNestedLoopJoin extends MiniDbJoin {

    public MiniDbNestedLoopJoin(RelOptCluster cluster, RelTraitSet traitSet,
                                RelNode left, RelNode right, RexNode condition,
                                JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbNestedLoopJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<Object[]> joinRows(List<Object[]> left, List<Object[]> right,
                                      JoinInfo info, JoinRelType type, ExecContext ctx) {
        boolean keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] matchedLeft = new boolean[left.size()];
        boolean[] matchedRight = new boolean[right.size()];
        Object[] nullRowLeft = new Object[leftColumnCount()];
        Object[] nullRowRight = new Object[rightColumnCount()];
        int totalCols = nullRowLeft.length + nullRowRight.length;
        VectorSchemaRoot probeRoot = buildProbeRoot(totalCols, ctx);
        List<Object[]> outputRows = new ArrayList<>();
        try {
            for (int leftIdx = 0; leftIdx < left.size(); leftIdx++) {
                for (int rightIdx = 0; rightIdx < right.size(); rightIdx++) {
                    writeProbeRow(probeRoot, left.get(leftIdx), right.get(rightIdx));
                    ValueVector conditionResult = ctx.interpreter().eval(getCondition(), probeRoot);
                    try {
                        boolean matches = !conditionResult.isNull(0)
                                && ((BitVector) conditionResult).get(0) == 1;
                        if (matches) {
                            outputRows.add(concat(left.get(leftIdx), right.get(rightIdx)));
                            matchedLeft[leftIdx] = true;
                            matchedRight[rightIdx] = true;
                        }
                    } finally {
                        conditionResult.close();
                    }
                }
            }
        } finally {
            probeRoot.close();
        }
        // Emit rows that matched nothing, padded with nulls on the other side.
        if (keepUnmatchedLeft) {
            for (int leftIdx = 0; leftIdx < left.size(); leftIdx++) {
                if (!matchedLeft[leftIdx]) {
                    outputRows.add(concat(left.get(leftIdx), nullRowRight));
                }
            }
        }
        if (keepUnmatchedRight) {
            for (int rightIdx = 0; rightIdx < right.size(); rightIdx++) {
                if (!matchedRight[rightIdx]) {
                    outputRows.add(concat(nullRowLeft, right.get(rightIdx)));
                }
            }
        }
        return outputRows;
    }

    private VectorSchemaRoot buildProbeRoot(int totalCols, ExecContext ctx) {
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

    private void writeProbeRow(VectorSchemaRoot probeRoot, Object[] leftRow, Object[] rightRow) {
        List<FieldVector> vectors = probeRoot.getFieldVectors();
        for (int c = 0; c < leftRow.length; c++) {
            RowVectors.writeObject(vectors.get(c), 0, leftRow[c]);
        }
        for (int c = 0; c < rightRow.length; c++) {
            RowVectors.writeObject(vectors.get(leftRow.length + c), 0, rightRow[c]);
        }
        probeRoot.setRowCount(1);
    }
}
