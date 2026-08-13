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

/** Nested-loop join: evaluates the full RexNode condition per candidate pair
 *  (correctness over speed). Works for any condition. */
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
        JoinRelType t = getJoinType();
        boolean leftPreserved = t == JoinRelType.LEFT || t == JoinRelType.FULL;
        boolean rightPreserved = t == JoinRelType.RIGHT || t == JoinRelType.FULL;
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
}
