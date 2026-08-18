package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double leftRows = mq.getRowCount(getLeft());
        double rightRows = mq.getRowCount(getRight());
        // Calcite 1.42's VolcanoCost.isLt compares ONLY the rowCount component
        // (cpu/io are ignored — see VolcanoCost.isLt). Encode the estimated
        // work there so the planner can rank the three join strategies:
        // nested-loop evaluates every (left,right) pair. 乘 10 是因为逐对 RexInterpreter
        // 求值比 HashJoin 的哈希查找贵一个量级;不乘的话一侧只有 1 行时 left×right ≈ left+right
        // (如 query72 的 warehouse=1),NestedLoop 会因便宜 1 被误选。
        return planner.getCostFactory().makeCost(leftRows * rightRows * 10, 0, 0);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbNestedLoopJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<int[]> joinPairs(VectorSchemaRoot left, VectorSchemaRoot right,
                                    JoinInfo info, JoinRelType type, ExecContext ctx) {
        boolean keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] matchedLeft = new boolean[left.getRowCount()];
        boolean[] matchedRight = new boolean[right.getRowCount()];
        VectorSchemaRoot probeRoot = buildProbeRoot(ctx);
        List<int[]> outputRows = new ArrayList<>();
        try {
            for (int leftIdx = 0; leftIdx < left.getRowCount(); leftIdx++) {
                if (leftIdx % 1000 == 0) {
                    ExecContext.checkInterrupted();
                }
                for (int rightIdx = 0; rightIdx < right.getRowCount(); rightIdx++) {
                    writeProbeRow(probeRoot, left, leftIdx, right, rightIdx);
                    ValueVector conditionResult = ctx.interpreter().eval(getCondition(), probeRoot);
                    try {
                        boolean matches = !conditionResult.isNull(0)
                                && ((BitVector) conditionResult).get(0) == 1;
                        if (matches) {
                            outputRows.add(new int[]{leftIdx, rightIdx});
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
            for (int leftIdx = 0; leftIdx < left.getRowCount(); leftIdx++) {
                if (!matchedLeft[leftIdx]) {
                    outputRows.add(new int[]{leftIdx, -1});
                }
            }
        }
        if (keepUnmatchedRight) {
            for (int rightIdx = 0; rightIdx < right.getRowCount(); rightIdx++) {
                if (!matchedRight[rightIdx]) {
                    outputRows.add(new int[]{-1, rightIdx});
                }
            }
        }
        return outputRows;
    }
}
