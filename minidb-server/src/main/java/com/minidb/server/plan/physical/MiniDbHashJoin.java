package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * Hash join: builds a hash table on the left input keyed by the equi columns,
 * then probes it with the right input. Equi-join only. Null keys never match
 * in an equi-join, so null-keyed rows are excluded from the table and, for
 * outer joins, emitted as preserved rows instead.
 */
public class MiniDbHashJoin extends MiniDbJoin {

    public MiniDbHashJoin(RelOptCluster cluster, RelTraitSet traitSet,
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
        // work there so the planner can rank the three join strategies: hash
        // join builds a table on the left and probes with the right, linear in
        // both inputs.
        return planner.getCostFactory().makeCost(leftRows + rightRows, 0, 0);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbHashJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<int[]> joinPairs(VectorSchemaRoot left, VectorSchemaRoot right,
                                    JoinInfo info, JoinRelType type, ExecContext ctx) {
        List<Integer> leftKeyCols = info.leftKeys;
        List<Integer> rightKeyCols = info.rightKeys;
        int[] leftKeyArr = toIntArray(leftKeyCols);
        int[] rightKeyArr = toIntArray(rightKeyCols);
        // 残留(非等值)条件:等值键之外的合取项(query13/15 的 OR 残留在等值键匹配后仍需过滤)。
        RexNode residual = info.getRemaining(getCluster().getRexBuilder());
        boolean hasResidual = !residual.isAlwaysTrue();
        VectorSchemaRoot probeRoot = hasResidual ? buildProbeRoot(ctx) : null;
        // Build: key -> row indices of the left input sharing that key.
        // Null-keyed rows are skipped (they can never match).
        Map<ColumnKey, List<Integer>> buildTable = new HashMap<>();
        for (int leftIdx = 0; leftIdx < left.getRowCount(); leftIdx++) {
            if (hasNullKey(left, leftIdx, leftKeyCols)) {
                continue;
            }
            buildTable.computeIfAbsent(new ColumnKey(left, leftIdx, leftKeyArr),
                    k -> new ArrayList<>()).add(leftIdx);
        }
        boolean keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] matchedLeft = new boolean[left.getRowCount()];
        List<int[]> outputRows = new ArrayList<>();
        try {
            // Probe: for each right row, join with every left row of the same key.
            for (int rightIdx = 0; rightIdx < right.getRowCount(); rightIdx++) {
                List<Integer> matchingLeftRows;
                if (hasNullKey(right, rightIdx, rightKeyCols)) {
                    matchingLeftRows = null;
                } else {
                    matchingLeftRows = buildTable.get(new ColumnKey(right, rightIdx, rightKeyArr));
                }
                if (matchingLeftRows != null) {
                    for (int leftIdx : matchingLeftRows) {
                        if (hasResidual && !residualMatches(probeRoot, left, leftIdx, right, rightIdx,
                                residual, ctx)) {
                            continue;
                        }
                        outputRows.add(new int[]{leftIdx, rightIdx});
                        matchedLeft[leftIdx] = true;
                    }
                } else if (keepUnmatchedRight) {
                    // Right-preserved outer join: emit the unmatched right row.
                    outputRows.add(new int[]{-1, rightIdx});
                }
            }
        } finally {
            if (probeRoot != null) {
                probeRoot.close();
            }
        }
        if (keepUnmatchedLeft) {
            for (int leftIdx = 0; leftIdx < left.getRowCount(); leftIdx++) {
                if (!matchedLeft[leftIdx]) {
                    outputRows.add(new int[]{leftIdx, -1});
                }
            }
        }
        return outputRows;
    }

    private static boolean residualMatches(VectorSchemaRoot probeRoot, VectorSchemaRoot left,
                                           int leftIdx, VectorSchemaRoot right, int rightIdx,
                                           RexNode residual, ExecContext ctx) {
        writeProbeRow(probeRoot, left, leftIdx, right, rightIdx);
        ValueVector result = ctx.interpreter().eval(residual, probeRoot);
        try {
            return !result.isNull(0) && ((BitVector) result).get(0) == 1;
        } finally {
            result.close();
        }
    }
}
