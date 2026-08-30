package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;

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

import java.util.List;

/**
 * Sort-merge join: both inputs are ordered by the equi key columns (nulls last) and merged,
 * emitting the cross product of equal-key groups. If an input's declared collation already covers
 * the join keys it is consumed as-is (no internal sort); otherwise that side is sorted internally.
 * Null keys never match in an equi-join, so for outer joins they are emitted as preserved rows
 * instead.
 */
public class MiniDbSortMergeJoin extends MiniDbJoin {

    private final boolean leftSorted;
    private final boolean rightSorted;

    public MiniDbSortMergeJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
        JoinInfo info = JoinInfo.of(left, right, condition);
        RelMetadataQuery mq = RelMetadataQuery.instance();
        this.leftSorted = coversKeys(mq.collations(left), info.leftKeys);
        this.rightSorted = coversKeys(mq.collations(right), info.rightKeys);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double leftRows = mq.getRowCount(getLeft());
        double rightRows = mq.getRowCount(getRight());
        double sort =
                (leftSorted ? 0 : leftRows * Math.log(leftRows + 1))
                        + (rightSorted ? 0 : rightRows * Math.log(rightRows + 1));
        // Calcite 1.42's VolcanoCost.isLt compares ONLY the rowCount component
        // (cpu/io are ignored — see VolcanoCost.isLt). Encode the estimated
        // work there: the merge pass is linear, and any side not already sorted
        // on the join keys pays an internal sort. When both sides arrive
        // pre-sorted this ties the hash join's build+probe cost, so the
        // sort-merge rule is registered before the hash rule (see
        // MiniDbPhysicalRules) to win that tie — the old deterministic rule's
        // "pre-sorted -> sort-merge" preference.
        return planner.getCostFactory().makeCost(leftRows + rightRows + sort, 0, 0);
    }

    public boolean leftInputSorted() {
        return leftSorted;
    }

    public boolean rightInputSorted() {
        return rightSorted;
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        MiniDbSortMergeJoin newJoin =
                new MiniDbSortMergeJoin(
                        getCluster(), traitSet, left, right, conditionExpr, joinType);
        copyProjectionTo(newJoin);
        return newJoin;
    }

    @Override
    protected PairSource joinPairs(
            VectorSchemaRoot left,
            VectorSchemaRoot right,
            JoinInfo info,
            JoinRelType type,
            ExecContext ctx) {
        List<Integer> leftKeyCols = info.leftKeys;
        List<Integer> rightKeyCols = info.rightKeys;
        // Row indices in merge order: a pre-sorted input can be consumed in
        // its natural order (identity), otherwise sort internally by the keys.
        List<Integer> leftScanOrder =
                leftSorted ? identity(left.getRowCount()) : sortedIndices(left, leftKeyCols);
        List<Integer> rightScanOrder =
                rightSorted ? identity(right.getRowCount()) : sortedIndices(right, rightKeyCols);
        return new MergePairSource(
                left, right, type, ctx, leftKeyCols, rightKeyCols, leftScanOrder, rightScanOrder);
    }

    /** 流式双指针 merge:游标推进产出匹配/保留行,相等键组的 cross product 按内层 游标逐对产出——不物化输出行对,内存 O(批大小)。 */
    private static final class MergePairSource implements PairSource {
        private final VectorSchemaRoot left;
        private final VectorSchemaRoot right;
        private final List<Integer> leftKeyCols;
        private final List<Integer> rightKeyCols;
        private final List<Integer> leftScanOrder;
        private final List<Integer> rightScanOrder;
        private final boolean keepUnmatchedLeft;
        private final boolean keepUnmatchedRight;

        // 主游标
        private int leftPos = 0;
        private int rightPos = 0;
        // 相等键组 cross 游标(inCross=true 时生效)
        private boolean inCross;
        private int crossLeftPos;
        private int crossRightPos;
        private int crossLeftEnd;
        private int crossRightEnd;
        private int crossRightStart;

        MergePairSource(
                VectorSchemaRoot left,
                VectorSchemaRoot right,
                JoinRelType type,
                ExecContext ctx,
                List<Integer> leftKeyCols,
                List<Integer> rightKeyCols,
                List<Integer> leftScanOrder,
                List<Integer> rightScanOrder) {
            this.left = left;
            this.right = right;
            this.leftKeyCols = leftKeyCols;
            this.rightKeyCols = rightKeyCols;
            this.leftScanOrder = leftScanOrder;
            this.rightScanOrder = rightScanOrder;
            this.keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
            this.keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        }

        @Override
        public int fill(int[] leftRows, int[] rightRows, int outPos, int len) {
            int out = outPos;
            int end = outPos + len;
            while (out < end) {
                if (inCross) {
                    // 产出当前相等键组的一对
                    leftRows[out] = leftScanOrder.get(crossLeftPos);
                    rightRows[out] = rightScanOrder.get(crossRightPos);
                    out++;
                    crossRightPos++;
                    if (crossRightPos >= crossRightEnd) {
                        crossLeftPos++;
                        crossRightPos = crossRightStart;
                        if (crossLeftPos >= crossLeftEnd) {
                            // 组耗尽,恢复主循环
                            inCross = false;
                            leftPos = crossLeftEnd;
                            rightPos = crossRightEnd;
                        }
                    }
                    continue;
                }
                if (leftPos >= leftScanOrder.size()) {
                    // 右侧余行:键都大于左侧耗尽侧(或 null 键),FULL/RIGHT 保留
                    while (rightPos < rightScanOrder.size() && out < end) {
                        if (keepUnmatchedRight) {
                            leftRows[out] = -1;
                            rightRows[out] = rightScanOrder.get(rightPos);
                            out++;
                        }
                        rightPos++;
                    }
                    break;
                }
                if (rightPos >= rightScanOrder.size()) {
                    while (leftPos < leftScanOrder.size() && out < end) {
                        if (keepUnmatchedLeft) {
                            leftRows[out] = leftScanOrder.get(leftPos);
                            rightRows[out] = -1;
                            out++;
                        }
                        leftPos++;
                    }
                    break;
                }
                int leftRowIdx = leftScanOrder.get(leftPos);
                int rightRowIdx = rightScanOrder.get(rightPos);
                boolean leftHasNullKey = hasNullKey(left, leftRowIdx, leftKeyCols);
                boolean rightHasNullKey = hasNullKey(right, rightRowIdx, rightKeyCols);
                if (leftHasNullKey || rightHasNullKey) {
                    // Null keys never match. For outer joins each null-keyed row is
                    // emitted as a preserved row; for inner joins it is skipped.
                    if (leftHasNullKey && rightHasNullKey) {
                        if (keepUnmatchedLeft) {
                            leftRows[out] = leftRowIdx;
                            rightRows[out] = -1;
                            out++;
                        }
                        if (keepUnmatchedRight) {
                            leftRows[out] = -1;
                            rightRows[out] = rightRowIdx;
                            out++;
                        }
                        leftPos++;
                        rightPos++;
                    } else if (leftHasNullKey) {
                        if (keepUnmatchedLeft) {
                            leftRows[out] = leftRowIdx;
                            rightRows[out] = -1;
                            out++;
                        }
                        leftPos++;
                    } else {
                        if (keepUnmatchedRight) {
                            leftRows[out] = -1;
                            rightRows[out] = rightRowIdx;
                            out++;
                        }
                        rightPos++;
                    }
                    continue;
                }
                int cmp =
                        compareKeys(
                                left, leftRowIdx, leftKeyCols, right, rightRowIdx, rightKeyCols);
                if (cmp < 0) {
                    if (keepUnmatchedLeft) {
                        leftRows[out] = leftRowIdx;
                        rightRows[out] = -1;
                        out++;
                    }
                    leftPos++;
                } else if (cmp > 0) {
                    if (keepUnmatchedRight) {
                        leftRows[out] = -1;
                        rightRows[out] = rightRowIdx;
                        out++;
                    }
                    rightPos++;
                } else {
                    // 相等键:找两侧完整组(连续、非 null、同键),进入 cross 逐对产出
                    int leftGroupEnd = leftPos;
                    while (leftGroupEnd < leftScanOrder.size()
                            && !hasNullKey(left, leftScanOrder.get(leftGroupEnd), leftKeyCols)
                            && compareKeys(
                                            left,
                                            leftScanOrder.get(leftGroupEnd),
                                            leftKeyCols,
                                            right,
                                            rightRowIdx,
                                            rightKeyCols)
                                    == 0) {
                        leftGroupEnd++;
                    }
                    int rightGroupEnd = rightPos;
                    while (rightGroupEnd < rightScanOrder.size()
                            && !hasNullKey(right, rightScanOrder.get(rightGroupEnd), rightKeyCols)
                            && compareKeys(
                                            right,
                                            rightScanOrder.get(rightGroupEnd),
                                            rightKeyCols,
                                            left,
                                            leftRowIdx,
                                            leftKeyCols)
                                    == 0) {
                        rightGroupEnd++;
                    }
                    inCross = true;
                    crossLeftPos = leftPos;
                    crossRightPos = rightPos;
                    crossLeftEnd = leftGroupEnd;
                    crossRightEnd = rightGroupEnd;
                    crossRightStart = rightPos;
                }
            }
            return out;
        }
    }
}
