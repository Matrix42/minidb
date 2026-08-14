package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.List;
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
 * Sort-merge join: both inputs are ordered by the equi key columns (nulls
 * last) and merged, emitting the cross product of equal-key groups. If an
 * input's declared collation already covers the join keys it is consumed
 * as-is (no internal sort); otherwise that side is sorted internally.
 * Null keys never match in an equi-join, so for outer joins they are emitted
 * as preserved rows instead.
 */
public class MiniDbSortMergeJoin extends MiniDbJoin {

    private final boolean leftSorted;
    private final boolean rightSorted;

    public MiniDbSortMergeJoin(RelOptCluster cluster, RelTraitSet traitSet,
                               RelNode left, RelNode right, RexNode condition,
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
        double sort = (leftSorted ? 0 : leftRows * Math.log(leftRows + 1))
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
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbSortMergeJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<Object[]> joinRows(List<Object[]> left, List<Object[]> right,
                                      JoinInfo info, JoinRelType type, ExecContext ctx) {
        List<Integer> leftKeyCols = info.leftKeys;
        List<Integer> rightKeyCols = info.rightKeys;
        // Row indices in merge order: a pre-sorted input can be consumed in
        // its natural order (identity), otherwise sort internally by the keys.
        List<Integer> leftScanOrder = leftSorted ? identity(left.size()) : sortedIndices(left, leftKeyCols);
        List<Integer> rightScanOrder = rightSorted ? identity(right.size()) : sortedIndices(right, rightKeyCols);
        // Outer-join semantics: unmatched rows on the preserved side are padded
        // with nulls on the other side.
        boolean keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        Object[] nullRowLeft = new Object[leftColumnCount()];
        Object[] nullRowRight = new Object[rightColumnCount()];
        List<Object[]> outputRows = new ArrayList<>();
        int leftPos = 0;
        int rightPos = 0;
        while (leftPos < leftScanOrder.size() && rightPos < rightScanOrder.size()) {
            int leftRowIdx = leftScanOrder.get(leftPos);
            int rightRowIdx = rightScanOrder.get(rightPos);
            boolean leftHasNullKey = hasNullKey(left.get(leftRowIdx), leftKeyCols);
            boolean rightHasNullKey = hasNullKey(right.get(rightRowIdx), rightKeyCols);
            if (leftHasNullKey || rightHasNullKey) {
                // Null keys never match. For outer joins each null-keyed row is
                // emitted as a preserved row (padded with nulls on the other side);
                // for inner joins it is simply skipped.
                if (leftHasNullKey && rightHasNullKey) {
                    if (keepUnmatchedLeft) {
                        outputRows.add(concat(left.get(leftRowIdx), nullRowRight));
                    }
                    if (keepUnmatchedRight) {
                        outputRows.add(concat(nullRowLeft, right.get(rightRowIdx)));
                    }
                    leftPos++;
                    rightPos++;
                } else if (leftHasNullKey) {
                    if (keepUnmatchedLeft) {
                        outputRows.add(concat(left.get(leftRowIdx), nullRowRight));
                    }
                    leftPos++;
                } else {
                    if (keepUnmatchedRight) {
                        outputRows.add(concat(nullRowLeft, right.get(rightRowIdx)));
                    }
                    rightPos++;
                }
                continue;
            }
            int cmp = compareKeys(left.get(leftRowIdx), leftKeyCols,
                    right.get(rightRowIdx), rightKeyCols);
            if (cmp < 0) {
                if (keepUnmatchedLeft) {
                    outputRows.add(concat(left.get(leftRowIdx), nullRowRight));
                }
                leftPos++;
            } else if (cmp > 0) {
                if (keepUnmatchedRight) {
                    outputRows.add(concat(nullRowLeft, right.get(rightRowIdx)));
                }
                rightPos++;
            } else {
                // Equal keys: find the full group of matching rows on each side
                // (contiguous, non-null, same key) and cross-product them.
                int leftGroupEnd = leftPos;
                while (leftGroupEnd < leftScanOrder.size()
                        && !hasNullKey(left.get(leftScanOrder.get(leftGroupEnd)), leftKeyCols)
                        && compareKeys(left.get(leftScanOrder.get(leftGroupEnd)), leftKeyCols,
                        right.get(rightRowIdx), rightKeyCols) == 0) {
                    leftGroupEnd++;
                }
                int rightGroupEnd = rightPos;
                while (rightGroupEnd < rightScanOrder.size()
                        && !hasNullKey(right.get(rightScanOrder.get(rightGroupEnd)), rightKeyCols)
                        && compareKeys(right.get(rightScanOrder.get(rightGroupEnd)), rightKeyCols,
                        left.get(leftRowIdx), leftKeyCols) == 0) {
                    rightGroupEnd++;
                }
                for (int lp = leftPos; lp < leftGroupEnd; lp++) {
                    for (int rp = rightPos; rp < rightGroupEnd; rp++) {
                        outputRows.add(concat(left.get(leftScanOrder.get(lp)),
                                right.get(rightScanOrder.get(rp))));
                    }
                }
                leftPos = leftGroupEnd;
                rightPos = rightGroupEnd;
            }
        }
        // Drain whichever side still has rows (all remaining rows there have
        // keys larger than the exhausted side's, or are null-keyed).
        while (leftPos < leftScanOrder.size()) {
            if (keepUnmatchedLeft) {
                outputRows.add(concat(left.get(leftScanOrder.get(leftPos)), nullRowRight));
            }
            leftPos++;
        }
        while (rightPos < rightScanOrder.size()) {
            if (keepUnmatchedRight) {
                outputRows.add(concat(nullRowLeft, right.get(rightScanOrder.get(rightPos))));
            }
            rightPos++;
        }
        return outputRows;
    }

    private static List<Integer> identity(int n) {
        List<Integer> order = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            order.add(i);
        }
        return order;
    }
}
