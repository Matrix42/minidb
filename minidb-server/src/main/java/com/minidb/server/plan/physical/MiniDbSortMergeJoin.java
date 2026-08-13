package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/** Sort-merge join: merges equal-key groups after sorting both sides by the
 *  equi columns (null keys last). If an input's declared collation already
 *  covers the join keys it is used as-is (no internal sort); otherwise that
 *  side is sorted internally. */
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
        List<Integer> lk = info.leftKeys;
        List<Integer> rk = info.rightKeys;
        List<Integer> lorder = leftSorted ? identity(left.size()) : sortedIndices(left, lk);
        List<Integer> rorder = rightSorted ? identity(right.size()) : sortedIndices(right, rk);
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

    private static List<Integer> identity(int n) {
        List<Integer> order = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            order.add(i);
        }
        return order;
    }
}
