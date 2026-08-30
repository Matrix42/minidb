package com.minidb.server.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

/**
 * 与 {@link FilterPullFactorsRule} 同款,但作用于 Join 条件:FilterIntoJoinRule 把 含 OR 的 Filter 下推进 Join
 * 后,公共等值项仍埋在 Join 条件的 OR 里,导致 {@code JoinInfo.of} 抽不出等值键、只能走 NestedLoopJoin。因子化后等值键回到顶层
 * AND,HashJoin/SortMergeJoin 规则才能选中它。
 */
public final class JoinPullFactorsRule extends RelOptRule {

    public JoinPullFactorsRule() {
        super(operand(LogicalJoin.class, any()), "JoinPullFactorsRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RexNode factored =
                RexUtil.pullFactors(join.getCluster().getRexBuilder(), join.getCondition());
        if (factored.equals(join.getCondition())) {
            return;
        }
        RelNode newJoin =
                join.copy(
                        join.getTraitSet(),
                        factored,
                        join.getLeft(),
                        join.getRight(),
                        join.getJoinType(),
                        join.isSemiJoinDone());
        call.transformTo(newJoin);
    }
}
