package com.minidb.server.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

/**
 * 把 Filter 条件里的公共等值项从 OR 分支提取到顶层 AND(布尔分配律):
 * {@code (a.x=b.x AND p) OR (a.x=b.x AND q)} → {@code a.x=b.x AND (p OR q)}。
 * 这样等值项能被 FilterJoinRule 下推成 HashJoin 键,而不是整体留在 OR 里退化成
 * 交叉连接(query13 的 3 个 OR 块各有 2~3 个 1:1 等值键,因子化前是笛卡尔积)。
 */
public final class FilterPullFactorsRule extends RelOptRule {

    public FilterPullFactorsRule() {
        super(operand(LogicalFilter.class, any()), "FilterPullFactorsRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        RexNode factored = RexUtil.pullFactors(
                filter.getCluster().getRexBuilder(), filter.getCondition());
        if (factored.equals(filter.getCondition())) {
            return;
        }
        RelNode newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), factored);
        call.transformTo(newFilter);
    }
}
