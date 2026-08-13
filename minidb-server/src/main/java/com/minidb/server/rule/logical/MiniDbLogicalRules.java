package com.minidb.server.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;

public final class MiniDbLogicalRules {

    /** HepPlanner 逻辑优化规则:FilterPushDown 进 join + 相邻算子合并/换位。 */
    public static final List<RelOptRule> ALL = List.of(
            new FilterJoinRule.FilterIntoJoinRule(false, RelFactories.LOGICAL_BUILDER,
                    FilterJoinRule.TRUE_PREDICATE),   // FilterPushDown into join
            FilterProjectTransposeRule.Config.DEFAULT.toRule(),
            ProjectMergeRule.Config.DEFAULT.toRule(),
            FilterMergeRule.Config.DEFAULT.toRule());

    private MiniDbLogicalRules() {
    }
}
