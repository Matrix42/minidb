package com.minidb.server.rule.physical;

import org.apache.calcite.plan.RelOptRule;

import java.util.List;

public final class MiniDbPhysicalRules {

    public static final List<RelOptRule> ALL =
            List.of(
                    new MiniDbScanRule(),
                    new MiniDbFilterRule(),
                    new MiniDbProjectRule(),
                    new MiniDbSortRule(),
                    new MiniDbValuesRule(),
                    new MiniDbModifyRule(),
                    new MiniDbAggregateRule(),
                    new MiniDbUnionRule(),
                    new MiniDbIntersectRule(),
                    new MiniDbExceptRule(),
                    new MiniDbCalcRule(),
                    // Registration order matters for equal-cost joins: VolcanoPlanner's
                    // VolcanoCost.isLt compares only the rowCount component, so a
                    // pre-sorted equi join (sort-merge and hash both cost left+right)
                    // is won by whichever rule fires first. Put sort-merge first to
                    // keep the "pre-sorted inputs -> sort-merge" preference that the
                    // old MiniDbJoinRule encoded deterministically.
                    new MiniDbSortMergeJoinRule(),
                    new MiniDbHashJoinRule(),
                    new MiniDbNestedLoopJoinRule(),
                    new MiniDbTableSpoolRule(),
                    new MiniDbRepeatUnionRule());

    private MiniDbPhysicalRules() {}
}
