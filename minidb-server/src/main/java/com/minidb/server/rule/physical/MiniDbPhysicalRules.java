package com.minidb.server.rule.physical;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;

public final class MiniDbPhysicalRules {

    public static final List<RelOptRule> ALL = List.of(
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
            new MiniDbJoinRule(),
            new MiniDbTableSpoolRule(),
            new MiniDbRepeatUnionRule());

    private MiniDbPhysicalRules() {
    }
}
