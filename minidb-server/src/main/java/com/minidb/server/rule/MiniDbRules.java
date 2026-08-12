package com.minidb.server.rule;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;

public final class MiniDbRules {

    public static final List<RelOptRule> ALL = List.of(
            new MiniDbScanRule(),
            new MiniDbFilterRule(),
            new MiniDbProjectRule(),
            new MiniDbSortRule(),
            new MiniDbValuesRule(),
            new MiniDbModifyRule());

    private MiniDbRules() {
    }
}
