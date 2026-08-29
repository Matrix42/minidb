package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbFilter;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

public final class MiniDbFilterRule extends ConverterRule {

    public MiniDbFilterRule() {
        this(
                ConverterRule.Config.INSTANCE
                        .withConversion(
                                LogicalFilter.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbFilterRule")
                        .withRuleFactory(MiniDbFilterRule::new));
    }

    private MiniDbFilterRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;
        return new MiniDbFilter(
                filter.getCluster(),
                filter.getTraitSet().replace(MiniDbConvention.INSTANCE),
                convert(filter.getInput(), MiniDbConvention.INSTANCE),
                filter.getCondition());
    }
}
