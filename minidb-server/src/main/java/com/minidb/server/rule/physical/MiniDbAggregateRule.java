package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbAggregate;
import com.minidb.server.plan.physical.MiniDbConvention;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

public final class MiniDbAggregateRule extends ConverterRule {

    public MiniDbAggregateRule() {
        this(
                ConverterRule.Config.INSTANCE
                        .withConversion(
                                LogicalAggregate.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbAggregateRule")
                        .withRuleFactory(MiniDbAggregateRule::new));
    }

    private MiniDbAggregateRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalAggregate agg = (LogicalAggregate) rel;
        return new MiniDbAggregate(
                agg.getCluster(),
                agg.getTraitSet().replace(MiniDbConvention.INSTANCE),
                convert(agg.getInput(), MiniDbConvention.INSTANCE),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());
    }
}
