package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbRepeatUnion;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;

public final class MiniDbRepeatUnionRule extends ConverterRule {

    public MiniDbRepeatUnionRule() {
        this(
                ConverterRule.Config.INSTANCE
                        .withConversion(
                                LogicalRepeatUnion.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbRepeatUnionRule")
                        .withRuleFactory(MiniDbRepeatUnionRule::new));
    }

    private MiniDbRepeatUnionRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalRepeatUnion repeatUnion = (LogicalRepeatUnion) rel;
        RelNode seed = convert(repeatUnion.getSeedRel(), MiniDbConvention.INSTANCE);
        RelNode iterative = convert(repeatUnion.getIterativeRel(), MiniDbConvention.INSTANCE);
        return new MiniDbRepeatUnion(
                repeatUnion.getCluster(),
                repeatUnion.getTraitSet().replace(MiniDbConvention.INSTANCE),
                seed,
                iterative,
                repeatUnion.all,
                repeatUnion.iterationLimit,
                repeatUnion.getTransientTable());
    }
}
