package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbUnion;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.ArrayList;
import java.util.List;

public final class MiniDbUnionRule extends ConverterRule {

    public MiniDbUnionRule() {
        this(
                ConverterRule.Config.INSTANCE
                        .withConversion(
                                LogicalUnion.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbUnionRule")
                        .withRuleFactory(MiniDbUnionRule::new));
    }

    private MiniDbUnionRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalUnion union = (LogicalUnion) rel;
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode in : union.getInputs()) {
            inputs.add(convert(in, MiniDbConvention.INSTANCE));
        }
        return new MiniDbUnion(
                union.getCluster(),
                union.getTraitSet().replace(MiniDbConvention.INSTANCE),
                inputs,
                union.all);
    }
}
