package com.minidb.server.rule;

import com.minidb.server.plan.MiniDbConvention;
import com.minidb.server.plan.MiniDbJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

public final class MiniDbJoinRule extends ConverterRule {

    public MiniDbJoinRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbJoinRule")
                .withRuleFactory(MiniDbJoinRule::new));
    }

    private MiniDbJoinRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        return new MiniDbJoin(join.getCluster(),
                join.getTraitSet().replace(MiniDbConvention.INSTANCE),
                convert(join.getLeft(), MiniDbConvention.INSTANCE),
                convert(join.getRight(), MiniDbConvention.INSTANCE),
                join.getCondition(), join.getJoinType(),
                MiniDbJoin.Strategy.AUTO);
    }
}
