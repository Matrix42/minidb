package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
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
        RelTraitSet traits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
        RelNode left = convert(join.getLeft(), MiniDbConvention.INSTANCE);
        RelNode right = convert(join.getRight(), MiniDbConvention.INSTANCE);
        JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        if (info.isEqui()) {
            return new MiniDbHashJoin(join.getCluster(), traits, left, right,
                    join.getCondition(), join.getJoinType());
        }
        return new MiniDbNestedLoopJoin(join.getCluster(), traits, left, right,
                join.getCondition(), join.getJoinType());
    }
}
