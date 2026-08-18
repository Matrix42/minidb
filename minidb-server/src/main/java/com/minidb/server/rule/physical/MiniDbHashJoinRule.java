package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;

public final class MiniDbHashJoinRule extends ConverterRule {

    public MiniDbHashJoinRule() {
        this(Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE, MiniDbConvention.INSTANCE, "MiniDbHashJoinRule")
                .withRuleFactory(MiniDbHashJoinRule::new));
    }

    private MiniDbHashJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        // 只要抽得出至少一个等值键就用 HashJoin;残留(非等值)条件由 joinPairs 在
        // 匹配对上述值(不再要求 isEqui,否则 query13/15 的 AND(等值键, OR 残留)退化成
        // NestedLoopJoin 笛卡尔积)。
        if (info.leftKeys.isEmpty()) {
            return null;
        }
        RelTraitSet traits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
        return new MiniDbHashJoin(join.getCluster(), traits,
                convert(join.getLeft(), MiniDbConvention.INSTANCE),
                convert(join.getRight(), MiniDbConvention.INSTANCE),
                join.getCondition(), join.getJoinType());
    }
}
