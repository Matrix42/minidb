package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Nested-loop join handles any condition (equi or not), so this rule has no equi pre-check; it is
 * the fallback when the Hash/SortMerge rules decline.
 */
public final class MiniDbNestedLoopJoinRule extends ConverterRule {

    public MiniDbNestedLoopJoinRule() {
        this(
                Config.INSTANCE
                        .withConversion(
                                LogicalJoin.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbNestedLoopJoinRule")
                        .withRuleFactory(MiniDbNestedLoopJoinRule::new));
    }

    private MiniDbNestedLoopJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        RelTraitSet traits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
        return new MiniDbNestedLoopJoin(
                join.getCluster(),
                traits,
                convert(join.getLeft(), MiniDbConvention.INSTANCE),
                convert(join.getRight(), MiniDbConvention.INSTANCE),
                join.getCondition(),
                join.getJoinType());
    }
}
