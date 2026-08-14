package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbSortMergeJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Produces a {@link MiniDbSortMergeJoin} for any equi-join. Whether each input
 * is actually pre-sorted is decided by the join's own constructor (via
 * {@code MiniDbJoin.coversKeys} on the logical collations), which feeds the
 * internal-sort cost term in {@code computeSelfCost}.
 */
public final class MiniDbSortMergeJoinRule extends ConverterRule {

    public MiniDbSortMergeJoinRule() {
        this(Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE, MiniDbConvention.INSTANCE, "MiniDbSortMergeJoinRule")
                .withRuleFactory(MiniDbSortMergeJoinRule::new));
    }

    private MiniDbSortMergeJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        if (!info.isEqui() || info.leftKeys.isEmpty()) {
            return null;
        }
        RelTraitSet traits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
        return new MiniDbSortMergeJoin(join.getCluster(), traits,
                convert(join.getLeft(), MiniDbConvention.INSTANCE),
                convert(join.getRight(), MiniDbConvention.INSTANCE),
                join.getCondition(), join.getJoinType());
    }
}
