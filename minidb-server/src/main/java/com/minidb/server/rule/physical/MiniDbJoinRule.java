package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;
import com.minidb.server.plan.physical.MiniDbSortMergeJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

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
        RelTraitSet physicalTraits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
        RelNode leftInput = convert(join.getLeft(), MiniDbConvention.INSTANCE);
        RelNode rightInput = convert(join.getRight(), MiniDbConvention.INSTANCE);
        JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        if (!joinInfo.isEqui()) {
            return new MiniDbNestedLoopJoin(join.getCluster(), physicalTraits, leftInput, rightInput,
                    join.getCondition(), join.getJoinType());
        }
        RelMetadataQuery metadataQuery = RelMetadataQuery.instance();
        // Query collations on the logical inputs: the converted children are
        // RelSubsets whose trait set only reports the requested traits (empty
        // collation); RelMdCollation on the logical tree propagates the
        // ORDER BY collation through Sort/Project, which the physical
        // conversion preserves.
        boolean leftInputSorted = MiniDbJoin.coversKeys(
                metadataQuery.collations(join.getLeft()), joinInfo.leftKeys);
        boolean rightInputSorted = MiniDbJoin.coversKeys(
                metadataQuery.collations(join.getRight()), joinInfo.rightKeys);
        if (leftInputSorted && rightInputSorted) {
            return new MiniDbSortMergeJoin(join.getCluster(), physicalTraits, leftInput, rightInput,
                    join.getCondition(), join.getJoinType());
        }
        return new MiniDbHashJoin(join.getCluster(), physicalTraits, leftInput, rightInput,
                join.getCondition(), join.getJoinType());
    }
}
