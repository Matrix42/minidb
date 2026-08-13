package com.minidb.server.rule;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbSort;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

public final class MiniDbSortRule extends ConverterRule {

    public MiniDbSortRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalSort.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbSortRule")
                .withRuleFactory(MiniDbSortRule::new));
    }

    private MiniDbSortRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalSort sort = (LogicalSort) rel;
        return new MiniDbSort(sort.getCluster(),
                sort.getTraitSet().replace(MiniDbConvention.INSTANCE),
                convert(sort.getInput(), MiniDbConvention.INSTANCE),
                sort.getCollation(), sort.offset, sort.fetch);
    }
}
