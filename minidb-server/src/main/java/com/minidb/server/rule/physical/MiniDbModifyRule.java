package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbModify;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableModify;

public final class MiniDbModifyRule extends ConverterRule {

    public MiniDbModifyRule() {
        this(
                ConverterRule.Config.INSTANCE
                        .withConversion(
                                LogicalTableModify.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbModifyRule")
                        .withRuleFactory(MiniDbModifyRule::new));
    }

    private MiniDbModifyRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableModify modify = (LogicalTableModify) rel;
        return new MiniDbModify(
                modify.getCluster(),
                modify.getTraitSet().replace(MiniDbConvention.INSTANCE),
                modify.getTable(),
                modify.getCatalogReader(),
                convert(modify.getInput(), MiniDbConvention.INSTANCE),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened());
    }
}
