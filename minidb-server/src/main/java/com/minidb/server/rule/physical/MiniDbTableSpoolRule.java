package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbTableSpool;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableSpool;

public final class MiniDbTableSpoolRule extends ConverterRule {

    public MiniDbTableSpoolRule() {
        this(
                ConverterRule.Config.INSTANCE
                        .withConversion(
                                LogicalTableSpool.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbTableSpoolRule")
                        .withRuleFactory(MiniDbTableSpoolRule::new));
    }

    private MiniDbTableSpoolRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableSpool spool = (LogicalTableSpool) rel;
        RelNode input = convert(spool.getInput(), MiniDbConvention.INSTANCE);
        return new MiniDbTableSpool(
                spool.getCluster(),
                spool.getTraitSet().replace(MiniDbConvention.INSTANCE),
                input,
                spool.readType,
                spool.writeType,
                spool.getTable());
    }
}
