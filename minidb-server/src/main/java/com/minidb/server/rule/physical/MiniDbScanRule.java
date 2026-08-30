package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbScan;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

public final class MiniDbScanRule extends ConverterRule {

    public MiniDbScanRule() {
        this(
                ConverterRule.Config.INSTANCE
                        .withConversion(
                                LogicalTableScan.class,
                                Convention.NONE,
                                MiniDbConvention.INSTANCE,
                                "MiniDbScanRule")
                        .withRuleFactory(MiniDbScanRule::new));
    }

    private MiniDbScanRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan scan = (LogicalTableScan) rel;
        return new MiniDbScan(
                scan.getCluster(),
                scan.getTraitSet().replace(MiniDbConvention.INSTANCE),
                scan.getTable());
    }
}
