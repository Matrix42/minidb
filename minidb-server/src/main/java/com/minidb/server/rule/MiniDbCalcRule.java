package com.minidb.server.rule;

import com.minidb.server.plan.MiniDbCalc;
import com.minidb.server.plan.MiniDbConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

public final class MiniDbCalcRule extends ConverterRule {

    public MiniDbCalcRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalCalc.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbCalcRule")
                .withRuleFactory(MiniDbCalcRule::new));
    }

    private MiniDbCalcRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalCalc calc = (LogicalCalc) rel;
        return new MiniDbCalc(calc.getCluster(),
                calc.getTraitSet().replace(MiniDbConvention.INSTANCE),
                convert(calc.getInput(), MiniDbConvention.INSTANCE),
                calc.getProgram());
    }
}
