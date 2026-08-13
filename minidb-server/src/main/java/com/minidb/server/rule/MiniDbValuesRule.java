package com.minidb.server.rule;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbValues;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;

public final class MiniDbValuesRule extends ConverterRule {

    public MiniDbValuesRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalValues.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbValuesRule")
                .withRuleFactory(MiniDbValuesRule::new));
    }

    private MiniDbValuesRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalValues values = (LogicalValues) rel;
        return new MiniDbValues(values.getCluster(),
                values.getTraitSet().replace(MiniDbConvention.INSTANCE),
                values.getRowType(), values.getTuples());
    }
}
