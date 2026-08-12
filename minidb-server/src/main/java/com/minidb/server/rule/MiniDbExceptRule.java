package com.minidb.server.rule;

import com.minidb.server.plan.MiniDbConvention;
import com.minidb.server.plan.MiniDbSetOp;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalMinus;

public final class MiniDbExceptRule extends ConverterRule {

    public MiniDbExceptRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalMinus.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbExceptRule")
                .withRuleFactory(MiniDbExceptRule::new));
    }

    private MiniDbExceptRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalMinus op = (LogicalMinus) rel;
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode in : op.getInputs()) {
            inputs.add(convert(in, MiniDbConvention.INSTANCE));
        }
        return new MiniDbSetOp(op.getCluster(),
                op.getTraitSet().replace(MiniDbConvention.INSTANCE),
                inputs, op.kind, op.all);
    }
}
