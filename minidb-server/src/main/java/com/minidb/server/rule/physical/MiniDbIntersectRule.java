package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbSetOp;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalIntersect;

public final class MiniDbIntersectRule extends ConverterRule {

    public MiniDbIntersectRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalIntersect.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbIntersectRule")
                .withRuleFactory(MiniDbIntersectRule::new));
    }

    private MiniDbIntersectRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalIntersect op = (LogicalIntersect) rel;
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode in : op.getInputs()) {
            inputs.add(convert(in, MiniDbConvention.INSTANCE));
        }
        return new MiniDbSetOp(op.getCluster(),
                op.getTraitSet().replace(MiniDbConvention.INSTANCE),
                inputs, op.kind, op.all);
    }
}
