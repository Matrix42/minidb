package com.minidb.server.rule;

import com.minidb.server.plan.MiniDbConvention;
import com.minidb.server.plan.MiniDbProject;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public final class MiniDbProjectRule extends ConverterRule {

    public MiniDbProjectRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalProject.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbProjectRule")
                .withRuleFactory(MiniDbProjectRule::new));
    }

    private MiniDbProjectRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;
        return new MiniDbProject(project.getCluster(),
                project.getTraitSet().replace(MiniDbConvention.INSTANCE),
                convert(project.getInput(), MiniDbConvention.INSTANCE),
                project.getProjects(), project.getRowType());
    }
}
