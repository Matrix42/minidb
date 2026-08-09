package com.minidb.server.plan;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;

public final class MiniDbRules {

    public static final List<RelOptRule> ALL = List.of(
            ConverterRule.Config.INSTANCE
                    .withConversion(LogicalTableScan.class, Convention.NONE,
                            MiniDbConvention.INSTANCE, "MiniDbScanRule")
                    .withRuleFactory(config -> new ConverterRule(config) {
                        @Override
                        public RelNode convert(RelNode rel) {
                            LogicalTableScan scan = (LogicalTableScan) rel;
                            return new MiniDbScan(scan.getCluster(),
                                    scan.getTraitSet().replace(MiniDbConvention.INSTANCE),
                                    scan.getTable());
                        }
                    }).toRule(),
            ConverterRule.Config.INSTANCE
                    .withConversion(LogicalFilter.class, Convention.NONE,
                            MiniDbConvention.INSTANCE, "MiniDbFilterRule")
                    .withRuleFactory(config -> new ConverterRule(config) {
                        @Override
                        public RelNode convert(RelNode rel) {
                            LogicalFilter filter = (LogicalFilter) rel;
                            return new MiniDbFilter(filter.getCluster(),
                                    filter.getTraitSet().replace(MiniDbConvention.INSTANCE),
                                    convert(filter.getInput(), MiniDbConvention.INSTANCE),
                                    filter.getCondition());
                        }
                    }).toRule(),
            ConverterRule.Config.INSTANCE
                    .withConversion(LogicalProject.class, Convention.NONE,
                            MiniDbConvention.INSTANCE, "MiniDbProjectRule")
                    .withRuleFactory(config -> new ConverterRule(config) {
                        @Override
                        public RelNode convert(RelNode rel) {
                            LogicalProject project = (LogicalProject) rel;
                            return new MiniDbProject(project.getCluster(),
                                    project.getTraitSet().replace(MiniDbConvention.INSTANCE),
                                    convert(project.getInput(), MiniDbConvention.INSTANCE),
                                    project.getProjects(), project.getRowType());
                        }
                    }).toRule(),
            ConverterRule.Config.INSTANCE
                    .withConversion(LogicalSort.class, Convention.NONE,
                            MiniDbConvention.INSTANCE, "MiniDbSortRule")
                    .withRuleFactory(config -> new ConverterRule(config) {
                        @Override
                        public RelNode convert(RelNode rel) {
                            LogicalSort sort = (LogicalSort) rel;
                            return new MiniDbSort(sort.getCluster(),
                                    sort.getTraitSet().replace(MiniDbConvention.INSTANCE),
                                    convert(sort.getInput(), MiniDbConvention.INSTANCE),
                                    sort.getCollation(), sort.offset, sort.fetch);
                        }
                    }).toRule(),
            ConverterRule.Config.INSTANCE
                    .withConversion(LogicalValues.class, Convention.NONE,
                            MiniDbConvention.INSTANCE, "MiniDbValuesRule")
                    .withRuleFactory(config -> new ConverterRule(config) {
                        @Override
                        public RelNode convert(RelNode rel) {
                            LogicalValues values = (LogicalValues) rel;
                            return new MiniDbValues(values.getCluster(),
                                    values.getTraitSet().replace(MiniDbConvention.INSTANCE),
                                    values.getRowType(), values.getTuples());
                        }
                    }).toRule(),
            ConverterRule.Config.INSTANCE
                    .withConversion(LogicalTableModify.class, Convention.NONE,
                            MiniDbConvention.INSTANCE, "MiniDbModifyRule")
                    .withRuleFactory(config -> new ConverterRule(config) {
                        @Override
                        public RelNode convert(RelNode rel) {
                            LogicalTableModify modify = (LogicalTableModify) rel;
                            return new MiniDbModify(modify.getCluster(),
                                    modify.getTraitSet().replace(MiniDbConvention.INSTANCE),
                                    modify.getTable(),
                                    modify.getCatalogReader(),
                                    convert(modify.getInput(), MiniDbConvention.INSTANCE),
                                    modify.getOperation(),
                                    modify.getUpdateColumnList(),
                                    modify.getSourceExpressionList(),
                                    modify.isFlattened());
                        }
                    }).toRule());

    private MiniDbRules() {
    }
}
