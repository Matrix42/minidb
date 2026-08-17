package com.minidb.server.plan.logical;

import com.minidb.server.rule.logical.MiniDbLogicalRules;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.sql2rel.RelDecorrelator;

public final class LogicalOptimizer {

    private LogicalOptimizer() {
    }

    /** Runs the logical optimization rules over the Calcite Logical* tree. */
    public static RelNode optimize(RelNode logical) {
        RelNode decorrelated = decorrelateSubqueries(logical);
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleCollection(MiniDbLogicalRules.HEP);
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build());
        hepPlanner.setRoot(decorrelated);
        RelNode optimized = hepPlanner.findBestExp();
        // 重排 INNER join 链,消除 FROM 顺序导致的交叉连接(见 JoinReorderer)。
        return JoinReorderer.reorder(optimized);
    }

    /**
     * 移除 EXISTS/IN/标量子查询:先 SubQueryRemoveRule 把嵌在 Filter/Project 条件里的
     * RexSubQuery 提升为 LogicalCorrelate,再 RelDecorrelator 把 Correlate 去相关为普通
     * join/聚合(EXISTS → INNER join + 聚合去重;NOT EXISTS → LEFT join + IS NULL 过滤)。
     * 无子查询的查询两阶段都原样返回,不受影响。
     */
    private static RelNode decorrelateSubqueries(RelNode logical) {
        HepPlanner subQueryPlanner = new HepPlanner(new HepProgramBuilder()
                .addRuleInstance(SubQueryRemoveRule.Config.FILTER.toRule())
                .addRuleInstance(SubQueryRemoveRule.Config.PROJECT.toRule())
                .addRuleInstance(SubQueryRemoveRule.Config.JOIN.toRule())
                .build());
        subQueryPlanner.setRoot(logical);
        RelNode correlated = subQueryPlanner.findBestExp();
        return RelDecorrelator.decorrelateQuery(correlated,
                RelFactories.LOGICAL_BUILDER.create(correlated.getCluster(), null));
    }
}
