package com.minidb.server.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterSortTransposeRule;
import org.apache.calcite.rel.rules.ProjectAggregateMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortRemoveDuplicateKeysRule;
import org.apache.calcite.rel.rules.SortRemoveRedundantRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionEliminatorRule;

public final class MiniDbLogicalRules {

    /** HepPlanner 阶段规则(不依赖 RelCollationTraitDef),Calcite 标准 Filter/Project 路径。
     *  注意:不含 ProjectRemoveRule / CalcRemoveRule —— 两者按「索引恒等」判 trivial,会删掉改名节点
     *  (SELECT a.id AS aid),丢失 JDBC 可见的列别名。 */
    public static final List<RelOptRule> HEP = List.of(
            // 常量折叠 + 条件简化(依赖 RexExecutor,见 Planner)
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            // 投影/过滤化简与换位
            ProjectMergeRule.Config.DEFAULT.toRule(),
            FilterMergeRule.Config.DEFAULT.toRule(),
            FilterProjectTransposeRule.Config.DEFAULT.toRule(),
            new FilterJoinRule.FilterIntoJoinRule(false, RelFactories.LOGICAL_BUILDER,
                    FilterJoinRule.TRUE_PREDICATE),
            FilterSetOpTransposeRule.Config.DEFAULT.toRule(),
            ProjectSetOpTransposeRule.Config.DEFAULT.toRule(),
            // 聚合:过滤下推 + 与相邻 Project 合并 + 移除无用聚合
            FilterAggregateTransposeRule.Config.DEFAULT.toRule(),
            ProjectAggregateMergeRule.Config.DEFAULT.toRule(),
            AggregateProjectMergeRule.Config.DEFAULT.toRule(),
            AggregateRemoveRule.Config.DEFAULT.toRule(),
            // 排序:换位(便于 SortRemove 找到有序输入)+ 化简
            FilterSortTransposeRule.Config.DEFAULT.toRule(),
            SortProjectTransposeRule.Config.DEFAULT.toRule(),
            SortRemoveConstantKeysRule.Config.DEFAULT.toRule(),
            // SortRemoveDuplicateKeysRule 会查 RelMdFunctionalDependency,在含聚合的排序上触发
            // Calcite 1.42 的 Mappings 越界 bug(query12/20/34/36),故禁用(仅失去排序去重优化)。
            SortRemoveRedundantRule.Config.DEFAULT.toRule(),
            // set op 化简
            UnionEliminatorRule.Config.DEFAULT.toRule());

    /** VolcanoPlanner 阶段规则:依赖 RelCollationTraitDef。
     *  SortRemoveRule 在输入已满足 collation 时移除冗余 Sort,只在 trait 注册(VolcanoPlanner)时触发。 */
    public static final List<RelOptRule> SORT = List.of(
            SortRemoveRule.Config.DEFAULT.toRule());

    private MiniDbLogicalRules() {
    }
}
