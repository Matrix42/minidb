package com.minidb.server.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortRemoveDuplicateKeysRule;
import org.apache.calcite.rel.rules.SortRemoveRedundantRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionEliminatorRule;

public final class MiniDbLogicalRules {

    /** HepPlanner 阶段规则(不依赖 RelCollationTraitDef)。
     *  注意:
     *  <ul>
     *    <li>不含 ProjectRemoveRule / CalcRemoveRule —— 两者按「索引恒等」判 trivial,会删掉改名节点(SELECT a.id AS aid),丢失列别名。</li>
     *    <li>换位规则(FilterSetOpTranspose/ProjectSetOpTranspose)放在 ToCalc 之前,先在 Filter/Project 节点上触发再转 Calc。</li>
     *  </ul> */
    public static final List<RelOptRule> HEP = List.of(
            // 投影/过滤化简与换位(先于 Calc 转换,作用于 Filter/Project 节点)
            ProjectMergeRule.Config.DEFAULT.toRule(),
            FilterMergeRule.Config.DEFAULT.toRule(),
            FilterProjectTransposeRule.Config.DEFAULT.toRule(),
            new FilterJoinRule.FilterIntoJoinRule(false, RelFactories.LOGICAL_BUILDER,
                    FilterJoinRule.TRUE_PREDICATE),
            FilterSetOpTransposeRule.Config.DEFAULT.toRule(),
            ProjectSetOpTransposeRule.Config.DEFAULT.toRule(),
            // Calc 转换与合并(Project+Filter → 单个 Calc)
            ProjectToCalcRule.Config.DEFAULT.toRule(),
            FilterToCalcRule.Config.DEFAULT.toRule(),
            CalcMergeRule.Config.DEFAULT.toRule(),
            FilterCalcMergeRule.Config.DEFAULT.toRule(),
            ProjectCalcMergeRule.Config.DEFAULT.toRule(),
            // set op 化简
            UnionEliminatorRule.Config.DEFAULT.toRule(),
            // 排序化简(常量/重复/冗余键,不依赖 trait)
            SortRemoveConstantKeysRule.Config.DEFAULT.toRule(),
            SortRemoveDuplicateKeysRule.Config.DEFAULT.toRule(),
            SortRemoveRedundantRule.Config.DEFAULT.toRule());

    /** VolcanoPlanner 阶段规则:依赖 RelCollationTraitDef。
     *  SortRemoveRule 在输入已满足 collation 时移除冗余 Sort,只在 trait 注册(VolcanoPlanner)时触发。 */
    public static final List<RelOptRule> SORT = List.of(
            SortRemoveRule.Config.DEFAULT.toRule());

    private MiniDbLogicalRules() {
    }
}
