package com.minidb.server.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortRemoveDuplicateKeysRule;
import org.apache.calcite.rel.rules.SortRemoveRedundantRule;
import org.apache.calcite.rel.rules.SortRemoveRule;

public final class MiniDbLogicalRules {

    /** HepPlanner 阶段规则(不依赖 RelCollationTraitDef)。
     *  注意:
     *  <ul>
     *    <li>不含 ProjectRemoveRule —— 它会删掉改名投影(SELECT a.id AS aid),丢失客户端可见的列别名。</li>
     *    <li>SortRemoveConstantKeysRule 因 CALCITE_6611 未修复,只在 trait 未注册(HepPlanner)时触发;
     *        常量排序键(如 WHERE id=1 ORDER BY id)的 Sort 在这里被移除。</li>
     *  </ul> */
    public static final List<RelOptRule> HEP = List.of(
            ProjectMergeRule.Config.DEFAULT.toRule(),
            FilterMergeRule.Config.DEFAULT.toRule(),
            FilterProjectTransposeRule.Config.DEFAULT.toRule(),
            new FilterJoinRule.FilterIntoJoinRule(false, RelFactories.LOGICAL_BUILDER,
                    FilterJoinRule.TRUE_PREDICATE),
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
