package com.minidb.server.plan;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.calcite.Utf8SqlTypeFactory;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.logical.LogicalOptimizer;
import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.rule.logical.MiniDbLogicalRules;
import com.minidb.server.rule.physical.MiniDbPhysicalRules;
import org.apache.calcite.DataContexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.List;

public class Planner {

    private final CalciteContext calcite;

    public Planner(MiniDbCatalog catalog) {
        this.calcite = new CalciteContext(catalog);
    }

    public RelNode plan(String sql) {
        return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public RelNode plan(String sql, String currentSchema) {
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        // 常量折叠/条件简化(ReduceExpressionsRule)依赖 RexExecutor 真正求值常量表达式;
        // 默认的 RexUtil.EXECUTOR 只能 reduce 很有限的东西(如 CAST(字面量)),挂上
        // RexExecutorImpl(经 Janino 编译求值)才能折叠 1+2 这类算术。
        volcanoPlanner.setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        for (RelOptRule rule : MiniDbPhysicalRules.ALL) {
            volcanoPlanner.addRule(rule);
        }
        // Sort simplification rules need RelCollationTraitDef, which the
        // HepPlanner cannot register (its addRelTraitDef is a no-op), so they
        // run here in the VolcanoPlanner alongside the converter rules.
        for (RelOptRule rule : MiniDbLogicalRules.SORT) {
            volcanoPlanner.addRule(rule);
        }
        // Join reordering: commute/associate multi-table joins by row-count cost
        // (RelMetadataQuery.getRowCount supplies the table sizes from Phase 1).
        // 注:去相关后含 exists 的复杂 join 树在重排时可能丢失等值条件变交叉连接
        // (query10/18 等 OOM),故禁用 commute/associate,保持去相关后的 join 顺序。
        // volcanoPlanner.addRule(JoinCommuteRule.Config.DEFAULT.toRule());
        // volcanoPlanner.addRule(JoinAssociateRule.Config.DEFAULT.toRule());
        SqlTypeFactoryImpl typeFactory =
                new Utf8SqlTypeFactory(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(typeFactory));

        // 视图展开器:共享本次规划的 VolcanoPlanner 与 typeFactory,保证视图内展开的 RelNode
        // 与外部树 traitSet 一致(否则 HepPlanner 新建的 cluster 缺 convention/collation 分量,
        // changeTraits 时 trait 不匹配抛 AssertionError,见坑 38)。
        RelOptTable.ViewExpander viewExpander =
                new ViewExpander(volcanoPlanner, typeFactory, calcite, currentSchema);
        RelRoot root = calcite.planInCluster(sql, cluster, currentSchema, viewExpander);
        RelNode logical = root.rel;
        // Phase 1: logical optimization (HepPlanner over Calcite Logical* tree)
        RelNode optimized = LogicalOptimizer.optimize(logical);
        // Phase 2: physical conversion (VolcanoPlanner)
        RelNode converted = volcanoPlanner.changeTraits(optimized,
                optimized.getTraitSet().replace(MiniDbConvention.INSTANCE));
        volcanoPlanner.setRoot(converted);
        RelNode best = volcanoPlanner.findBestExp();
        if (!(best instanceof MiniDbRel)) {
            throw new IllegalStateException(
                    "planner produced non-physical root: " + best);
        }
        return best;
    }

    /** 展开视图定义 SQL:取 schemaPath 最后一段作视图所在 schema,复用共享 planner 重新 plan。 */
    private static final class ViewExpander implements RelOptTable.ViewExpander {
        private final VolcanoPlanner planner;
        private final SqlTypeFactoryImpl typeFactory;
        private final CalciteContext calcite;
        private final String fallbackSchema;

        ViewExpander(VolcanoPlanner planner, SqlTypeFactoryImpl typeFactory,
                     CalciteContext calcite, String fallbackSchema) {
            this.planner = planner;
            this.typeFactory = typeFactory;
            this.calcite = calcite;
            this.fallbackSchema = fallbackSchema;
        }

        @Override
        public RelRoot expandView(RelDataType rowType, String queryString,
                                  List<String> schemaPath, List<String> viewPath) {
            String schema = schemaPath.isEmpty()
                    ? fallbackSchema
                    : schemaPath.get(schemaPath.size() - 1);
            RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
            return calcite.planInCluster(queryString, cluster, schema, this);
        }
    }
}
