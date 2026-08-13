package com.minidb.server.plan;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.logical.LogicalOptimizer;
import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.rule.physical.MiniDbPhysicalRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

public class Planner {

    private final CalciteContext calcite;

    public Planner(MiniDbCatalog catalog) {
        this.calcite = new CalciteContext(catalog);
    }

    public RelNode plan(String sql) {
        return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public RelNode plan(String sql, String currentSchema) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        for (RelOptRule rule : MiniDbPhysicalRules.ALL) {
            planner.addRule(rule);
        }
        SqlTypeFactoryImpl typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        RelRoot root = calcite.planInCluster(sql, cluster, currentSchema);
        RelNode logical = root.rel;
        // Phase 1: logical optimization (HepPlanner over Calcite Logical* tree)
        RelNode optimized = LogicalOptimizer.optimize(logical);
        // Phase 2: physical conversion (VolcanoPlanner)
        RelNode converted = planner.changeTraits(optimized,
                optimized.getTraitSet().replace(MiniDbConvention.INSTANCE));
        planner.setRoot(converted);
        RelNode best = planner.findBestExp();
        if (!(best instanceof MiniDbRel)) {
            throw new IllegalStateException(
                    "planner produced non-physical root: " + best);
        }
        return best;
    }
}
