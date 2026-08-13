package com.minidb.server.plan.logical;

import com.minidb.server.rule.logical.MiniDbLogicalRules;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;

public final class LogicalOptimizer {

    private LogicalOptimizer() {
    }

    /** Runs the logical optimization rules over the Calcite Logical* tree. */
    public static RelNode optimize(RelNode logical) {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleCollection(MiniDbLogicalRules.HEP);
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build());
        hepPlanner.setRoot(logical);
        return hepPlanner.findBestExp();
    }
}
