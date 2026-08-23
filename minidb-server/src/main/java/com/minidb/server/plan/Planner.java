package com.minidb.server.plan;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.calcite.Utf8SqlTypeFactory;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.logical.LogicalOptimizer;
import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbFilter;
import com.minidb.server.plan.physical.MiniDbProject;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.plan.physical.MiniDbScan;
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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.ArrayList;
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
        // root.project() 按 SELECT 的实际输出列裁剪:ORDER BY 引入的临时表达式列(如
        // SELECT * ORDER BY (a-b) 会把 a-b 作为额外列挂在 Sort 上)在此被 Project 裁掉,
        // 否则会泄露成输出列(坑:禁用 ProjectRemoveRule 保列别名,但该规则只按索引判
        // trivial、无法处理 ORDER BY 临时列的裁剪,故须在 plan 入口显式 project())。
        RelNode logical = root.project();
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
        // Phase 3: 谓词下推 + 列裁剪到 Scan——折叠 Filter(Scan)/Project(Scan) 进 Scan 自身,
        // 消除中间 Filter/Project 的逐行拷贝和全量列读取。
        return pushDownToScan(best);
    }

    /**
     * 遍历物理树,把 {@code MiniDbFilter(MiniDbScan)} 和
     * {@code MiniDbProject(MiniDbScan)} 折叠进 Scan 自身。
     * 多层下推递归处理,但 {@code Filter(Project(Scan))} 时只推 Filter 进 Scan,
     * 保留 Project 做列裁剪——若 Project 也折叠,Filter 需要的列可能被裁掉(如视图
     * {@code SELECT name WHERE id=2} 的 Project 裁掉 id,但 Filter 需要 id)。
     */
    private static RelNode pushDownToScan(RelNode node) {
        if (node instanceof MiniDbFilter filter) {
            // Filter(Project(Scan)):推 Filter 进 Scan,保留 Project
            RelNode filterChild = filter.getInput();
            if (filterChild instanceof MiniDbProject project) {
                RelNode projChild = pushDownToScan(project.getInput());
                if (projChild instanceof MiniDbScan scan) {
                    int[] cols = extractProjectColumns(project.getProjects());
                    RexNode cond = filter.getCondition();
                    if (cols != null) {
                        cond = remapToOriginal(cond, cols);
                    }
                    // 只推 Filter,不推 Project(列裁剪保留在 Project 中)
                    MiniDbScan newScan = new MiniDbScan(scan.getCluster(),
                            scan.getTraitSet(), scan.getTable(), null, cond);
                    return project.copy(project.getTraitSet(), newScan,
                            project.getProjects(), project.getRowType());
                }
                if (projChild != project.getInput()) {
                    RelNode newProj = project.copy(project.getTraitSet(), projChild,
                            project.getProjects(), project.getRowType());
                    return filter.copy(filter.getTraitSet(), newProj, filter.getCondition());
                }
            }
            // Filter(Scan):折叠 Filter 进 Scan
            RelNode child = pushDownToScan(filter.getInput());
            if (child instanceof MiniDbScan scan) {
                RexNode cond = filter.getCondition();
                if (scan.projectedColumns() != null) {
                    cond = remapToOriginal(cond, scan.projectedColumns());
                }
                return new MiniDbScan(scan.getCluster(), scan.getTraitSet(),
                        scan.getTable(), scan.projectedColumns(), cond);
            }
            if (child != filter.getInput()) {
                return filter.copy(filter.getTraitSet(), child, filter.getCondition());
            }
            return filter;
        }
        if (node instanceof MiniDbProject project) {
            RelNode child = pushDownToScan(project.getInput());
            if (child instanceof MiniDbScan scan && !scan.hasPushdown()) {
                // 纯列裁剪:Project 表达式全为 RexInputRef 时折叠进 Scan
                int[] cols = extractProjectColumns(project.getProjects());
                if (cols != null) {
                    return new MiniDbScan(scan.getCluster(), scan.getTraitSet(),
                            scan.getTable(), cols, null);
                }
            }
            if (child != project.getInput()) {
                return project.copy(project.getTraitSet(), child,
                        project.getProjects(), project.getRowType());
            }
            return project;
        }
        // 递归处理其他算子(Join/Aggregate/Sort 等)
        if (node.getInputs().isEmpty()) {
            return node;
        }
        List<RelNode> inputs = node.getInputs();
        List<RelNode> newInputs = null;
        for (int i = 0; i < inputs.size(); i++) {
            RelNode newChild = pushDownToScan(inputs.get(i));
            if (newChild != inputs.get(i)) {
                if (newInputs == null) {
                    newInputs = new ArrayList<>(inputs);
                }
                newInputs.set(i, newChild);
            }
        }
        if (newInputs == null) {
            return node;
        }
        return node.copy(node.getTraitSet(), newInputs);
    }

    /**
     * 若 project 表达式全是 RexInputRef(纯列索引),返回索引数组;否则返回 null。
     * 索引连续恒等(0,1,2,...)时也返回 null——不需要裁剪,调用方不建新 Scan。
     */
    /**
     * 将 RexNode 中的 RexInputRef 索引从「投影后位置」映射回「原始列索引」。
     * 当 Scan 先被 Project 裁剪列(如 [2,0]),后续 Filter 条件引用的是新索引(0→原列2),
     * 推入 Scan 时需还原为原索引。
     */
    private static RexNode remapToOriginal(RexNode node, int[] projectedColumns) {
        return node.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                int origIdx = projectedColumns[inputRef.getIndex()];
                return new RexInputRef(origIdx, inputRef.getType());
            }
        });
    }

    private static int[] extractProjectColumns(List<RexNode> projects) {
        int[] cols = new int[projects.size()];
        boolean identity = true;
        for (int i = 0; i < projects.size(); i++) {
            if (!(projects.get(i) instanceof RexInputRef ref)) {
                return null;
            }
            cols[i] = ref.getIndex();
            if (cols[i] != i) {
                identity = false;
            }
        }
        return identity ? null : cols;
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
