package com.minidb.server.plan;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.calcite.Utf8SqlTypeFactory;
import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.common.MVDefinition;
import com.minidb.server.plan.logical.LogicalOptimizer;
import com.minidb.server.plan.physical.MiniDbAggregate;
import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbCse;
import com.minidb.server.plan.physical.MiniDbFilter;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbProject;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.plan.physical.MiniDbScan;
import com.minidb.server.plan.physical.MiniDbSort;
import com.minidb.server.rule.logical.MiniDbLogicalRules;
import com.minidb.server.rule.physical.MiniDbPhysicalRules;
import org.apache.calcite.DataContexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Planner {

    private final MiniDbCatalog catalog;
    private final CalciteContext calcite;

    public Planner(MiniDbCatalog catalog) {
        this.catalog = catalog;
        this.calcite = new CalciteContext(catalog);
    }

    public RelNode plan(String sql) {
        return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA, true);
    }

    public RelNode plan(String sql, String currentSchema) {
        return plan(sql, currentSchema, true);
    }

    /**
     * 规划查询但不做物化视图重写。重算 MV 定义查询(REFRESH / DML 自动刷新 / 初始填充)时必须
     * 用它:若带重写,定义查询会被重写为扫描 MV 自身——而此刻 MV 表刚被清空/尚未填充,结果恒空,
     * 且构成自引用。用户查询走 {@link #plan(String, String)} 保持重写开启。
     */
    public RelNode planWithoutMvRewrite(String sql, String currentSchema) {
        return plan(sql, currentSchema, false);
    }

    private RelNode plan(String sql, String currentSchema, boolean mvRewrite) {
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
        // Phase 1.5: 物化视图查询重写——与 MV 定义结构一致的查询子树替换为对 MV 表的扫描。
        // 仅用户查询开启;重算 MV 自身定义时关闭(见 planWithoutMvRewrite)。
        RelNode rewritten = mvRewrite
                ? rewriteWithMaterializedViews(optimized, currentSchema)
                : optimized;
        // Phase 2: physical conversion (VolcanoPlanner)
        RelNode converted = volcanoPlanner.changeTraits(rewritten,
                rewritten.getTraitSet().replace(MiniDbConvention.INSTANCE));
        volcanoPlanner.setRoot(converted);
        RelNode best = volcanoPlanner.findBestExp();
        if (!(best instanceof MiniDbRel)) {
            throw new IllegalStateException(
                    "planner produced non-physical root: " + best);
        }
        // Phase 3: 谓词下推 + 列裁剪到 Scan
        RelNode pushed = pushDownToScan(best);
        // Phase 4: 中间结果列裁剪——自顶向下传需求,每层 join/Project 输出只保留被引用的列
        // (query64 等大 join 链每层物化全列,裁剪后列数大减)。在 CSE 之前做:裁剪使
        // 两分支列索引趋于一致(独立裁剪由相同的上层需求驱动),CSE digest 更可能匹配。
        RelNode pruned = pruneJoinColumns(pushed);
        // Phase 5: 公共子表达式消除(CSE)——相同子树只执行一次,后续缓存回放
        return deduplicateSubtrees(pruned);
    }

    /**
     * CSE:自底向上遍历,对重复的 Join/Aggregate 子树(如 query65 的 ss⨝date_dim 出现两次)
     * 包装为共享 {@link MiniDbCse} 节点。首次执行物化到缓存,后续命中回放。
     * 只对 Join/Aggregate 做——它们是昂贵操作,Scan/Filter/Project 已下推无需 CSE。
     */
    private static RelNode deduplicateSubtrees(RelNode node) {        Map<String, Integer> counts = new HashMap<>();
        Map<String, MiniDbCse> cseMap = new HashMap<>();
        countJoinsAndAggregates(node, counts);
        return deduplicateSubtrees(node, counts, cseMap);
    }

    /** 第一轮:统计 Join/Aggregate 子树的出现次数。 */
    private static void countJoinsAndAggregates(RelNode node, Map<String, Integer> counts) {
        if (node instanceof MiniDbJoin || node instanceof MiniDbAggregate) {
            counts.merge(structuralDigest(node), 1, Integer::sum);
        }
        for (RelNode input : node.getInputs()) {
            countJoinsAndAggregates(input, counts);
        }
    }

    /**
     * 计算子树的结构哈希(不含 RelNode 实例 ID)。Join/Aggregate 递归包含子节点哈希,
     * 其他节点用 explainTerms 去掉实例 ID 的部分。
     */
    private static String structuralDigest(RelNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getClass().getSimpleName());
        // 用 explainTerms 输出,去掉 #id 后缀
        sb.append('{').append(inputStrippedDigest(node)).append('}');
        // 追加子节点信息
        for (RelNode input : node.getInputs()) {
            if (input instanceof MiniDbJoin || input instanceof MiniDbAggregate) {
                sb.append('(').append(structuralDigest(input)).append(')');
            } else {
                sb.append('[').append(inputStrippedDigest(input)).append(']');
            }
        }
        return sb.toString();
    }

    /** 节点的 explainTerms 去掉 RelNode 实例 ID(#id) 后的摘要。 */
    private static String inputStrippedDigest(RelNode node) {
        String full = node.getDigest();
        return full.replaceAll("#\\d+", "");
    }

    /**
     * 第二轮:对出现次数 >1 的 Join/Aggregate 子树,首次出现包装 MiniDbCse,
     * 后续出现替换为共享的 MiniDbCse。
     */
    private static RelNode deduplicateSubtrees(RelNode node, Map<String, Integer> counts,
                                                Map<String, MiniDbCse> cseMap) {
        // 先递归处理子节点
        List<RelNode> inputs = node.getInputs();
        List<RelNode> newInputs = null;
        for (int i = 0; i < inputs.size(); i++) {
            RelNode newChild = deduplicateSubtrees(inputs.get(i), counts, cseMap);
            if (newChild != inputs.get(i)) {
                if (newInputs == null) {
                    newInputs = new ArrayList<>(inputs);
                }
                newInputs.set(i, newChild);
            }
        }
        if (newInputs != null) {
            node = node.copy(node.getTraitSet(), newInputs);
        }
        if (!(node instanceof MiniDbJoin) && !(node instanceof MiniDbAggregate)) {
            return node;
        }
        String digest = structuralDigest(node);
        if (counts.getOrDefault(digest, 0) <= 1) {
            return node; // 唯一子树,不包装
        }
        MiniDbCse existing = cseMap.get(digest);
        if (existing != null) {
            return existing; // 重复子树:返回已缓存的 CSE 节点
        }
        // 首次出现:包装 CSE 节点
        MiniDbCse cse = new MiniDbCse(node.getCluster(), node.getTraitSet(), node, digest);
        cseMap.put(digest, cse);
        return cse;
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

    // ---- 中间结果列裁剪(Phase 4) ----

    /**
     * 中间结果列裁剪:自顶向下传「上层需要的输出列」,自底向上重建——每层
     * Project/Join/Scan/Sort/Filter/Aggregate 输出只保留被引用的列。query64 等大
     * join 链每层物化全列(20+ 列 × 120 万行),裁剪后中间结果列数大减,物化/拷贝/
     * 哈希成本随之下降。
     */
    private static RelNode pruneJoinColumns(RelNode root) {
        return pruneColumns(root, identityList(root.getRowType().getFieldCount()));
    }

    /** 裁剪结果:重建后的节点 + 输出列序(第 i 列对应原输出列 outputOrder[i])。 */
    private record Pruned(RelNode node, List<Integer> outputOrder) {}

    private static RelNode pruneColumns(RelNode node, List<Integer> neededCols) {
        Pruned p;
        if (node instanceof MiniDbProject project) {
            p = pruneProject(project, neededCols);
        } else if (node instanceof MiniDbJoin join) {
            p = pruneJoin(join, neededCols);
        } else if (node instanceof MiniDbScan scan) {
            p = pruneScan(scan, neededCols);
        } else if (node instanceof MiniDbSort sort) {
            p = pruneSort(sort, neededCols);
        } else if (node instanceof MiniDbFilter filter) {
            p = pruneFilter(filter, neededCols);
        } else if (node instanceof MiniDbAggregate aggregate) {
            p = pruneAggregate(aggregate);
        } else {
            // 其他节点不支持裁剪;递归输入传全列,自身原样(列序 = 原序)
            RelNode result = node;
            if (node.getInputs().size() == 1) {
                RelNode child = pruneColumns(node.getInput(0),
                        identityList(node.getInput(0).getRowType().getFieldCount()));
                if (child != node.getInput(0)) {
                    result = node.copy(node.getTraitSet(), List.of(child));
                }
            }
            p = new Pruned(result, identityList(node.getRowType().getFieldCount()));
        }
        // 统一归一化:输出列序必须 = neededCols 序,否则插 Project
        return ensureProject(p.node(), p.outputOrder(), node.getRowType(), neededCols);
    }

    /**
     * 输出序归一化:outputOrder 是裁剪后节点输出列序(第 i 列对应原输出列 outputOrder[i])。
     * 若输出序 != neededCols 序(如 join 的条件列、Filter/Sort 的条件引用列混在输出中),
     * 插 MiniDbProject 把输出裁剪为 neededCols 序;序一致则原样返回。
     */
    private static RelNode ensureProject(RelNode node, List<Integer> outputOrder,
                                         RelDataType originalRowType, List<Integer> neededCols) {
        if (outputOrder.equals(neededCols)) {
            return node;
        }
        int[] pos = new int[neededCols.size()];
        for (int k = 0; k < neededCols.size(); k++) {
            pos[k] = outputOrder.indexOf(neededCols.get(k));
            if (pos[k] < 0) {
                throw new IllegalStateException("列裁剪后仍被引用: " + neededCols.get(k));
            }
        }
        List<RexNode> exprs = new ArrayList<>(neededCols.size());
        for (int k = 0; k < neededCols.size(); k++) {
            RelDataTypeField field = node.getRowType().getFieldList().get(pos[k]);
            exprs.add(new RexInputRef(pos[k], field.getType()));
        }
        RelDataTypeFactory tf = node.getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = tf.builder();
        for (int k : neededCols) {
            RelDataTypeField field = originalRowType.getFieldList().get(k);
            builder.add(field.getName(), field.getType());
        }
        return new MiniDbProject(node.getCluster(), node.getTraitSet(), node, exprs, builder.build());
    }

    private static Pruned pruneScan(MiniDbScan scan, List<Integer> neededCols) {
        int[] existing = scan.projectedColumns();
        int n = scan.getRowType().getFieldCount();
        if (isIdentity(neededCols, n)) {
            return new Pruned(scan, neededCols);
        }
        // 统一到表列空间:neededCols 引用当前输出列(投影位置或表列),pushedFilter 恒引用表列
        TreeSet<Integer> tableCols = new TreeSet<>();
        for (int k : neededCols) {
            tableCols.add(existing == null ? k : existing[k]);
        }
        if (scan.pushedFilter() != null) {
            // pushedFilter 引用的表列必须保留在投影里(applyPushdown 用它过滤),
            // 否则 remapToProjected 越界
            collectRefs(scan.pushedFilter(), tableCols);
        }
        int[] newProj = new int[tableCols.size()];
        int idx = 0;
        for (int col : tableCols) {
            newProj[idx++] = col;
        }
        if (existing != null && Arrays.equals(newProj, existing)) {
            return new Pruned(scan, toList(existing));
        }
        RelNode newScan = new MiniDbScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(),
                newProj, scan.pushedFilter());
        // outputOrder:新输出第 i 列 = 原输出索引(无投影:表列;有投影:existing 中的位置)
        List<Integer> outputOrder = new ArrayList<>(newProj.length);
        for (int col : newProj) {
            outputOrder.add(existing == null ? col : indexOf(existing, col));
        }
        return new Pruned(newScan, outputOrder);
    }

    private static List<Integer> toList(int[] arr) {
        List<Integer> list = new ArrayList<>(arr.length);
        for (int v : arr) {
            list.add(v);
        }
        return list;
    }

    private static int indexOf(int[] arr, int value) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == value) {
                return i;
            }
        }
        throw new IllegalStateException("列不在投影中: " + value);
    }

    private static Pruned pruneProject(MiniDbProject project, List<Integer> neededCols) {
        List<RexNode> exprs = project.getProjects();
        TreeSet<Integer> neededInput = new TreeSet<>();
        for (int k : neededCols) {
            collectRefs(exprs.get(k), neededInput);
        }
        List<Integer> neededInputList = new ArrayList<>(neededInput);
        RelNode newInput = pruneColumns(project.getInput(), neededInputList);
        if (neededInputList.size() == project.getInput().getRowType().getFieldCount()
                && newInput == project.getInput()) {
            return new Pruned(project, identityList(project.getRowType().getFieldCount()));
        }
        int[] inputMap = new int[project.getInput().getRowType().getFieldCount()];
        Arrays.fill(inputMap, -1);
        for (int k = 0; k < neededInputList.size(); k++) {
            inputMap[neededInputList.get(k)] = k;
        }
        List<RexNode> newExprs = new ArrayList<>(neededCols.size());
        for (int k : neededCols) {
            newExprs.add(remapRefs(exprs.get(k), inputMap));
        }
        RelDataTypeFactory tf = project.getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = tf.builder();
        for (int k : neededCols) {
            RelDataTypeField field = project.getRowType().getFieldList().get(k);
            builder.add(field.getName(), field.getType());
        }
        return new Pruned(new MiniDbProject(project.getCluster(), project.getTraitSet(),
                newInput, newExprs, builder.build()), neededCols);
    }

    private static Pruned pruneJoin(MiniDbJoin join, List<Integer> neededCols) {
        int leftCols = join.getLeft().getRowType().getFieldCount();
        int total = join.getRowType().getFieldCount();
        TreeSet<Integer> leftNeeded = new TreeSet<>();
        TreeSet<Integer> rightNeeded = new TreeSet<>();
        for (int k : neededCols) {
            if (k < leftCols) {
                leftNeeded.add(k);
            } else {
                rightNeeded.add(k - leftCols);
            }
        }
        // join 条件引用的列必须保留(join 内部匹配用)
        join.getCondition().accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef ref) {
                int i = ref.getIndex();
                if (i < leftCols) {
                    leftNeeded.add(i);
                } else {
                    rightNeeded.add(i - leftCols);
                }
                return null;
            }
        });
        // join 输入也裁剪(传需要的列):子节点输出列序变为 needed 序,join 重建后输出 =
        // 左右拼接(含条件列),再由输出投影归一化为 neededCols 序。copy 保留投影(见
        // MiniDbJoin.copyProjectionTo),重建/CSE 不会丢投影导致上层引用错位。
        List<Integer> leftNeededList = new ArrayList<>(leftNeeded);
        List<Integer> rightNeededList = new ArrayList<>(rightNeeded);
        RelNode newLeft = pruneColumns(join.getLeft(), leftNeededList);
        RelNode newRight = pruneColumns(join.getRight(), rightNeededList);
        if (isIdentity(neededCols, total) && newLeft == join.getLeft()
                && newRight == join.getRight()) {
            return new Pruned(join, identityList(total));
        }
        int[] condMap = new int[total];
        Arrays.fill(condMap, -1);
        for (int k = 0; k < leftNeededList.size(); k++) {
            condMap[leftNeededList.get(k)] = k;
        }
        for (int k = 0; k < rightNeededList.size(); k++) {
            condMap[leftCols + rightNeededList.get(k)] = leftNeededList.size() + k;
        }
        RexNode newCond = remapRefs(join.getCondition(), condMap);
        RelNode newJoin = join.copy(join.getTraitSet(), newCond, newLeft, newRight,
                join.getJoinType(), false);
        List<Integer> joinOrder = new ArrayList<>(leftNeededList.size() + rightNeededList.size());
        joinOrder.addAll(leftNeededList);
        for (int k : rightNeededList) {
            joinOrder.add(leftCols + k);
        }
        if (!isIdentity(neededCols, total) && newJoin instanceof MiniDbJoin j) {
            // join 输出投影到上层需要的列(条件列内部保留不输出):buildOutput 按投影列
            // 输出 + rowType 收窄——避免插额外 Project(插 Project 会让每层多一次全列
            // 拷贝,反而负优化)。投影数组是 newJoin 输出索引(左 needed + 右 needed 拼接序),
            // 即原输出列 neededCols[k] 在 joinOrder 中的位置。
            int[] proj = new int[neededCols.size()];
            for (int k = 0; k < neededCols.size(); k++) {
                proj[k] = joinOrder.indexOf(neededCols.get(k));
                if (proj[k] < 0) {
                    throw new IllegalStateException("join 输出列不在裁剪结果中: " + neededCols.get(k));
                }
            }
            j.setOutputProjection(proj, neededCols, join.getCluster().getTypeFactory(),
                    join.getRowType());
            // join 输出列序 = neededCols 序(投影序)
            return new Pruned(newJoin, neededCols);
        }
        return new Pruned(newJoin, joinOrder);
    }

    private static Pruned pruneSort(MiniDbSort sort, List<Integer> neededCols) {
        TreeSet<Integer> needed = new TreeSet<>(neededCols);
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            needed.add(fc.getFieldIndex());
        }
        List<Integer> neededList = new ArrayList<>(needed);
        RelNode newInput = pruneColumns(sort.getInput(), neededList);
        if (isIdentity(neededList, sort.getInput().getRowType().getFieldCount())
                && newInput == sort.getInput()) {
            return new Pruned(sort, identityList(sort.getRowType().getFieldCount()));
        }
        int[] map = new int[sort.getInput().getRowType().getFieldCount()];
        Arrays.fill(map, -1);
        for (int k = 0; k < neededList.size(); k++) {
            map[neededList.get(k)] = k;
        }
        List<RelFieldCollation> newFcs = new ArrayList<>();
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            newFcs.add(fc.copy(map[fc.getFieldIndex()]));
        }
        RelCollation newCollation = RelCollationImpl.of(newFcs);
        // traitSet 的 collation 分量必须与新 collation 一致(旧分量引用被裁剪的列,Sort 校验失败)
        RelTraitSet newTraits = sort.getTraitSet().replace(newCollation);
        RelNode newSort = sort.copy(newTraits, newInput, newCollation, sort.offset, sort.fetch);
        return new Pruned(newSort, neededList);
    }

    private static Pruned pruneFilter(MiniDbFilter filter, List<Integer> neededCols) {
        TreeSet<Integer> needed = new TreeSet<>(neededCols);
        collectRefs(filter.getCondition(), needed);
        List<Integer> neededList = new ArrayList<>(needed);
        RelNode newInput = pruneColumns(filter.getInput(), neededList);
        if (isIdentity(neededList, filter.getInput().getRowType().getFieldCount())
                && newInput == filter.getInput()) {
            return new Pruned(filter, identityList(filter.getRowType().getFieldCount()));
        }
        int[] map = new int[filter.getInput().getRowType().getFieldCount()];
        Arrays.fill(map, -1);
        for (int k = 0; k < neededList.size(); k++) {
            map[neededList.get(k)] = k;
        }
        RelNode newFilter = new MiniDbFilter(filter.getCluster(), filter.getTraitSet(), newInput,
                remapRefs(filter.getCondition(), map));
        return new Pruned(newFilter, neededList);
    }

    private static Pruned pruneAggregate(MiniDbAggregate aggregate) {
        // 聚合输出(group + aggCalls)不裁剪——输出行数少,裁剪收益小;
        // 但输入可裁剪:输入需求 = group 列 + 各 aggCall 参数引用的列
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.rexList != null && !call.rexList.isEmpty()) {
                // 表达式参数聚合(如 SUM(id*2))的 rexList 引用输入列,copy 不重映射
                // —— 这类聚合罕见,为它们放弃输入裁剪(保守)。
                return new Pruned(aggregate, identityList(aggregate.getRowType().getFieldCount()));
            }
        }
        TreeSet<Integer> neededInput = new TreeSet<>();
        for (int g : aggregate.getGroupSet()) {
            neededInput.add(g);
        }
        for (AggregateCall call : aggregate.getAggCallList()) {
            neededInput.addAll(call.getArgList());
            if (call.filterArg >= 0) {
                neededInput.add(call.filterArg);
            }
        }
        List<Integer> neededInputList = new ArrayList<>(neededInput);
        RelNode newInput = pruneColumns(aggregate.getInput(), neededInputList);
        if (newInput == aggregate.getInput()) {
            return new Pruned(aggregate, identityList(aggregate.getRowType().getFieldCount()));
        }
        int[] map = new int[aggregate.getInput().getRowType().getFieldCount()];
        Arrays.fill(map, -1);
        for (int k = 0; k < neededInputList.size(); k++) {
            map[neededInputList.get(k)] = k;
        }
        // groupSet/groupSets 也引用输入列,输入裁剪后必须重映射
        ImmutableBitSet newGroupSet = ImmutableBitSet.of();
        for (int g : aggregate.getGroupSet()) {
            newGroupSet = newGroupSet.set(map[g]);
        }
        List<ImmutableBitSet> newGroupSets = new ArrayList<>();
        for (ImmutableBitSet gs : aggregate.getGroupSets()) {
            ImmutableBitSet ng = ImmutableBitSet.of();
            for (int g : gs) {
                ng = ng.set(map[g]);
            }
            newGroupSets.add(ng);
        }
        List<AggregateCall> newCalls = new ArrayList<>(aggregate.getAggCallList().size());
        for (AggregateCall call : aggregate.getAggCallList()) {
            List<Integer> newArgs = new ArrayList<>(call.getArgList().size());
            for (int arg : call.getArgList()) {
                newArgs.add(map[arg]);
            }
            int newFilterArg = call.filterArg >= 0 ? map[call.filterArg] : call.filterArg;
            newCalls.add(call.copy(newArgs, newFilterArg, call.collation));
        }
        RelNode newAgg = aggregate.copy(aggregate.getTraitSet(), newInput, newGroupSet,
                newGroupSets, newCalls);
        // 聚合输出 = group + aggCalls(不裁剪),输出序 = 原序
        return new Pruned(newAgg, identityList(aggregate.getRowType().getFieldCount()));
    }

    /** 收集表达式引用的输入列索引。 */
    private static void collectRefs(RexNode expr, Set<Integer> out) {
        expr.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef ref) {
                out.add(ref.getIndex());
                return null;
            }
        });
    }

    /** 按映射重写表达式里的 RexInputRef 索引(map[old]=new,值为 -1 表示被裁剪但被引用)。 */
    private static RexNode remapRefs(RexNode expr, int[] map) {
        return expr.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int ni = map[ref.getIndex()];
                if (ni < 0) {
                    throw new IllegalStateException("列裁剪后仍被引用: " + ref.getIndex());
                }
                return new RexInputRef(ni, ref.getType());
            }
        });
    }

    private static List<Integer> identityList(int n) {
        List<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(i);
        }
        return list;
    }

    /** neededCols 是否恰好是全列恒等(无需裁剪)。 */
    private static boolean isIdentity(List<Integer> neededCols, int count) {
        if (neededCols.size() != count) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            if (neededCols.get(i) != i) {
                return false;
            }
        }
        return true;
    }


    // ---- 物化视图查询重写(Phase 1.5) ----

    /**
     * 物化视图查询重写:把「与某个 MV 定义结构完全一致」的查询子树替换为对该 MV 表的扫描。
     *
     * <p>重写只做保守的精确匹配(见 {@link #substituteMv}):用户查询子树与 MV 定义查询的
     * 规范化逻辑计划 digest 完全相等才替换。这是正确的关键——MV 只存了定义查询的结果,只有
     * 查询恰好等于(或在其上叠加投影/排序)MV 内容时,扫描 MV 才与原查询等价。聚合 MV 的
     * 重写同样只覆盖「查询聚合 == MV 聚合」的精确匹配;更灵活的补偿式重写(查询过滤更宽/
     * 分组更细等)留待后续。</p>
     */
    private RelNode rewriteWithMaterializedViews(RelNode logical, String currentSchema) {
        List<MVDefinition> mvs = new ArrayList<>();
        for (String schema : catalog.schemaNames()) {
            if (InformationSchemaCatalog.isSystemSchema(schema)) {
                continue;
            }
            mvs.addAll(catalog.getMaterializedViews(schema));
        }
        if (mvs.isEmpty()) {
            return logical;
        }
        // 预规划每个 MV 的定义 SQL:与用户查询走同一套流水线(root.project + LogicalOptimizer),
        // 保证两侧形状可比。失败(如基表已改导致解析错误)的 MV 直接跳过,不影响查询本身。
        Map<String, RelNode> mvPlans = new HashMap<>();
        for (MVDefinition mv : mvs) {
            try {
                RelNode mvPlan = planMvLogical(mv.querySql(), mv.schemaName());
                mvPlans.put(mvKey(mv), mvPlan);
            } catch (RuntimeException e) {
                // MV 定义 SQL 无法重新规划:跳过该 MV(不破坏用户查询)
            }
        }
        if (mvPlans.isEmpty()) {
            return logical;
        }
        return substituteMv(logical, mvs, mvPlans);
    }

    /** 用独立 HepPlanner cluster 规划 MV 定义 SQL(不走火山规划器,避免污染本次规划会话)。 */
    private RelNode planMvLogical(String sql, String schema) {
        HepPlanner mvPlanner = new HepPlanner(new HepProgramBuilder().build());
        SqlTypeFactoryImpl typeFactory = new Utf8SqlTypeFactory(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(mvPlanner, new RexBuilder(typeFactory));
        RelRoot root = calcite.planInCluster(sql, cluster, schema);
        return LogicalOptimizer.optimize(root.project());
    }

    private static String mvKey(MVDefinition mv) {
        return mv.schemaName() + "." + mv.name();
    }

    /**
     * 自底向上替换:先递归处理输入(深层子树优先),再尝试替换当前节点。
     * 匹配规则(保守):
     * <ol>
     *   <li>当前节点与某 MV 计划结构完全相等(见 {@link #structurallyEqual}) →
     *       整体替换为 MV 扫描;</li>
     *   <li>当前节点是 Project 且其输入与某 MV 计划结构相等 → 保留 Project、把输入
     *       替换为 MV 扫描(MV 输出列 == 被替换子树的输出列,Project 的列引用保持有效)。</li>
     * </ol>
     */
    private RelNode substituteMv(RelNode node, List<MVDefinition> mvs,
                                 Map<String, RelNode> mvPlans) {
        List<RelNode> inputs = node.getInputs();
        List<RelNode> newInputs = null;
        for (int i = 0; i < inputs.size(); i++) {
            RelNode newChild = substituteMv(inputs.get(i), mvs, mvPlans);
            if (newChild != inputs.get(i)) {
                if (newInputs == null) {
                    newInputs = new ArrayList<>(inputs);
                }
                newInputs.set(i, newChild);
            }
        }
        RelNode current = newInputs == null ? node : node.copy(node.getTraitSet(), newInputs);
        for (MVDefinition mv : mvs) {
            RelNode mvPlan = mvPlans.get(mvKey(mv));
            if (mvPlan != null && structurallyEqual(current, mvPlan)) {
                return scanMv(mv, current);
            }
        }
        // Project(匹配 MV 的输入):保留投影,替换输入为 MV 扫描。
        if (current instanceof LogicalProject proj) {
            for (MVDefinition mv : mvs) {
                RelNode mvPlan = mvPlans.get(mvKey(mv));
                if (mvPlan != null && structurallyEqual(proj.getInput(), mvPlan)) {
                    return LogicalProject.create(
                            scanMv(mv, proj.getInput()), List.of(),
                            proj.getProjects(), proj.getRowType());
                }
            }
        }
        return current;
    }

    /**
     * 结构相等:两棵逻辑计划树逐节点比较——类名、rowType 字段名、各自的 RexNode 表达式
     * (RexCall/RexInputRef/RexLiteral 的 equals 是结构化的)与 Scan 的 qualified name。
     * 不用 {@code getDigest()} 字符串比较:digest 会带上 hints/variablesSet 等与重写
     * 无关的表示差异(如 {@code LogicalProject.NONE.[]} vs {@code LogicalProject.}),
     * 导致相同的查询形状误判不等。
     */
    private static boolean structurallyEqual(RelNode a, RelNode b) {
        if (a.getClass() != b.getClass()) {
            return false;
        }
        if (!a.getRowType().getFieldNames().equals(b.getRowType().getFieldNames())) {
            return false;
        }
        if (a instanceof LogicalProject pa && b instanceof LogicalProject pb) {
            if (!pa.getProjects().equals(pb.getProjects())) {
                return false;
            }
        } else if (a instanceof LogicalFilter fa && b instanceof LogicalFilter fb) {
            if (!fa.getCondition().equals(fb.getCondition())) {
                return false;
            }
        } else if (a instanceof LogicalAggregate aa && b instanceof LogicalAggregate ab) {
            if (!aa.getGroupSet().equals(ab.getGroupSet())) {
                return false;
            }
            if (!aa.getAggCallList().equals(ab.getAggCallList())) {
                return false;
            }
        } else if (a instanceof LogicalJoin ja && b instanceof LogicalJoin jb) {
            if (ja.getJoinType() != jb.getJoinType()) {
                return false;
            }
            if (!ja.getCondition().equals(jb.getCondition())) {
                return false;
            }
        } else if (a instanceof LogicalTableScan) {
            List<String> qa = ((LogicalTableScan) a).getTable().getQualifiedName();
            List<String> qb = ((LogicalTableScan) b).getTable().getQualifiedName();
            if (!qa.equals(qb)) {
                return false;
            }
        }
        List<RelNode> ia = a.getInputs();
        List<RelNode> ib = b.getInputs();
        if (ia.size() != ib.size()) {
            return false;
        }
        for (int i = 0; i < ia.size(); i++) {
            if (!structurallyEqual(ia.get(i), ib.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** 构造对 MV 存储表的逻辑扫描(物理阶段由 MiniDbScanRule 转成 MiniDbScan)。 */
    private RelNode scanMv(MVDefinition mv, RelNode anchor) {
        SqlTypeFactoryImpl typeFactory =
                (SqlTypeFactoryImpl) anchor.getCluster().getTypeFactory();
        RelOptTable table = calcite.resolveTable(mv.schemaName(), mv.name(), typeFactory);
        if (table == null) {
            throw new IllegalStateException("MV table not resolvable: " + mvKey(mv));
        }
        return LogicalTableScan.create(anchor.getCluster(), table, List.of());
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
