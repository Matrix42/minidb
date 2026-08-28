package com.minidb.server.plan.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

/**
 * 贪心重排 INNER join 链,消除「FROM 顺序导致的交叉连接」。
 *
 * <p>SqlToRelConverter 按 FROM 顺序生成左深 join 树;若某表(如 query18 的 cd2)排在它的等值
 * 连接伙伴(customer)之前,该表先被交叉连接(cond=true),再把等值条件推迟到后续 join。这会让
 * NestedLoopJoin 物化笛卡尔积(344 亿对)而 OOM。Calcite 的 JoinAssociateRule/JoinCommuteRule
 * 在去相关树上会丢条件、也变交叉连接(故已禁用),所以这里用「等值连接图贪心」:先选连接度最高的
 * 表做种子,每步加入「与当前集合有等值连接」的表,保证不产生本可避免的交叉连接。</p>
 */
public final class JoinReorderer {

    private JoinReorderer() {
    }

    public static RelNode reorder(RelNode node) {
        if (node instanceof LogicalJoin join && join.getJoinType() == JoinRelType.INNER) {
            return reorderInnerChain(join);
        }
        List<RelNode> inputs = node.getInputs();
        List<RelNode> newInputs = null;
        for (int i = 0; i < inputs.size(); i++) {
            RelNode newInput = reorder(inputs.get(i));
            if (newInput != inputs.get(i)) {
                if (newInputs == null) {
                    newInputs = new ArrayList<>(inputs);
                }
                newInputs.set(i, newInput);
            }
        }
        if (newInputs == null) {
            return node;
        }
        return node.copy(node.getTraitSet(), newInputs);
    }

    private static RelNode reorderInnerChain(LogicalJoin root) {
        List<RelNode> leaves = new ArrayList<>();
        List<RexNode> conditions = new ArrayList<>();
        collectInnerChain(root, leaves, conditions);

        // 先递归重排各叶子(叶子里可能还嵌着非 INNER 的 join)。
        List<RelNode> reorderedLeaves = new ArrayList<>(leaves.size());
        for (RelNode leaf : leaves) {
            reorderedLeaves.add(reorder(leaf));
        }

        if (reorderedLeaves.size() <= 2) {
            // 单/双叶子无需重排,只需把递归重排过的叶子接回原条件。
            return rebuildTwoOrOne(root, reorderedLeaves, conditions);
        }
        return greedyRebuild(root, reorderedLeaves, conditions);
    }

    /** 收集 INNER join 链:所有非 INNER-join 的输入是叶子,INNER join 的条件进 conditions。 */
    private static void collectInnerChain(RelNode node, List<RelNode> leaves, List<RexNode> conditions) {
        if (node instanceof LogicalJoin join && join.getJoinType() == JoinRelType.INNER) {
            collectInnerChain(join.getLeft(), leaves, conditions);
            collectInnerChain(join.getRight(), leaves, conditions);
            if (!join.getCondition().isAlwaysTrue()) {
                conditions.add(join.getCondition());
            }
        } else {
            leaves.add(node);
        }
    }

    private static RelNode rebuildTwoOrOne(LogicalJoin root, List<RelNode> leaves, List<RexNode> conditions) {
        if (leaves.size() == 1) {
            return leaves.get(0);
        }
        RexNode cond = RexUtil.composeConjunction(root.getCluster().getRexBuilder(), conditions);
        return root.copy(root.getTraitSet(), cond, leaves.get(0), leaves.get(1),
                root.getJoinType(), root.isSemiJoinDone());
    }

    private static RelNode greedyRebuild(LogicalJoin root, List<RelNode> leaves, List<RexNode> conditions) {
        int n = leaves.size();
        int[] fieldStart = new int[n];
        int[] fieldCount = new int[n];
        int totalFields = 0;
        for (int i = 0; i < n; i++) {
            fieldStart[i] = totalFields;
            fieldCount[i] = leaves.get(i).getRowType().getFieldCount();
            totalFields += fieldCount[i];
        }

        // 1. 把每个条件拆成合取项,记录每项引用的叶子集合。
        List<RexNode> conjuncts = new ArrayList<>();
        for (RexNode cond : conditions) {
            conjuncts.addAll(RexUtil.flattenAnd(List.of(cond)));
        }
        List<Set<Integer>> conjunctLeaves = new ArrayList<>();
        for (RexNode conjunct : conjuncts) {
            conjunctLeaves.add(referencedLeaves(conjunct, fieldStart, fieldCount));
        }

        // 2. 连接图:叶子 i 与 j 之间只要有合取项同时引用两者,就有一条边(等值或残差)。
        List<Set<Integer>> adjacency = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            adjacency.add(new java.util.LinkedHashSet<>());
        }
        for (int c = 0; c < conjuncts.size(); c++) {
            Set<Integer> refs = conjunctLeaves.get(c);
            if (refs.size() == 2) {
                Integer[] pair = refs.toArray(new Integer[0]);
                adjacency.get(pair[0]).add(pair[1]);
                adjacency.get(pair[1]).add(pair[0]);
            }
        }

        // 3. 贪心排序:种子 = 连接度最高;每步加入「与已选集合连接数最多」的叶子。
        // 连接度平手时按行数破平手(小表优先):否则大表(如 TPC-DS query8 的 store_sales,
        // 行数最多、下标靠前)会因平手取首见而当选种子,把非等值条件推到大表 join 结果之上,
        // NestedLoop 物化 left×right 对(2.3 亿次求值)。行数取 RelMetadataQuery.getRowCount,
        // 无统计时默认 1e8(Calcite 对无统计表的估算默认值),保证不被误当小表种子。
        RelMetadataQuery mq = root.getCluster().getMetadataQuery();
        int[] order = new int[n];
        boolean[] used = new boolean[n];
        int seed = 0;
        int seedDegree = -1;
        double seedRows = Double.POSITIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            int degree = adjacency.get(i).size();
            if (degree > seedDegree
                    || (degree == seedDegree && rowCount(leaves.get(i), mq) < seedRows)) {
                seedDegree = degree;
                seedRows = rowCount(leaves.get(i), mq);
                seed = i;
            }
        }
        order[0] = seed;
        used[seed] = true;
        for (int k = 1; k < n; k++) {
            int best = -1;
            int bestScore = -1;
            double bestRows = Double.POSITIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                if (used[i]) {
                    continue;
                }
                int score = 0;
                for (int neighbor : adjacency.get(i)) {
                    if (used[neighbor]) {
                        score++;
                    }
                }
                double rows = rowCount(leaves.get(i), mq);
                if (score > bestScore
                        || (score == bestScore && rows < bestRows)) {
                    bestScore = score;
                    bestRows = rows;
                    best = i;
                }
            }
            order[k] = best;
            used[best] = true;
        }

        // 4. 重建左深树,把每个合取项赋给「后加入的那个叶子」所在的 join。
        // 引用恰好 2 个叶子的合取项作为 join 条件;引用 >2 个叶子的合取项(如 query4 的
        // CASE ratio 比较引用 4 个 year_total 列)无法放进单个 join,收集起来在重建后
        // 作为顶层 Filter 保留(否则会被丢弃 → 查询结果错误)。
        RelOptCluster cluster = root.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<Integer> currentOrder = new ArrayList<>();
        RelNode current = leaves.get(order[0]);
        currentOrder.add(order[0]);
        List<RexNode> unassignedConjuncts = new ArrayList<>();
        for (int k = 1; k < n; k++) {
            int leafId = order[k];
            List<RexNode> joinConjuncts = new ArrayList<>();
            for (int c = 0; c < conjuncts.size(); c++) {
                Set<Integer> refs = conjunctLeaves.get(c);
                if (!refs.contains(leafId)) {
                    continue;
                }
                if (refs.size() != 2) {
                    // 跨 >2 叶子的合取项:不能放进单个 join,留待顶层 Filter。
                    continue;
                }
                boolean otherInCurrent = false;
                for (int r : refs) {
                    if (r != leafId && currentOrder.contains(r)) {
                        otherInCurrent = true;
                        break;
                    }
                }
                if (otherInCurrent) {
                    joinConjuncts.add(remap(conjuncts.get(c), currentOrder, leafId,
                            fieldStart, fieldCount));
                }
            }
            RexNode joinCond = RexUtil.composeConjunction(rexBuilder, joinConjuncts);
            current = LogicalJoin.create(current, leaves.get(leafId), List.of(),
                    joinCond, root.getVariablesSet(), JoinRelType.INNER);
            currentOrder.add(leafId);
        }
        // 收集所有引用 >2 个叶子的合取项(原扁平偏移,稍后经 Project 还原字段顺序后引用)。
        for (int c = 0; c < conjuncts.size(); c++) {
            if (conjunctLeaves.get(c).size() > 2) {
                unassignedConjuncts.add(conjuncts.get(c));
            }
        }

        // 重排改变了 join 输出字段顺序,上层节点(Project/Aggregate)仍按原顺序引用列,
        // 必须补一个 Project 把字段顺序还原成原扁平顺序(order 为恒等置换时跳过)。
        boolean identity = true;
        for (int i = 0; i < n; i++) {
            if (order[i] != i) {
                identity = false;
                break;
            }
        }
        // 跨 >2 叶子的合取项(原扁平偏移)在 Project 还原字段顺序后引用,故对 identity
        // 和 non-identity 两种情况都适用:identity 时直接对 join 输出过滤;non-identity
        // 时对 Project 输出过滤(Project 已把字段还原成原扁平顺序)。
        RelNode result;
        if (identity) {
            result = current;
        } else {
            int[] reorderedOffset = new int[n];
            int off = 0;
            for (int p = 0; p < n; p++) {
                reorderedOffset[order[p]] = off;
                off += fieldCount[order[p]];
            }
            List<RexNode> projects = new ArrayList<>(totalFields(fieldCount));
            for (int l = 0; l < n; l++) {
                for (int f = 0; f < fieldCount[l]; f++) {
                    projects.add(rexBuilder.makeInputRef(current, reorderedOffset[l] + f));
                }
            }
            result = LogicalProject.create(current, List.of(), projects,
                    root.getRowType().getFieldNames());
        }
        if (!unassignedConjuncts.isEmpty()) {
            RexNode filterCond = RexUtil.composeConjunction(rexBuilder, unassignedConjuncts);
            result = LogicalFilter.create(result, filterCond);
        }
        return result;
    }

    /** 合取项引用的叶子下标集合(按字段偏移范围归到叶子)。 */
    private static Set<Integer> referencedLeaves(RexNode node, int[] fieldStart, int[] fieldCount) {
        Set<Integer> refs = new java.util.LinkedHashSet<>();
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                int index = inputRef.getIndex();
                for (int i = 0; i < fieldStart.length; i++) {
                    if (index >= fieldStart[i] && index < fieldStart[i] + fieldCount[i]) {
                        refs.add(i);
                        break;
                    }
                }
                return inputRef;
            }
        };
        shuttle.apply(node);
        return refs;
    }

    /** 把合取项从「原扁平字段偏移」重映射到「(currentOrder 叶子 + leafId)」的左右输入空间。 */
    private static RexNode remap(RexNode node, List<Integer> currentOrder, int leafId,
                                 int[] fieldStart, int[] fieldCount) {
        int leftFieldCount = 0;
        for (int id : currentOrder) {
            leftFieldCount += fieldCount[id];
        }
        int[] mapping = new int[totalFields(fieldCount)];
        java.util.Arrays.fill(mapping, -1);
        int newOffset = 0;
        for (int id : currentOrder) {
            for (int f = fieldStart[id]; f < fieldStart[id] + fieldCount[id]; f++) {
                mapping[f] = newOffset++;
            }
        }
        int rightOffset = leftFieldCount;
        for (int f = fieldStart[leafId]; f < fieldStart[leafId] + fieldCount[leafId]; f++) {
            mapping[f] = rightOffset++;
        }
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                int mapped = mapping[inputRef.getIndex()];
                if (mapped < 0) {
                    throw new IllegalStateException(
                            "unmapped field " + inputRef.getIndex() + " in remap");
                }
                return new RexInputRef(mapped, inputRef.getType());
            }
        };
        return shuttle.apply(node);
    }

    /**
     * 叶子行数估算,用于连接度平手时按「小表优先」破平手。
     *
     * <p>无统计时(Calcite 对无 ANALYZE 的表返回 null rowCount,且 1.42 的
     * {@code RelMdUtil.estimateFilteredRows} 对 null selectivity 直接 unboxing 抛 NPE)
     * 回退 1e8:所有叶子取相同值,行数破平手退化为不生效,贪心回到纯连接度决策——
     * 与修复前行为一致,保证无统计场景不回退已有交叉连接消除(query10/18)。</p>
     */
    private static double rowCount(RelNode leaf, RelMetadataQuery mq) {
        try {
            Double r = mq.getRowCount(leaf);
            return r == null ? 1e8 : r;
        } catch (RuntimeException e) {
            // Calcite 1.42 无统计 Filter 的 estimateFilteredRows NPE;回退默认值。
            return 1e8;
        }
    }

    private static int totalFields(int[] fieldCount) {
        int total = 0;
        for (int c : fieldCount) {
            total += c;
        }
        return total;
    }
}
