package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Nested-loop join: for every (left, right) candidate pair it builds a one-row joined vector and
 * evaluates the full RexNode condition on it. Correctness over speed — works for any condition, not
 * just equi-joins.
 *
 * <p>Hash acceleration: when the join condition is {@code f(left_cols) = g(right_cols)} (a single
 * equality whose operands reference disjoint sides), we build a hash map from the smaller side and
 * probe from the larger side, reducing the nested loop from O(n×m) to O(n+m). This handles TPC-DS
 * query8's {@code substr(s_zip) = substr(ca_zip)} and similar expression-equalities that Calcite's
 * HashJoin/SortMergeJoin rules can't match (they require bare RexInputRef keys).
 */
public class MiniDbNestedLoopJoin extends MiniDbJoin {

    public MiniDbNestedLoopJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double leftRows = mq.getRowCount(getLeft());
        double rightRows = mq.getRowCount(getRight());
        // Calcite 1.42's VolcanoCost.isLt compares ONLY the rowCount component
        // (cpu/io are ignored — see VolcanoCost.isLt). Encode the estimated
        // work there so the planner can rank the three join strategies:
        // nested-loop evaluates every (left,right) pair. 乘 10 是因为逐对 RexInterpreter
        // 求值比 HashJoin 的哈希查找贵一个量级;不乘的话一侧只有 1 行时 left×right ≈ left+right
        // (如 query72 的 warehouse=1),NestedLoop 会因便宜 1 被误选。
        return planner.getCostFactory().makeCost(leftRows * rightRows * 10, 0, 0);
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        MiniDbNestedLoopJoin newJoin =
                new MiniDbNestedLoopJoin(
                        getCluster(), traitSet, left, right, conditionExpr, joinType);
        copyProjectionTo(newJoin);
        return newJoin;
    }

    @Override
    protected PairSource joinPairs(
            VectorSchemaRoot left,
            VectorSchemaRoot right,
            JoinInfo info,
            JoinRelType type,
            ExecContext ctx) {
        // 尝试 hash 加速:f(left_cols) = g(right_cols) 可用 hash map 代替逐对求值。
        PairSource accelerated = tryHashAccelerated(left, right, type, ctx);
        if (accelerated != null) {
            return accelerated;
        }
        return new NestedLoopPairSource(left, right, type, ctx);
    }

    /**
     * 若条件为 {@code f(left_cols) = g(right_cols)}(各操作数只引用单侧列),则对右侧建 hash map、左侧 probe,避免 O(n×m)
     * 逐对求值。无法加速时返回 null,调用方回退到 {@link NestedLoopPairSource}。
     */
    private PairSource tryHashAccelerated(
            VectorSchemaRoot left, VectorSchemaRoot right, JoinRelType type, ExecContext ctx) {
        RexNode condition = getCondition();
        if (!(condition instanceof RexCall call)
                || call.getKind() != SqlKind.EQUALS
                || call.getOperands().size() != 2) {
            return null;
        }
        RexNode op0 = call.getOperands().get(0);
        RexNode op1 = call.getOperands().get(1);
        int leftCols = leftColumnCount();
        int rightCols = rightColumnCount();
        boolean op0Left = onlyReferences(op0, 0, leftCols);
        boolean op0Right = onlyReferences(op0, leftCols, leftCols + rightCols);
        boolean op1Left = onlyReferences(op1, 0, leftCols);
        boolean op1Right = onlyReferences(op1, leftCols, leftCols + rightCols);
        if (!(op0Left && op1Right) && !(op0Right && op1Left)) {
            return null;
        }
        // 统一:leftExpr 只引用左侧列,rightExpr 只引用右侧列。
        RexNode leftExpr = op0Left ? op0 : op1;
        RexNode rightExpr = op0Left ? op1 : op0;
        return hashAcceleratedJoin(left, right, leftExpr, rightExpr, leftCols, type, ctx);
    }

    /** 检查 expr 里所有 RexInputRef 索引是否都在 [minIndex, maxIndex) 范围内。 */
    private static boolean onlyReferences(RexNode expr, int minIndex, int maxIndex) {
        boolean[] ok = {true};
        expr.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitInputRef(RexInputRef inputRef) {
                        if (inputRef.getIndex() < minIndex || inputRef.getIndex() >= maxIndex) {
                            ok[0] = false;
                        }
                        return null;
                    }
                });
        return ok[0];
    }

    /**
     * 对右侧表达式建 hash map,左侧逐行 probe(流式产出)。rightExpr 的 RexInputRef 索引需从 [leftCols..] 偏移到
     * [0..rightCols),由 {@link #shiftIndices} 完成。
     */
    private PairSource hashAcceleratedJoin(
            VectorSchemaRoot left,
            VectorSchemaRoot right,
            RexNode leftExpr,
            RexNode rightExpr,
            int leftCols,
            JoinRelType type,
            ExecContext ctx) {
        // 右侧建 hash map:shift 索引后一次求值全量右行,逐行读 key 入 hash。
        RexNode rightExprShifted = shiftIndices(rightExpr, -leftCols);
        Map<Object, List<Integer>> hashMap = new HashMap<>();
        ValueVector rightResults = ctx.interpreter().eval(rightExprShifted, right);
        try {
            for (int rightIdx = 0; rightIdx < right.getRowCount(); rightIdx++) {
                if (!rightResults.isNull(rightIdx)) {
                    Object key = RowVectors.readObject(rightResults, rightIdx);
                    hashMap.computeIfAbsent(key, k -> new ArrayList<>()).add(rightIdx);
                }
            }
        } finally {
            rightResults.close();
        }
        // 左侧 probe 结果一次求值全量(表达式求值无状态,行对产出流式)。
        ValueVector leftResults = ctx.interpreter().eval(leftExpr, left);
        return new HashAccelPairSource(left, right, type, hashMap, leftResults);
    }

    /** 流式 probe 左侧匹配行;阶段 2/3 产出未匹配左行与未匹配右行(outer join)。 */
    private static final class HashAccelPairSource implements PairSource {
        private final VectorSchemaRoot left;
        private final VectorSchemaRoot right;
        private final Map<Object, List<Integer>> hashMap;
        private final ValueVector leftResults;
        private final boolean keepUnmatchedLeft;
        private final boolean keepUnmatchedRight;
        private final boolean[] matchedLeft;
        private final boolean[] matchedRight;

        private int leftIdx = 0;
        private List<Integer> matchList;
        private int matchPos = 0;
        private int phase = 0; // 0=probe, 1=unmatched left, 2=unmatched right
        private int scanIdx = 0;

        HashAccelPairSource(
                VectorSchemaRoot left,
                VectorSchemaRoot right,
                JoinRelType type,
                Map<Object, List<Integer>> hashMap,
                ValueVector leftResults) {
            this.left = left;
            this.right = right;
            this.hashMap = hashMap;
            this.leftResults = leftResults;
            this.keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
            this.keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
            this.matchedLeft = new boolean[left.getRowCount()];
            this.matchedRight = new boolean[right.getRowCount()];
            // 预加载第 0 行的匹配列表(fill 循环推进前先消费当前行)
            if (left.getRowCount() > 0 && !leftResults.isNull(0)) {
                this.matchList = hashMap.get(RowVectors.readObject(leftResults, 0));
            }
        }

        @Override
        public int fill(int[] leftRows, int[] rightRows, int outPos, int len) {
            int out = outPos;
            int end = outPos + len;
            while (out < end) {
                if (phase == 0) {
                    if (leftIdx >= left.getRowCount()) {
                        phase = 1;
                        scanIdx = 0;
                        continue;
                    }
                    if (matchList != null && matchPos < matchList.size()) {
                        int rightIdx = matchList.get(matchPos++);
                        leftRows[out] = leftIdx;
                        rightRows[out] = rightIdx;
                        matchedLeft[leftIdx] = true;
                        matchedRight[rightIdx] = true;
                        out++;
                        continue;
                    }
                    leftIdx++;
                    if (leftIdx % 1000 == 0) {
                        ExecContext.checkInterrupted();
                    }
                    matchList = null;
                    matchPos = 0;
                    if (leftIdx < left.getRowCount() && !leftResults.isNull(leftIdx)) {
                        matchList = hashMap.get(RowVectors.readObject(leftResults, leftIdx));
                    }
                    continue;
                }
                if (phase == 1 && keepUnmatchedLeft) {
                    while (scanIdx < left.getRowCount() && matchedLeft[scanIdx]) {
                        scanIdx++;
                    }
                    if (scanIdx < left.getRowCount()) {
                        leftRows[out] = scanIdx;
                        rightRows[out] = -1;
                        out++;
                        scanIdx++;
                        continue;
                    }
                    // 左未匹配耗尽 → 阶段 3
                    if (keepUnmatchedRight) {
                        phase = 2;
                        scanIdx = 0;
                    } else {
                        break;
                    }
                }
                if (phase == 2 && keepUnmatchedRight) {
                    while (scanIdx < right.getRowCount() && matchedRight[scanIdx]) {
                        scanIdx++;
                    }
                    if (scanIdx < right.getRowCount()) {
                        leftRows[out] = -1;
                        rightRows[out] = scanIdx;
                        out++;
                        scanIdx++;
                        continue;
                    }
                    break;
                }
                break;
            }
            return out;
        }

        @Override
        public void close() {
            leftResults.close();
        }
    }

    /**
     * 纯嵌套循环:O(n×m) 逐对求值,当 hash 加速不可用时回退到此处。 双游标推进 + probeRoot 复用,天然流式。非静态内部类以访问
     * buildProbeRoot/getCondition。
     */
    private final class NestedLoopPairSource implements PairSource {
        private final VectorSchemaRoot left;
        private final VectorSchemaRoot right;
        private final ExecContext ctx;
        private final boolean keepUnmatchedLeft;
        private final boolean keepUnmatchedRight;
        private final boolean[] matchedLeft;
        private final boolean[] matchedRight;
        private final VectorSchemaRoot probeRoot;

        private int leftIdx = 0;
        private int rightIdx = 0;
        private int phase = 0; // 0=loop, 1=unmatched left, 2=unmatched right
        private int scanIdx = 0;

        NestedLoopPairSource(
                VectorSchemaRoot left, VectorSchemaRoot right, JoinRelType type, ExecContext ctx) {
            this.left = left;
            this.right = right;
            this.ctx = ctx;
            this.keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
            this.keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
            this.matchedLeft = new boolean[left.getRowCount()];
            this.matchedRight = new boolean[right.getRowCount()];
            this.probeRoot = buildProbeRoot(ctx, left, right);
        }

        @Override
        public int fill(int[] leftRows, int[] rightRows, int outPos, int len) {
            int out = outPos;
            int end = outPos + len;
            while (out < end) {
                if (phase == 0) {
                    if (leftIdx >= left.getRowCount()) {
                        phase = 1;
                        scanIdx = 0;
                        continue;
                    }
                    if (rightIdx >= right.getRowCount()) {
                        leftIdx++;
                        rightIdx = 0;
                        if (leftIdx % 1000 == 0) {
                            ExecContext.checkInterrupted();
                        }
                        continue;
                    }
                    writeProbeRow(probeRoot, left, leftIdx, right, rightIdx);
                    try (ValueVector conditionResult =
                            ctx.interpreter().eval(getCondition(), probeRoot)) {
                        boolean matches =
                                !conditionResult.isNull(0)
                                        && ((BitVector) conditionResult).get(0) == 1;
                        if (matches) {
                            leftRows[out] = leftIdx;
                            rightRows[out] = rightIdx;
                            matchedLeft[leftIdx] = true;
                            matchedRight[rightIdx] = true;
                            out++;
                        }
                    }
                    rightIdx++;
                    continue;
                }
                if (phase == 1 && keepUnmatchedLeft) {
                    while (scanIdx < left.getRowCount() && matchedLeft[scanIdx]) {
                        scanIdx++;
                    }
                    if (scanIdx < left.getRowCount()) {
                        leftRows[out] = scanIdx;
                        rightRows[out] = -1;
                        out++;
                        scanIdx++;
                        continue;
                    }
                    // 左未匹配耗尽 → 阶段 3
                    if (keepUnmatchedRight) {
                        phase = 2;
                        scanIdx = 0;
                    } else {
                        break;
                    }
                }
                if (phase == 2 && keepUnmatchedRight) {
                    while (scanIdx < right.getRowCount() && matchedRight[scanIdx]) {
                        scanIdx++;
                    }
                    if (scanIdx < right.getRowCount()) {
                        leftRows[out] = -1;
                        rightRows[out] = scanIdx;
                        out++;
                        scanIdx++;
                        continue;
                    }
                    break;
                }
                break;
            }
            return out;
        }

        @Override
        public void close() {
            probeRoot.close();
        }
    }

    /** 把 RexNode 里所有 RexInputRef 索引偏移 {@code delta}(可为负)。 */
    private static RexNode shiftIndices(RexNode expr, int delta) {
        return expr.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        return new RexInputRef(inputRef.getIndex() + delta, inputRef.getType());
                    }
                });
    }
}
