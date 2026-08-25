package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.calcite.rex.RexNode;

/**
 * Hash join: builds a hash table on the left input keyed by the equi columns,
 * then probes it with the right input. Equi-join only. Null keys never match
 * in an equi-join, so null-keyed rows are excluded from the table and, for
 * outer joins, emitted as preserved rows instead.
 */
public class MiniDbHashJoin extends MiniDbJoin {

    public MiniDbHashJoin(RelOptCluster cluster, RelTraitSet traitSet,
                          RelNode left, RelNode right, RexNode condition,
                          JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double leftRows = mq.getRowCount(getLeft());
        double rightRows = mq.getRowCount(getRight());
        // Calcite 1.42's VolcanoCost.isLt compares ONLY the rowCount component
        // (cpu/io are ignored — see VolcanoCost.isLt). Encode the estimated
        // work there so the planner can rank the three join strategies: hash
        // join builds a table on the left and probes with the right, linear in
        // both inputs.
        return planner.getCostFactory().makeCost(leftRows + rightRows, 0, 0);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbHashJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected PairSource joinPairs(VectorSchemaRoot left, VectorSchemaRoot right,
                                   JoinInfo info, JoinRelType type, ExecContext ctx) {
        List<Integer> leftKeyCols = info.leftKeys;
        List<Integer> rightKeyCols = info.rightKeys;
        int[] leftKeyArr = toIntArray(leftKeyCols);
        int[] rightKeyArr = toIntArray(rightKeyCols);
        // 残留(非等值)条件:等值键之外的合取项(query13/15 的 OR 残留在等值键匹配后仍需过滤)。
        RexNode residual = info.getRemaining(getCluster().getRexBuilder());
        boolean hasResidual = !residual.isAlwaysTrue();
        VectorSchemaRoot probeRoot = hasResidual ? buildProbeRoot(ctx, left, right) : null;
        // Build: key -> row indices of the left input sharing that key.
        // Null-keyed rows are skipped (they can never match).
        Map<ColumnKey, List<Integer>> buildTable = new HashMap<>();
        for (int leftIdx = 0; leftIdx < left.getRowCount(); leftIdx++) {
            if (hasNullKey(left, leftIdx, leftKeyCols)) {
                continue;
            }
            buildTable.computeIfAbsent(new ColumnKey(left, leftIdx, leftKeyArr),
                    k -> new ArrayList<>()).add(leftIdx);
        }
        return new HashPairSource(left, right, type, ctx, buildTable, leftKeyCols, rightKeyCols,
                leftKeyArr, rightKeyArr, residual, hasResidual, probeRoot);
    }

    /**
     * 流式 probe:右侧逐行查 build 表产出匹配对;probe 耗尽后阶段 2 产出
     * 未匹配左行(LEFT/FULL 的 null-pad——行是否匹配要等整个 probe 结束才能定)。
     * 不物化输出行对,内存 O(批大小)。
     */
    private static final class HashPairSource implements PairSource {
        private final VectorSchemaRoot left;
        private final VectorSchemaRoot right;
        private final JoinRelType type;
        private final ExecContext ctx;
        private final Map<ColumnKey, List<Integer>> buildTable;
        private final List<Integer> leftKeyCols;
        private final List<Integer> rightKeyCols;
        private final int[] leftKeyArr;
        private final int[] rightKeyArr;
        private final RexNode residual;
        private final boolean hasResidual;
        private final VectorSchemaRoot probeRoot;
        private final boolean keepUnmatchedLeft;
        private final boolean keepUnmatchedRight;
        private final boolean[] matchedLeft;

        // probe 游标
        private int probeIdx = 0;
        private List<Integer> matchList;
        private int matchPos = 0;
        // 阶段 2(未匹配左行)游标
        private boolean phaseTwo;
        private int unmatchedLeftIdx = 0;

        HashPairSource(VectorSchemaRoot left, VectorSchemaRoot right, JoinRelType type,
                       ExecContext ctx, Map<ColumnKey, List<Integer>> buildTable,
                       List<Integer> leftKeyCols, List<Integer> rightKeyCols,
                       int[] leftKeyArr, int[] rightKeyArr, RexNode residual,
                       boolean hasResidual, VectorSchemaRoot probeRoot) {
            this.left = left;
            this.right = right;
            this.type = type;
            this.ctx = ctx;
            this.buildTable = buildTable;
            this.leftKeyCols = leftKeyCols;
            this.rightKeyCols = rightKeyCols;
            this.leftKeyArr = leftKeyArr;
            this.rightKeyArr = rightKeyArr;
            this.residual = residual;
            this.hasResidual = hasResidual;
            this.probeRoot = probeRoot;
            this.keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
            this.keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
            this.matchedLeft = new boolean[left.getRowCount()];
            loadMatchList();
        }

        private void loadMatchList() {
            if (probeIdx >= right.getRowCount()) {
                matchList = null;
                return;
            }
            if (hasNullKey(right, probeIdx, rightKeyCols)) {
                matchList = null; // null 键等值永不匹配
            } else {
                matchList = buildTable.get(new ColumnKey(right, probeIdx, rightKeyArr));
            }
            matchPos = 0;
        }

        @Override
        public int fill(int[] leftRows, int[] rightRows, int outPos, int len) {
            int out = outPos;
            int end = outPos + len;
            while (out < end) {
                if (phaseTwo) {
                    // 阶段 2:LEFT/FULL 的未匹配左行(null-pad 右侧)
                    while (unmatchedLeftIdx < left.getRowCount()
                            && matchedLeft[unmatchedLeftIdx]) {
                        unmatchedLeftIdx++;
                    }
                    if (unmatchedLeftIdx >= left.getRowCount()) {
                        break;
                    }
                    leftRows[out] = unmatchedLeftIdx;
                    rightRows[out] = -1;
                    out++;
                    unmatchedLeftIdx++;
                    continue;
                }
                if (probeIdx >= right.getRowCount()) {
                    if (keepUnmatchedLeft) {
                        phaseTwo = true;
                        unmatchedLeftIdx = 0;
                        continue;
                    }
                    break;
                }
                if (matchList != null && matchPos < matchList.size()) {
                    int leftIdx = matchList.get(matchPos++);
                    if (!hasResidual || residualMatches(probeRoot, left, leftIdx, right,
                            probeIdx, residual, ctx)) {
                        leftRows[out] = leftIdx;
                        rightRows[out] = probeIdx;
                        matchedLeft[leftIdx] = true;
                        out++;
                    }
                    continue;
                }
                // 当前 probe 行匹配耗尽:无匹配(或 null 键)且需保留 → null-pad 右行
                if (matchList == null && keepUnmatchedRight) {
                    leftRows[out] = -1;
                    rightRows[out] = probeIdx;
                    out++;
                }
                probeIdx++;
                if (probeIdx % 1000 == 0) {
                    ExecContext.checkInterrupted();
                }
                loadMatchList();
            }
            return out;
        }

        @Override
        public void close() {
            if (probeRoot != null) {
                probeRoot.close();
            }
        }
    }

    private static boolean residualMatches(VectorSchemaRoot probeRoot, VectorSchemaRoot left,
                                           int leftIdx, VectorSchemaRoot right, int rightIdx,
                                           RexNode residual, ExecContext ctx) {
        writeProbeRow(probeRoot, left, leftIdx, right, rightIdx);
        ValueVector result = ctx.interpreter().eval(residual, probeRoot);
        try {
            return !result.isNull(0) && ((BitVector) result).get(0) == 1;
        } finally {
            result.close();
        }
    }
}
