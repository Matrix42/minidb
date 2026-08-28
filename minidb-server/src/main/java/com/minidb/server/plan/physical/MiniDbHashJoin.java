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
        MiniDbHashJoin newJoin = new MiniDbHashJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
        copyProjectionTo(newJoin);
        return newJoin;
    }

    @Override
    protected PairSource joinPairs(VectorSchemaRoot left, VectorSchemaRoot right,
                                   JoinInfo info, JoinRelType type, ExecContext ctx) {
        List<Integer> leftKeyCols = info.leftKeys;
        List<Integer> rightKeyCols = info.rightKeys;
        // Build 侧选行数少的一侧:哈希表构建成本 O(build 行数),probe 大侧流式查表 O(1)/行。
        // 左深 join 链每层 left=累积大结果,固定 build=left 会每层为百万行建哈希;
        // 交换后 build 小维表、probe 大结果流式,建表成本大降(query64 等 17 表链)。
        boolean buildLeft = left.getRowCount() <= right.getRowCount();
        VectorSchemaRoot build = buildLeft ? left : right;
        VectorSchemaRoot probe = buildLeft ? right : left;
        List<Integer> buildKeyCols = buildLeft ? leftKeyCols : rightKeyCols;
        List<Integer> probeKeyCols = buildLeft ? rightKeyCols : leftKeyCols;
        int[] buildKeyArr = toIntArray(buildKeyCols);
        int[] probeKeyArr = toIntArray(probeKeyCols);
        // 残留(非等值)条件:等值键之外的合取项(query13/15 的 OR 残留在等值键匹配后仍需过滤)。
        RexNode residual = info.getRemaining(getCluster().getRexBuilder());
        boolean hasResidual = !residual.isAlwaysTrue();
        VectorSchemaRoot probeRoot = hasResidual ? buildProbeRoot(ctx, left, right) : null;
        // Build: key -> row indices of the build input sharing that key.
        // Null-keyed rows are skipped (they can never match).
        Map<ColumnKey, List<Integer>> buildTable = new HashMap<>();
        for (int buildIdx = 0; buildIdx < build.getRowCount(); buildIdx++) {
            if (hasNullKey(build, buildIdx, buildKeyCols)) {
                continue;
            }
            buildTable.computeIfAbsent(new ColumnKey(build, buildIdx, buildKeyArr),
                    k -> new ArrayList<>()).add(buildIdx);
        }
        // null-pad 语义按 build 侧翻转:
        //  LEFT=保留 left 未匹配 / RIGHT=保留 right 未匹配 / FULL=都保留
        boolean keepUnmatchedBuild = (type == JoinRelType.LEFT && buildLeft)
                || (type == JoinRelType.RIGHT && !buildLeft)
                || type == JoinRelType.FULL;
        boolean keepUnmatchedProbe = (type == JoinRelType.RIGHT && buildLeft)
                || (type == JoinRelType.LEFT && !buildLeft)
                || type == JoinRelType.FULL;
        return new HashPairSource(left, right, buildLeft, build, probe, type, ctx, buildTable,
                buildKeyCols, probeKeyCols, buildKeyArr, probeKeyArr,
                residual, hasResidual, probeRoot, keepUnmatchedBuild, keepUnmatchedProbe);
    }

    /**
     * 流式 probe:右侧逐行查 build 表产出匹配对;probe 耗尽后阶段 2 产出
     * 未匹配的保留行(outer join 的 null-pad——行是否匹配要等整个 probe 结束才能定)。
     * build 侧恒为小侧(建表成本 O(build 行数)),probe 大侧流式查表。
     * 不物化输出行对,内存 O(批大小)。
     */
    private static final class HashPairSource implements PairSource {
        private final VectorSchemaRoot left;
        private final VectorSchemaRoot right;
        private final boolean buildLeft;
        private final VectorSchemaRoot build;
        private final VectorSchemaRoot probe;
        private final ExecContext ctx;
        private final Map<ColumnKey, List<Integer>> buildTable;
        private final List<Integer> probeKeyCols;
        private final int[] probeKeyArr;
        private final RexNode residual;
        private final boolean hasResidual;
        private final VectorSchemaRoot probeRoot;
        private final boolean keepUnmatchedBuild;
        private final boolean keepUnmatchedProbe;
        private final boolean[] matchedBuild;

        // probe 游标
        private int probeIdx = 0;
        private List<Integer> matchList;
        private int matchPos = 0;
        // 阶段 2(未匹配 build 行)游标
        private boolean phaseTwo;
        private int unmatchedBuildIdx = 0;

        HashPairSource(VectorSchemaRoot left, VectorSchemaRoot right, boolean buildLeft,
                       VectorSchemaRoot build, VectorSchemaRoot probe, JoinRelType type,
                       ExecContext ctx, Map<ColumnKey, List<Integer>> buildTable,
                       List<Integer> buildKeyCols, List<Integer> probeKeyCols,
                       int[] buildKeyArr, int[] probeKeyArr, RexNode residual,
                       boolean hasResidual, VectorSchemaRoot probeRoot,
                       boolean keepUnmatchedBuild, boolean keepUnmatchedProbe) {
            this.left = left;
            this.right = right;
            this.buildLeft = buildLeft;
            this.build = build;
            this.probe = probe;
            this.ctx = ctx;
            this.buildTable = buildTable;
            this.probeKeyCols = probeKeyCols;
            this.probeKeyArr = probeKeyArr;
            this.residual = residual;
            this.hasResidual = hasResidual;
            this.probeRoot = probeRoot;
            this.keepUnmatchedBuild = keepUnmatchedBuild;
            this.keepUnmatchedProbe = keepUnmatchedProbe;
            this.matchedBuild = new boolean[build.getRowCount()];
            loadMatchList();
        }

        private void loadMatchList() {
            if (probeIdx >= probe.getRowCount()) {
                matchList = null;
                return;
            }
            if (hasNullKey(probe, probeIdx, probeKeyCols)) {
                matchList = null; // null 键等值永不匹配
            } else {
                matchList = buildTable.get(new ColumnKey(probe, probeIdx, probeKeyArr));
            }
            matchPos = 0;
        }

        @Override
        public int fill(int[] leftRows, int[] rightRows, int outPos, int len) {
            int out = outPos;
            int end = outPos + len;
            while (out < end) {
                if (phaseTwo) {
                    // 阶段 2:未匹配 build 行(null-pad probe 侧)
                    while (unmatchedBuildIdx < build.getRowCount()
                            && matchedBuild[unmatchedBuildIdx]) {
                        unmatchedBuildIdx++;
                    }
                    if (unmatchedBuildIdx >= build.getRowCount()) {
                        break;
                    }
                    writePair(leftRows, rightRows, out, unmatchedBuildIdx, -1);
                    out++;
                    unmatchedBuildIdx++;
                    continue;
                }
                if (probeIdx >= probe.getRowCount()) {
                    if (keepUnmatchedBuild) {
                        phaseTwo = true;
                        unmatchedBuildIdx = 0;
                        continue;
                    }
                    break;
                }
                if (matchList != null && matchPos < matchList.size()) {
                    int buildIdx = matchList.get(matchPos++);
                    if (!hasResidual || residualMatches(probeRoot, left, buildLeft ? buildIdx : probeIdx,
                            right, buildLeft ? probeIdx : buildIdx, residual, ctx)) {
                        writePair(leftRows, rightRows, out, buildIdx, probeIdx);
                        matchedBuild[buildIdx] = true;
                        out++;
                    }
                    continue;
                }
                // 当前 probe 行匹配耗尽:无匹配(或 null 键)且需保留 → null-pad probe 行
                if (matchList == null && keepUnmatchedProbe) {
                    writePair(leftRows, rightRows, out, -1, probeIdx);
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

        /** 按 build 侧方向写出 (leftIdx, rightIdx) 行对:-1 = null-pad 对应侧。 */
        private void writePair(int[] leftRows, int[] rightRows, int out,
                               int buildIdx, int probeIdx) {
            if (buildLeft) {
                leftRows[out] = buildIdx;
                rightRows[out] = probeIdx;
            } else {
                leftRows[out] = probeIdx;
                rightRows[out] = buildIdx;
            }
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
        try (ValueVector result = ctx.interpreter().eval(residual, probeRoot)) {
            return !result.isNull(0) && ((BitVector) result).get(0) == 1;
        }
    }
}
