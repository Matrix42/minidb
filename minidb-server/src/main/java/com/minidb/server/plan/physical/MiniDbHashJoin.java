package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
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
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbHashJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<Object[]> joinRows(List<Object[]> left, List<Object[]> right,
                                      JoinInfo info, JoinRelType type, ExecContext ctx) {
        List<Integer> leftKeyCols = info.leftKeys;
        List<Integer> rightKeyCols = info.rightKeys;
        // Build: key -> row indices of the left input sharing that key.
        // Null-keyed rows are skipped (they can never match).
        Map<List<Object>, List<Integer>> buildTable = new HashMap<>();
        for (int leftIdx = 0; leftIdx < left.size(); leftIdx++) {
            if (hasNullKey(left.get(leftIdx), leftKeyCols)) {
                continue;
            }
            buildTable.computeIfAbsent(buildKey(left.get(leftIdx), leftKeyCols),
                    k -> new ArrayList<>()).add(leftIdx);
        }
        boolean keepUnmatchedLeft = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean keepUnmatchedRight = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] matchedLeft = new boolean[left.size()];
        Object[] nullRowLeft = new Object[leftColumnCount()];
        Object[] nullRowRight = new Object[rightColumnCount()];
        List<Object[]> outputRows = new ArrayList<>();
        // Probe: for each right row, join with every left row of the same key.
        for (int rightIdx = 0; rightIdx < right.size(); rightIdx++) {
            List<Integer> matchingLeftRows;
            if (hasNullKey(right.get(rightIdx), rightKeyCols)) {
                matchingLeftRows = null;
            } else {
                matchingLeftRows = buildTable.get(buildKey(right.get(rightIdx), rightKeyCols));
            }
            if (matchingLeftRows != null) {
                for (int leftIdx : matchingLeftRows) {
                    outputRows.add(concat(left.get(leftIdx), right.get(rightIdx)));
                    matchedLeft[leftIdx] = true;
                }
            } else if (keepUnmatchedRight) {
                // Right-preserved outer join: emit the unmatched right row.
                outputRows.add(concat(nullRowLeft, right.get(rightIdx)));
            }
        }
        if (keepUnmatchedLeft) {
            for (int leftIdx = 0; leftIdx < left.size(); leftIdx++) {
                if (!matchedLeft[leftIdx]) {
                    outputRows.add(concat(left.get(leftIdx), nullRowRight));
                }
            }
        }
        return outputRows;
    }
}
