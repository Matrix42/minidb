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

/** Hash join: builds a hash table on the left input keyed by the equi
 *  columns and probes with the right input. Equi-join only. */
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
        List<Integer> lk = info.leftKeys;
        List<Integer> rk = info.rightKeys;
        Map<List<Object>, List<Integer>> hash = new HashMap<>();
        for (int i = 0; i < left.size(); i++) {
            if (containsNull(left.get(i), lk)) {
                continue;
            }
            hash.computeIfAbsent(keyOf(left.get(i), lk), k -> new ArrayList<>()).add(i);
        }
        boolean leftPreserved = type == JoinRelType.LEFT || type == JoinRelType.FULL;
        boolean rightPreserved = type == JoinRelType.RIGHT || type == JoinRelType.FULL;
        boolean[] leftMatched = new boolean[left.size()];
        Object[] nullLeft = new Object[left.get(0).length];
        Object[] nullRight = new Object[right.get(0).length];
        List<Object[]> out = new ArrayList<>();
        for (int j = 0; j < right.size(); j++) {
            List<Integer> matches;
            if (containsNull(right.get(j), rk)) {
                matches = null;
            } else {
                matches = hash.get(keyOf(right.get(j), rk));
            }
            if (matches != null) {
                for (int i : matches) {
                    out.add(concat(left.get(i), right.get(j)));
                    leftMatched[i] = true;
                }
            } else if (rightPreserved) {
                out.add(concat(nullLeft, right.get(j)));
            }
        }
        if (leftPreserved) {
            for (int i = 0; i < left.size(); i++) {
                if (!leftMatched[i]) {
                    out.add(concat(left.get(i), nullRight));
                }
            }
        }
        return out;
    }
}
