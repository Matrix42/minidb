package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RepeatUnion;

/**
 * Recursive CTE (WITH RECURSIVE). Executes the non-recursive seed once, then
 * repeatedly evaluates the iterative term against the working table until it
 * stops producing new rows (or iterationLimit is reached). The working table
 * lives in ExecContext under the transient table name so the recursive body's
 * scan can read it back.
 */
public class MiniDbRepeatUnion extends RepeatUnion implements MiniDbRel {

    public MiniDbRepeatUnion(RelOptCluster cluster, RelTraitSet traitSet,
                             RelNode seed, RelNode iterative, boolean all,
                             int iterationLimit, RelOptTable transientTable) {
        super(cluster, traitSet, seed, iterative, all, iterationLimit, transientTable);
    }

    @Override
    public MiniDbRepeatUnion copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbRepeatUnion(getCluster(), traitSet, inputs.get(0), inputs.get(1),
                all, iterationLimit, getTransientTable());
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        String transientName = transientName();
        List<Object[]> result = new ArrayList<>();
        // Only UNION (not UNION ALL) dedups; the set spans the seed and every
        // iteration so a row is emitted at most once globally.
        Set<List<Object>> seen = all ? null : new LinkedHashSet<>();

        List<Object[]> workingRows = RowVectors.materialize(getSeedRel(), ctx);
        for (Object[] row : workingRows) {
            if (addIfNew(seen, row)) {
                result.add(row);
            }
        }

        int iterations = 0;
        while (iterationLimit < 0 || iterations < iterationLimit) {
            ctx.putTransientTable(transientName, workingRows);
            List<Object[]> produced;
            try {
                produced = RowVectors.materialize(getIterativeRel(), ctx);
            } finally {
                ctx.removeTransientTable(transientName);
            }
            if (produced.isEmpty()) {
                break;
            }
            List<Object[]> newRows = new ArrayList<>();
            for (Object[] row : produced) {
                if (addIfNew(seen, row)) {
                    result.add(row);
                    newRows.add(row);
                }
            }
            if (newRows.isEmpty()) {
                // Everything this iteration produced was already emitted;
                // iterating again would only repeat it, so stop.
                break;
            }
            workingRows = newRows;
            iterations++;
        }

        VectorSchemaRoot root =
                RowVectors.buildRoot(result, getRowType(), ctx.allocator());
        boolean[] done = {false};
        return new BatchIterator() {
            @Override
            public boolean hasNext() {
                return !done[0];
            }

            @Override
            public VectorSchemaRoot next() {
                done[0] = true;
                return root;
            }

            @Override
            public void close() {
                root.close();
            }
        };
    }

    /** The transient table name (last segment of its qualified name) — the
     *  same key the recursive body's scan looks up in ExecContext. */
    private String transientName() {
        RelOptTable table = getTransientTable();
        List<String> qualified = table.getQualifiedName();
        return qualified.get(qualified.size() - 1);
    }

    /** Records {@code row} in the dedup set and reports whether it is new.
     *  With UNION ALL (all=true) there is no set and every row is new. */
    private static boolean addIfNew(Set<List<Object>> seen, Object[] row) {
        if (seen == null) {
            return true;
        }
        return seen.add(Arrays.asList(row));
    }
}
