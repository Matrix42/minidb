package com.minidb.server.exec;

import com.minidb.server.plan.MiniDbAggregate;
import com.minidb.server.plan.MiniDbFilter;
import com.minidb.server.plan.MiniDbProject;
import com.minidb.server.plan.MiniDbRel;
import com.minidb.server.plan.MiniDbScan;
import com.minidb.server.plan.MiniDbSetOp;
import com.minidb.server.plan.MiniDbSort;
import com.minidb.server.plan.MiniDbUnion;
import com.minidb.server.plan.MiniDbValues;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

public final class Instrumenter {

    private Instrumenter() {
    }

    public static RelNode instrument(RelNode plan, Map<RelNode, NodeStats> sink) {
        return wrap(plan, sink);
    }

    private static RelNode wrap(RelNode node, Map<RelNode, NodeStats> sink) {
        // First, recursively instrument all child inputs.
        List<RelNode> originalInputs = node.getInputs();
        List<RelNode> instrumentedInputs = new ArrayList<>();
        for (RelNode in : originalInputs) {
            instrumentedInputs.add(wrap(in, sink));
        }
        // Build a copy of this node whose inputs are the instrumented children.
        RelNode copy = copyWithInputs(node, instrumentedInputs);
        NodeStats ns = new NodeStats();
        sink.put(node, ns); // key by ORIGINAL node, so ExplainExecutor can look up by plan id
        return new InstrumentedRel(copy, node, ns);
    }

    private static RelNode copyWithInputs(RelNode node, List<RelNode> inputs) {
        RelTraitSet traits = node.getTraitSet();
        if (node instanceof MiniDbScan scan) {
            return scan.copy(traits, scan.getInputs()); // scan has no inputs; reuses table
        }
        if (node instanceof MiniDbFilter filter) {
            return filter.copy(traits, inputs.get(0), filter.getCondition());
        }
        if (node instanceof MiniDbProject project) {
            return project.copy(traits, inputs.get(0), project.getProjects(), project.getRowType());
        }
        if (node instanceof MiniDbSort sort) {
            return sort.copy(traits, inputs.get(0), sort.getCollation(), sort.offset, sort.fetch);
        }
        if (node instanceof MiniDbValues values) {
            return values.copy(traits, values.getInputs());
        }
        if (node instanceof MiniDbAggregate agg) {
            return agg.copy(traits, inputs.get(0),
                    agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
        }
        if (node instanceof MiniDbUnion union) {
            return union.copy(traits, inputs, union.all);
        }
        if (node instanceof MiniDbSetOp setOp) {
            return setOp.copy(traits, inputs, setOp.all);
        }
        throw new UnsupportedOperationException("cannot instrument: " + node.getClass());
    }

    /**
     * Wraps a copied operator node. Its execute() runs the wrapped operator and
     * measures rows/batches/elapsed for THIS node only. Child measurements are
     * captured independently because each child is itself an InstrumentedRel
     * that the wrapped operator invokes via getInput().execute().
     *
     * Extends AbstractRelNode so it satisfies the RelNode type required by
     * parent operators' copy() methods, and implements MiniDbRel so the parent
     * can cast and call execute(). getRowType() delegates to the original node.
     */
    static final class InstrumentedRel extends AbstractRelNode implements MiniDbRel {
        private final RelNode wrapped;
        private final RelNode original;
        private final NodeStats stats;

        InstrumentedRel(RelNode wrapped, RelNode original, NodeStats stats) {
            super(original.getCluster(), original.getTraitSet());
            this.wrapped = wrapped;
            this.original = original;
            this.stats = stats;
            this.rowType = original.getRowType();
        }

        @Override
        public BatchIterator execute(ExecContext ctx) {
            long start = System.nanoTime();
            BatchIterator inner = ((MiniDbRel) wrapped).execute(ctx);
            BatchIterator measured = new BatchIterator() {
                @Override
                public boolean hasNext() {
                    return inner.hasNext();
                }

                @Override
                public VectorSchemaRoot next() {
                    VectorSchemaRoot batch = inner.next();
                    stats.rows += batch.getRowCount();
                    stats.batches += 1;
                    return batch;
                }

                @Override
                public void close() {
                    inner.close();
                    stats.elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
                }
            };
            return measured;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            throw new UnsupportedOperationException("InstrumentedRel cannot be copied");
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return pw.item("instrumented", original.getClass().getSimpleName());
        }
    }
}
