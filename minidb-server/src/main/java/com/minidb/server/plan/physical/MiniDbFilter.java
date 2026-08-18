package com.minidb.server.plan.physical;

import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class MiniDbFilter extends Filter implements MiniDbRel {

    public MiniDbFilter(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, RexNode condition) {
        super(cluster, traitSet, java.util.List.of(), input, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new MiniDbFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        Deque<VectorSchemaRoot> owned = new ArrayDeque<>();
        return BatchIterator.interruptible(new BatchIterator() {
            VectorSchemaRoot pending;

            @Override
            public boolean hasNext() {
                while (pending == null && input.hasNext()) {
                    VectorSchemaRoot batch = input.next();
                    ValueVector condition = ctx.interpreter().eval(getCondition(), batch);
                    try {
                        int kept = 0;
                        for (int i = 0; i < batch.getRowCount(); i++) {
                            if (!condition.isNull(i) && ((BitVector) condition).get(i) == 1) {
                                kept++;
                            }
                        }
                        if (kept == 0) {
                            continue;
                        }
                        VectorSchemaRoot out = VectorSchemaRoot.create(
                                batch.getSchema(), ctx.allocator());
                        out.allocateNew();
                        int dst = 0;
                        for (int i = 0; i < batch.getRowCount(); i++) {
                            if (!condition.isNull(i)
                                    && ((BitVector) condition).get(i) == 1) {
                                RowCopier.copyRow(batch, i, out, dst++);
                            }
                        }
                        out.setRowCount(kept);
                        owned.add(out);
                        pending = out;
                    } finally {
                        condition.close();
                    }
                }
                return pending != null;
            }

            @Override
            public VectorSchemaRoot next() {
                VectorSchemaRoot out = pending;
                pending = null;
                return out;
            }

            @Override
            public void close() {
                input.close();
                for (VectorSchemaRoot root : owned) {
                    root.close();
                }
                owned.clear();
            }
        });
    }
}
