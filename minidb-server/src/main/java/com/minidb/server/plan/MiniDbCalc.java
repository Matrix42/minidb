package com.minidb.server.plan;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

/**
 * Generalized Project+Filter over a RexProgram: evaluates the program's
 * projections on each input row, keeps rows satisfying the program's
 * condition (if any), and renames output columns to the Calc row type.
 */
public class MiniDbCalc extends Calc implements MiniDbRel {

    public MiniDbCalc(RelOptCluster cluster, RelTraitSet traitSet,
                      RelNode input, RexProgram program) {
        super(cluster, traitSet, java.util.List.of(), input, program);
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode input, RexProgram program) {
        return new MiniDbCalc(getCluster(), traitSet, input, program);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        RexProgram program = getProgram();
        List<RexNode> projects = new ArrayList<>();
        for (RexLocalRef ref : program.getProjectList()) {
            projects.add(program.expandLocalRef(ref));
        }
        RexLocalRef condRef = program.getCondition();
        RexNode condition = condRef == null ? null : program.expandLocalRef(condRef);

        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        Deque<VectorSchemaRoot> owned = new ArrayDeque<>();
        return new BatchIterator() {
            VectorSchemaRoot pending;

            @Override
            public boolean hasNext() {
                while (pending == null && input.hasNext()) {
                    VectorSchemaRoot batch = input.next();
                    pending = calcBatch(batch, projects, condition, ctx);
                    if (pending != null) {
                        owned.add(pending);
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
        };
    }

    private VectorSchemaRoot calcBatch(VectorSchemaRoot batch, List<RexNode> projects,
                                       RexNode condition, ExecContext ctx) {
        List<ValueVector> evals = new ArrayList<>();
        List<FieldVector> outVectors = new ArrayList<>();
        ValueVector cond = null;
        try {
            for (RexNode project : projects) {
                evals.add(ctx.interpreter().eval(project, batch));
            }
            if (condition != null) {
                cond = ctx.interpreter().eval(condition, batch);
            }
            int kept = keptRows(batch, cond);
            if (kept == 0) {
                return null;
            }
            RelDataType rowType = getRowType();
            for (int p = 0; p < evals.size(); p++) {
                RelDataTypeField field = rowType.getFieldList().get(p);
                outVectors.add(renameFiltered(evals.get(p), field.getName(),
                        cond, kept, ctx));
            }
            VectorSchemaRoot out = VectorSchemaRoot.of(
                    outVectors.toArray(new FieldVector[0]));
            out.setRowCount(kept);
            return out;
        } catch (RuntimeException e) {
            for (FieldVector v : outVectors) {
                v.close();
            }
            throw e;
        } finally {
            for (ValueVector v : evals) {
                v.close();
            }
            if (cond != null) {
                cond.close();
            }
        }
    }

    private static int keptRows(VectorSchemaRoot batch, ValueVector cond) {
        if (cond == null) {
            return batch.getRowCount();
        }
        int kept = 0;
        for (int i = 0; i < batch.getRowCount(); i++) {
            if (!cond.isNull(i) && ((BitVector) cond).get(i) == 1) {
                kept++;
            }
        }
        return kept;
    }

    private FieldVector renameFiltered(ValueVector src, String name,
                                       ValueVector cond, int kept, ExecContext ctx) {
        Field targetField = new Field(name, src.getField().getFieldType(), List.of());
        FieldVector dst = targetField.createVector(ctx.allocator());
        dst.setInitialCapacity(kept);
        dst.allocateNew();
        int dstIdx = 0;
        for (int i = 0; i < src.getValueCount(); i++) {
            if (cond == null || (!cond.isNull(i) && ((BitVector) cond).get(i) == 1)) {
                dst.copyFromSafe(i, dstIdx++, src);
            }
        }
        dst.setValueCount(kept);
        return dst;
    }
}
