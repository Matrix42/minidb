package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;

/**
 * Generalized Project+Filter over a RexProgram: evaluates the program's
 * projections on each input row, keeps rows satisfying the program's condition
 * (if any), and renames output columns to the Calc row type.
 *
 * <p>Window functions (RexOver) take an eager path: the input is materialized,
 * the condition filters rows first (SQL WHERE runs before window functions),
 * then each RexOver is computed and the projections are evaluated.
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
        List<RexNode> projects = expandedProjects();
        RexNode condition = expandedCondition();
        for (RexNode project : projects) {
            if (containsOver(project)) {
                return windowExecute(projects, condition, ctx);
            }
        }
        return lazyExecute(projects, condition, ctx);
    }

    // ---- RexProgram accessors ----

    private List<RexNode> expandedProjects() {
        List<RexNode> projects = new ArrayList<>();
        for (RexLocalRef ref : getProgram().getProjectList()) {
            projects.add(getProgram().expandLocalRef(ref));
        }
        return projects;
    }

    private RexNode expandedCondition() {
        RexLocalRef condRef = getProgram().getCondition();
        return condRef == null ? null : getProgram().expandLocalRef(condRef);
    }

    private static boolean containsOver(RexNode node) {
        if (node instanceof RexOver) {
            return true;
        }
        if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                if (containsOver(operand)) {
                    return true;
                }
            }
        }
        return false;
    }

    // ---- eager window path ----

    private BatchIterator windowExecute(List<RexNode> projects, RexNode condition,
                                        ExecContext ctx) {
        // WHERE runs before window functions, so filter the materialized rows
        // before computing the windows.
        List<Object[]> rows = filterRows(WindowFunctions.materialize(getInput(), ctx), condition, ctx);

        int inputCols = getInput().getRowType().getFieldCount();
        List<RexOver> overs = new ArrayList<>();
        RexShuttle extract = new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                overs.add(over);
                return new RexInputRef(inputCols + overs.size() - 1, over.getType());
            }
        };
        List<RexNode> rewritten = new ArrayList<>();
        for (RexNode project : projects) {
            rewritten.add(project.accept(extract));
        }
        List<List<Object>> windowCols = new ArrayList<>();
        for (RexOver over : overs) {
            windowCols.add(WindowFunctions.computeOver(over, rows));
        }

        VectorSchemaRoot joined = buildWindowBatch(rows, windowCols, inputCols, overs, ctx);
        try {
            List<FieldVector> outVectors = new ArrayList<>();
            for (int p = 0; p < rewritten.size(); p++) {
                ValueVector evaluated = ctx.interpreter().eval(rewritten.get(p), joined);
                RelDataTypeField field = getRowType().getFieldList().get(p);
                outVectors.add(rename(evaluated, field.getName(), ctx));
            }
            VectorSchemaRoot out = VectorSchemaRoot.of(outVectors.toArray(new FieldVector[0]));
            out.setRowCount(rows.size());
            boolean[] done = {false};
            return new BatchIterator() {
                @Override
                public boolean hasNext() {
                    return !done[0];
                }

                @Override
                public VectorSchemaRoot next() {
                    done[0] = true;
                    return out;
                }

                @Override
                public void close() {
                    out.close();
                }
            };
        } finally {
            joined.close();
        }
    }

    private List<Object[]> filterRows(List<Object[]> rows, RexNode condition, ExecContext ctx) {
        if (condition == null) {
            return rows;
        }
        VectorSchemaRoot inputBatch = buildInputBatch(rows, ctx);
        try {
            ValueVector cond = ctx.interpreter().eval(condition, inputBatch);
            try {
                List<Object[]> kept = new ArrayList<>();
                for (int i = 0; i < rows.size(); i++) {
                    if (!cond.isNull(i) && ((BitVector) cond).get(i) == 1) {
                        kept.add(rows.get(i));
                    }
                }
                return kept;
            } finally {
                cond.close();
            }
        } finally {
            inputBatch.close();
        }
    }

    private VectorSchemaRoot buildInputBatch(List<Object[]> rows, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField field : getInput().getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(field).createVector(ctx.allocator()));
        }
        for (FieldVector vector : vectors) {
            vector.setInitialCapacity(rows.size());
            vector.allocateNew();
        }
        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            Object[] row = rows.get(rowIdx);
            for (int colIdx = 0; colIdx < row.length; colIdx++) {
                writeObject(vectors.get(colIdx), rowIdx, row[colIdx]);
            }
        }
        for (FieldVector vector : vectors) {
            vector.setValueCount(rows.size());
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    private VectorSchemaRoot buildWindowBatch(List<Object[]> rows, List<List<Object>> windowCols,
                                              int inputCols, List<RexOver> overs, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField field : getInput().getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(field).createVector(ctx.allocator()));
        }
        for (RexOver over : overs) {
            vectors.add(ArrowTypes.field(over.getType(), "w" + overs.indexOf(over))
                    .createVector(ctx.allocator()));
        }
        for (FieldVector vector : vectors) {
            vector.setInitialCapacity(rows.size());
            vector.allocateNew();
        }
        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            Object[] row = rows.get(rowIdx);
            for (int colIdx = 0; colIdx < inputCols; colIdx++) {
                writeObject(vectors.get(colIdx), rowIdx, row[colIdx]);
            }
        }
        for (int w = 0; w < windowCols.size(); w++) {
            List<Object> col = windowCols.get(w);
            for (int rowIdx = 0; rowIdx < col.size(); rowIdx++) {
                writeObject(vectors.get(inputCols + w), rowIdx, col.get(rowIdx));
            }
        }
        for (FieldVector vector : vectors) {
            vector.setValueCount(rows.size());
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    private FieldVector rename(ValueVector src, String name, ExecContext ctx) {
        Field targetField = new Field(name, src.getField().getFieldType(), List.of());
        FieldVector dst = targetField.createVector(ctx.allocator());
        dst.setInitialCapacity(src.getValueCount());
        dst.allocateNew();
        for (int i = 0; i < src.getValueCount(); i++) {
            dst.copyFromSafe(i, i, src);
        }
        dst.setValueCount(src.getValueCount());
        src.close();
        return dst;
    }

    // ---- lazy (non-window) path ----

    private BatchIterator lazyExecute(List<RexNode> projects, RexNode condition, ExecContext ctx) {
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
                outVectors.add(renameFiltered(evals.get(p), field.getName(), cond, kept, ctx));
            }
            VectorSchemaRoot out = VectorSchemaRoot.of(outVectors.toArray(new FieldVector[0]));
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

    private static void writeObject(FieldVector out, int row, Object o) {
        if (o == null) {
            out.setNull(row);
            return;
        }
        if (out instanceof IntVector iv) {
            iv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof BigIntVector bv) {
            bv.setSafe(row, ((Number) o).longValue());
        } else if (out instanceof Float8Vector fv) {
            fv.setSafe(row, ((Number) o).doubleValue());
        } else if (out instanceof VarCharVector vv) {
            vv.setSafe(row, o.toString().getBytes(StandardCharsets.UTF_8));
        } else if (out instanceof BitVector bv) {
            bv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof DateDayVector dv) {
            dv.setSafe(row, ((Number) o).intValue());
        } else if (out instanceof TimeStampMilliVector tv) {
            tv.setSafe(row, ((Number) o).longValue());
        } else {
            throw new UnsupportedOperationException(
                    "cannot write value to " + out.getMinorType());
        }
    }
}
