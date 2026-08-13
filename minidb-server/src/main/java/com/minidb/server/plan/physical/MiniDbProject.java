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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;

public class MiniDbProject extends Project implements MiniDbRel {

    public MiniDbProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                         List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traitSet, java.util.List.of(), input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RelDataType rowType) {
        return new MiniDbProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        for (RexNode p : getProjects()) {
            if (containsOver(p)) {
                return windowExecute(ctx);
            }
        }
        return lazyExecute(ctx);
    }

    private static boolean containsOver(RexNode node) {
        if (node instanceof RexOver) {
            return true;
        }
        if (node instanceof RexCall call) {
            for (RexNode o : call.getOperands()) {
                if (containsOver(o)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Eager window path: materialize the input, extract every RexOver (in
     * order) into dedicated window columns referenced by the rewritten
     * projections, then evaluate the rewritten expressions row-wise.
     */
    private BatchIterator windowExecute(ExecContext ctx) {
        List<Object[]> rows = WindowFunctions.materialize(getInput(), ctx);
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
        for (RexNode p : getProjects()) {
            rewritten.add(p.accept(extract));
        }
        List<List<Object>> winCols = new ArrayList<>();
        for (RexOver over : overs) {
            winCols.add(WindowFunctions.computeOver(over, rows));
        }
        VectorSchemaRoot joined = buildWindowBatch(rows, winCols, inputCols, overs, ctx);
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

    private VectorSchemaRoot buildWindowBatch(List<Object[]> rows,
                                              List<List<Object>> winCols,
                                              int inputCols,
                                              List<RexOver> overs,
                                              ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getInput().getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (RexOver over : overs) {
            vectors.add(ArrowTypes.field(over.getType(), "w" + overs.indexOf(over)).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(rows.size());
            v.allocateNew();
        }
        for (int r = 0; r < rows.size(); r++) {
            Object[] row = rows.get(r);
            for (int c = 0; c < inputCols; c++) {
                writeObject(vectors.get(c), r, row[c]);
            }
        }
        for (int w = 0; w < winCols.size(); w++) {
            List<Object> col = winCols.get(w);
            for (int r = 0; r < col.size(); r++) {
                writeObject(vectors.get(inputCols + w), r, col.get(r));
            }
        }
        for (FieldVector v : vectors) {
            v.setValueCount(rows.size());
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    private BatchIterator lazyExecute(ExecContext ctx) {
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        Deque<VectorSchemaRoot> owned = new ArrayDeque<>();
        return new BatchIterator() {
            VectorSchemaRoot pending;

            @Override
            public boolean hasNext() {
                if (pending == null && input.hasNext()) {
                    VectorSchemaRoot batch = input.next();
                    pending = projectBatch(batch, ctx);
                    owned.add(pending);
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

    private VectorSchemaRoot projectBatch(VectorSchemaRoot batch, ExecContext ctx) {
        List<? extends RexNode> projects = getProjects();
        RelDataType rowType = getRowType();
        List<FieldVector> outVectors = new ArrayList<>();
        try {
            for (int p = 0; p < projects.size(); p++) {
                ValueVector evaluated = ctx.interpreter().eval(projects.get(p), batch);
                RelDataTypeField field = rowType.getFieldList().get(p);
                FieldVector renamed = rename(evaluated, field.getName(), ctx);
                outVectors.add(renamed);
            }
        } catch (RuntimeException e) {
            for (FieldVector v : outVectors) {
                v.close();
            }
            throw e;
        }
        VectorSchemaRoot out = VectorSchemaRoot.of(outVectors.toArray(new FieldVector[0]));
        out.setRowCount(batch.getRowCount());
        return out;
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
