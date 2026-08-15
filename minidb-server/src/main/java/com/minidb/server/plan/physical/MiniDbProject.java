package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
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
        VectorSchemaRoot rows = WindowFunctions.materialize(getInput(), ctx);
        int inputCols = getInput().getRowType().getFieldCount();
        List<RexOver> windowOvers = new ArrayList<>();
        RexShuttle overExtractor = new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                windowOvers.add(over);
                return new RexInputRef(inputCols + windowOvers.size() - 1, over.getType());
            }
        };
        List<RexNode> rewrittenProjects = new ArrayList<>();
        for (RexNode project : getProjects()) {
            rewrittenProjects.add(project.accept(overExtractor));
        }
        List<List<Object>> windowColumns = new ArrayList<>();
        for (RexOver over : windowOvers) {
            windowColumns.add(WindowFunctions.computeOver(over, rows));
        }
        VectorSchemaRoot windowBatch = buildWindowBatch(rows, windowColumns, inputCols, windowOvers, ctx);
        try {
            List<FieldVector> outputVectors = new ArrayList<>();
            for (int projectIdx = 0; projectIdx < rewrittenProjects.size(); projectIdx++) {
                ValueVector evaluated = ctx.interpreter().eval(rewrittenProjects.get(projectIdx), windowBatch);
                RelDataTypeField field = getRowType().getFieldList().get(projectIdx);
                outputVectors.add(rename(evaluated, field.getName(), ctx));
            }
            VectorSchemaRoot outputRoot = VectorSchemaRoot.of(outputVectors.toArray(new FieldVector[0]));
            outputRoot.setRowCount(rows.getRowCount());
            boolean[] done = {false};
            return new BatchIterator() {
                @Override
                public boolean hasNext() {
                    return !done[0];
                }

                @Override
                public VectorSchemaRoot next() {
                    done[0] = true;
                    return outputRoot;
                }

                @Override
                public void close() {
                    outputRoot.close();
                }
            };
        } finally {
            windowBatch.close();
            rows.close();
        }
    }

    private VectorSchemaRoot buildWindowBatch(VectorSchemaRoot rows,
                                              List<List<Object>> windowColumns,
                                              int inputCols,
                                              List<RexOver> windowOvers,
                                              ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField field : getInput().getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(field).createVector(ctx.allocator()));
        }
        for (RexOver over : windowOvers) {
            vectors.add(ArrowTypes.field(over.getType(), "w" + windowOvers.indexOf(over))
                    .createVector(ctx.allocator()));
        }
        for (FieldVector vector : vectors) {
            vector.setInitialCapacity(rows.getRowCount());
            vector.allocateNew();
        }
        // 输入列:从物化的列式 root 逐列 copy(不装箱)。
        for (int colIdx = 0; colIdx < inputCols; colIdx++) {
            for (int rowIdx = 0; rowIdx < rows.getRowCount(); rowIdx++) {
                RowCopier.copyRow(rows.getVector(colIdx), rowIdx, vectors.get(colIdx), rowIdx);
            }
        }
        // 窗口列:窗口结果是每行一个装箱标量,writeObject 写回。
        for (int windowColIdx = 0; windowColIdx < windowColumns.size(); windowColIdx++) {
            List<Object> windowColumn = windowColumns.get(windowColIdx);
            for (int rowIdx = 0; rowIdx < windowColumn.size(); rowIdx++) {
                RowVectors.writeObject(vectors.get(inputCols + windowColIdx), rowIdx, windowColumn.get(rowIdx));
            }
        }
        for (FieldVector vector : vectors) {
            vector.setValueCount(rows.getRowCount());
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

}
