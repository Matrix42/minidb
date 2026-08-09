package com.minidb.server.plan;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
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
import org.apache.calcite.rex.RexNode;

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
        List<Field> fields = new ArrayList<>();
        try {
            for (int p = 0; p < projects.size(); p++) {
                ValueVector evaluated = ctx.interpreter().eval(projects.get(p), batch);
                RelDataTypeField field = rowType.getFieldList().get(p);
                FieldVector renamed = rename(evaluated, field.getName(), ctx);
                outVectors.add(renamed);
                fields.add(renamed.getField());
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
            dst.copyFromSafe(i, i, (FieldVector) src);
        }
        dst.setValueCount(src.getValueCount());
        src.close();
        return dst;
    }
}
