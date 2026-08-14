package com.minidb.server.plan.physical;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataTypeField;

public class MiniDbUnion extends Union implements MiniDbRel {

    public MiniDbUnion(RelOptCluster cluster, RelTraitSet traitSet,
                       List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MiniDbUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<BatchIterator> iterators = new ArrayList<>();
        List<VectorSchemaRoot> batches = new ArrayList<>();
        int total = 0;
        for (RelNode input : getInputs()) {
            BatchIterator it = ((MiniDbRel) input).execute(ctx);
            iterators.add(it);
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                batches.add(b);
                total += b.getRowCount();
            }
        }
        if (batches.isEmpty()) {
            for (BatchIterator it : iterators) {
                it.close();
            }
            return BatchIterator.empty();
        }
        // copy BEFORE closing inputs: Project/Filter own their batches and
        // release them on close
        VectorSchemaRoot out = mergeBatches(batches, total, ctx);
        for (BatchIterator it : iterators) {
            it.close();
        }

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
    }

    private VectorSchemaRoot mergeBatches(List<VectorSchemaRoot> batches, int total,
                                          ExecContext ctx) {
        boolean distinct = !all;
        Set<List<Object>> seen = distinct ? new LinkedHashSet<>() : null;
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }
        VectorSchemaRoot out = VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        try {
            int dst = 0;
            for (VectorSchemaRoot batch : batches) {
                for (int i = 0; i < batch.getRowCount(); i++) {
                    if (distinct && !seen.add(rowKey(batch, i))) {
                        continue;
                    }
                    RowCopier.copyRow(batch, i, out, dst++);
                }
            }
            // setValueCount BEFORE of() so the root's rowCount picks it up
            // (of() derives rowCount from the first vector's valueCount)
            for (FieldVector v : vectors) {
                v.setValueCount(dst);
            }
            return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        } catch (RuntimeException e) {
            out.close();
            throw e;
        }
    }

    private static List<Object> rowKey(VectorSchemaRoot batch, int row) {
        List<Object> key = new ArrayList<>(batch.getFieldVectors().size());
        for (ValueVector v : batch.getFieldVectors()) {
            key.add(readObject(v, row));
        }
        return key;
    }

    private static Object readObject(ValueVector v, int row) {
        if (v.isNull(row)) {
            return null;
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(row);
        }
        if (v instanceof IntVector iv) {
            return iv.get(row);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(row);
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(row);
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(row);
        }
        if (v instanceof VarCharVector vv) {
            return new String(vv.get(row), StandardCharsets.UTF_8);
        }
        if (v instanceof BitVector bv) {
            return bv.get(row);
        }
        if (v instanceof DateDayVector dv) {
            return dv.get(row);
        }
        if (v instanceof TimeMilliVector tv) {
            return tv.get(row);
        }
        if (v instanceof TimeStampMilliVector tv) {
            return tv.get(row);
        }
        if (v instanceof VarBinaryVector bv) {
            return bv.get(row);
        }
        throw new UnsupportedOperationException(
                "cannot deduplicate column type: " + v.getMinorType());
    }
}
