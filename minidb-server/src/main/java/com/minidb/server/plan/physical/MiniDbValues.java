package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.functions.Kernels;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;

import com.google.common.collect.ImmutableList;
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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BitString;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MiniDbValues extends Values implements MiniDbRel {

    public MiniDbValues(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> tuples) {
        super(cluster, rowType, tuples, traitSet);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbValues(getCluster(), traitSet, getRowType(), tuples);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        int rows = tuples.size();
        List<FieldVector> vectors = new ArrayList<>();
        for (int col = 0; col < getRowType().getFieldCount(); col++) {
            RelDataTypeField field = getRowType().getFieldList().get(col);
            FieldVector vector = arrowField(field).createVector(ctx.allocator());
            vector.setInitialCapacity(rows);
            vector.allocateNew();
            for (int row = 0; row < rows; row++) {
                setLiteral(vector, row, tuples.get(row).get(col));
            }
            vector.setValueCount(rows);
            vectors.add(vector);
        }
        VectorSchemaRoot out = VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        out.setRowCount(rows);
        boolean[] emitted = {false};
        return BatchIterator.interruptible(
                new BatchIterator() {
                    @Override
                    public boolean hasNext() {
                        return !emitted[0];
                    }

                    @Override
                    public VectorSchemaRoot next() {
                        emitted[0] = true;
                        return out;
                    }

                    @Override
                    public void close() {
                        out.close();
                    }
                });
    }

    private Field arrowField(RelDataTypeField dataTypeField) {
        // Delegate to ArrowTypes so VALUES literals produce the same native
        // vectors as the expression layer (SMALLINT/FLOAT/REAL/DECIMAL/TIME/
        // VARBINARY), including DECIMAL precision/scale from the row type.
        return ArrowTypes.field(dataTypeField);
    }

    private static void setLiteral(FieldVector vector, int row, RexLiteral literal) {
        if (literal.isNull()) {
            return;
        }
        if (vector instanceof SmallIntVector sv) {
            sv.setSafe(row, literal.getValueAs(BigDecimal.class).shortValue());
        } else if (vector instanceof IntVector iv) {
            iv.setSafe(row, literal.getValueAs(BigDecimal.class).intValue());
        } else if (vector instanceof BigIntVector bv) {
            bv.setSafe(row, literal.getValueAs(BigDecimal.class).longValue());
        } else if (vector instanceof Float4Vector fv) {
            fv.setSafe(row, literal.getValueAs(BigDecimal.class).floatValue());
        } else if (vector instanceof Float8Vector fv) {
            fv.setSafe(row, literal.getValueAs(BigDecimal.class).doubleValue());
        } else if (vector instanceof DecimalVector dv) {
            dv.setSafe(row, Kernels.scaleTo(dv, literal.getValueAs(BigDecimal.class)));
        } else if (vector instanceof VarCharVector vv) {
            vv.setSafe(row, literal.getValueAs(String.class).getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof BitVector bv) {
            bv.setSafe(row, literal.getValueAs(Boolean.class) ? 1 : 0);
        } else if (vector instanceof DateDayVector dv) {
            Calendar cal = literal.getValueAs(Calendar.class);
            dv.setSafe(row, (int) TimeUnit.MILLISECONDS.toDays(cal.getTimeInMillis()));
        } else if (vector instanceof TimeMilliVector tv) {
            Calendar cal = literal.getValueAs(Calendar.class);
            int millis =
                    (int)
                            (cal.get(Calendar.HOUR_OF_DAY) * 3_600_000L
                                    + cal.get(Calendar.MINUTE) * 60_000L
                                    + cal.get(Calendar.SECOND) * 1_000L
                                    + cal.get(Calendar.MILLISECOND));
            tv.setSafe(row, millis);
        } else if (vector instanceof TimeStampMilliVector tv) {
            Calendar cal = literal.getValueAs(Calendar.class);
            tv.setSafe(row, cal.getTimeInMillis());
        } else if (vector instanceof VarBinaryVector bv) {
            bv.setSafe(row, literalBytes(literal));
        } else {
            throw new UnsupportedOperationException(
                    "unsupported values vector: " + vector.getClass());
        }
    }

    /**
     * BINARY/VARBINARY 字面量的字节值:Calcite 1.42 把 `X'...'`/`B'...'` 存为 ByteString, 旧版本可能存 byte[] 或
     * BitString,三者都兼容。
     */
    private static byte[] literalBytes(RexLiteral literal) {
        Object raw = literal.getValue();
        if (raw instanceof ByteString byteString) {
            return byteString.getBytes();
        }
        if (raw instanceof BitString bitString) {
            return bitString.getAsByteArray();
        }
        throw new UnsupportedOperationException(
                "unsupported binary literal value: " + raw.getClass());
    }
}
