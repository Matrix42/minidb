package com.minidb.server.plan;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

public class MiniDbValues extends Values implements MiniDbRel {

    public MiniDbValues(RelOptCluster cluster, RelTraitSet traitSet,
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
        return new BatchIterator() {
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
        };
    }

    private Field arrowField(RelDataTypeField dataTypeField) {
        String name = dataTypeField.getName();
        SqlTypeName type = dataTypeField.getType().getSqlTypeName();
        ArrowType arrowType;
        switch (type) {
            case INTEGER:
                arrowType = new ArrowType.Int(32, true);
                break;
            case BIGINT:
                arrowType = new ArrowType.Int(64, true);
                break;
            case DOUBLE:
            case FLOAT:
            case REAL:
            case DECIMAL:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                break;
            case VARCHAR:
            case CHAR:
                arrowType = ArrowType.Utf8.INSTANCE;
                break;
            case BOOLEAN:
                arrowType = ArrowType.Bool.INSTANCE;
                break;
            case DATE:
                arrowType = new ArrowType.Date(DateUnit.DAY);
                break;
            case TIMESTAMP:
                arrowType = new ArrowType.Timestamp(
                        org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
                break;
            default:
                throw new UnsupportedOperationException("unsupported values type: " + type);
        }
        return new Field(name, FieldType.nullable(arrowType), List.of());
    }

    private static void setLiteral(FieldVector vector, int row, RexLiteral literal) {
        if (literal.isNull()) {
            return;
        }
        if (vector instanceof IntVector iv) {
            iv.setSafe(row, literal.getValueAs(BigDecimal.class).intValue());
        } else if (vector instanceof BigIntVector bv) {
            bv.setSafe(row, literal.getValueAs(BigDecimal.class).longValue());
        } else if (vector instanceof Float8Vector fv) {
            fv.setSafe(row, literal.getValueAs(BigDecimal.class).doubleValue());
        } else if (vector instanceof VarCharVector vv) {
            vv.setSafe(row, literal.getValueAs(String.class).getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof BitVector bv) {
            bv.setSafe(row, literal.getValueAs(Boolean.class) ? 1 : 0);
        } else if (vector instanceof DateDayVector dv) {
            Calendar cal = literal.getValueAs(Calendar.class);
            dv.setSafe(row, (int) TimeUnit.MILLISECONDS.toDays(cal.getTimeInMillis()));
        } else if (vector instanceof TimeStampMilliVector tv) {
            Calendar cal = literal.getValueAs(Calendar.class);
            tv.setSafe(row, cal.getTimeInMillis());
        } else {
            throw new UnsupportedOperationException(
                    "unsupported values vector: " + vector.getClass());
        }
    }
}
