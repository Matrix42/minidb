package com.minidb.server.exec;

import com.minidb.server.exec.functions.Kernels;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;

/**
 * 列向量值类型转换的单一来源:供 CAST 表达式求值与 ALTER COLUMN SET DATA TYPE
 * 共用,避免两处转换语义漂移。目标类型按 {@link ColumnType} 分派(与
 * {@link ArrowTypes} 的类型映射一致),NULL 透传,源值读取用
 * {@code asLong/asDouble/asString/asBoolean} 做跨类型规约。
 */
public final class VectorCasts {

    private VectorCasts() {
    }

    /** 把源向量的每一行值转成目标类型的新向量(不关闭 src,调用方负责)。 */
    public static FieldVector cast(ValueVector src, ColumnType target, int precision,
                                   int scale, BufferAllocator allocator) {
        int rows = src.getValueCount();
        switch (target) {
            case SMALLINT: {
                SmallIntVector out = new SmallIntVector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, (short) asLong(src, i));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case INTEGER: {
                IntVector out = new IntVector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, (int) asLong(src, i));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case BIGINT: {
                BigIntVector out = new BigIntVector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, asLong(src, i));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case REAL:
            case FLOAT: {
                Float4Vector out = new Float4Vector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, (float) asDouble(src, i));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case DOUBLE: {
                Float8Vector out = new Float8Vector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, asDouble(src, i));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case DECIMAL:
            case NUMERIC: {
                DecimalVector out = (DecimalVector) ArrowTypes.field(
                        new ColumnMeta("cast", target, precision, scale)).createVector(allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, Kernels.scaleTo(out, new BigDecimal(asString(src, i))));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case VARCHAR:
            case CHAR:
            case NCHAR:
            case NVARCHAR: {
                VarCharVector out = new VarCharVector("cast", allocator);
                out.allocateNew();
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, asString(src, i).getBytes(StandardCharsets.UTF_8));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case BOOLEAN: {
                BitVector out = new BitVector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, asBoolean(src, i) ? 1 : 0);
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case DATE: {
                DateDayVector out = new DateDayVector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else if (src instanceof DateDayVector ddv) {
                        out.setSafe(i, ddv.get(i));
                    } else {
                        out.setSafe(i, new DateString(asString(src, i)).getDaysSinceEpoch());
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case TIME: {
                TimeMilliVector out = new TimeMilliVector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, (int) asLong(src, i));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case TIMESTAMP: {
                TimeStampMilliVector out = new TimeStampMilliVector("cast", allocator);
                out.allocateNew(rows);
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else if (src instanceof TimeStampMilliVector tsv) {
                        out.setSafe(i, tsv.get(i));
                    } else {
                        out.setSafe(i, new TimestampString(asString(src, i)).getMillisSinceEpoch());
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            case BINARY:
            case VARBINARY: {
                VarBinaryVector out = new VarBinaryVector("cast", allocator);
                out.allocateNew();
                for (int i = 0; i < rows; i++) {
                    if (src.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setSafe(i, asString(src, i).getBytes(StandardCharsets.UTF_8));
                    }
                }
                out.setValueCount(rows);
                return out;
            }
            default:
                throw new UnsupportedOperationException("unsupported CAST target: " + target);
        }
    }

    private static long asLong(ValueVector v, int i) {
        if (v instanceof SmallIntVector sv) {
            return sv.get(i);
        }
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof Float4Vector fv) {
            return (long) fv.get(i);
        }
        if (v instanceof Float8Vector fv) {
            return (long) fv.get(i);
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).longValue();
        }
        if (v instanceof BitVector bv) {
            return bv.get(i);
        }
        if (v instanceof VarCharVector vv) {
            return Long.parseLong(new String(vv.get(i), StandardCharsets.UTF_8).trim());
        }
        throw new IllegalArgumentException("not a numeric vector: " + v.getClass());
    }

    private static double asDouble(ValueVector v, int i) {
        if (v instanceof SmallIntVector sv) {
            return sv.get(i);
        }
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(i);
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(i);
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).doubleValue();
        }
        if (v instanceof BitVector bv) {
            return bv.get(i);
        }
        if (v instanceof VarCharVector vv) {
            return Double.parseDouble(new String(vv.get(i), StandardCharsets.UTF_8).trim());
        }
        throw new IllegalArgumentException("not a numeric vector: " + v.getClass());
    }

    private static String asString(ValueVector v, int i) {
        if (v instanceof VarCharVector vv) {
            return new String(vv.get(i), StandardCharsets.UTF_8);
        }
        if (v instanceof SmallIntVector sv) {
            return Short.toString(sv.get(i));
        }
        if (v instanceof IntVector iv) {
            return Integer.toString(iv.get(i));
        }
        if (v instanceof BigIntVector bv) {
            return Long.toString(bv.get(i));
        }
        if (v instanceof Float4Vector fv) {
            return Float.toString(fv.get(i));
        }
        if (v instanceof Float8Vector fv) {
            return Double.toString(fv.get(i));
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).toPlainString();
        }
        if (v instanceof BitVector bv) {
            return bv.get(i) == 1 ? "true" : "false";
        }
        throw new IllegalArgumentException("cannot cast to string: " + v.getClass());
    }

    private static boolean asBoolean(ValueVector v, int i) {
        if (v instanceof BitVector bv) {
            return bv.get(i) == 1;
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(i) != 0;
        }
        if (v instanceof IntVector iv) {
            return iv.get(i) != 0;
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i) != 0;
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(i) != 0;
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(i) != 0;
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).signum() != 0;
        }
        if (v instanceof VarCharVector vv) {
            return Boolean.parseBoolean(new String(vv.get(i), StandardCharsets.UTF_8).trim());
        }
        throw new IllegalArgumentException("cannot cast to boolean: " + v.getClass());
    }
}
