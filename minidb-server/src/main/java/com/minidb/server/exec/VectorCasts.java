package com.minidb.server.exec;

import com.minidb.server.exec.functions.Kernels;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;

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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * 列向量值类型转换的单一来源:供 CAST 表达式求值与 ALTER COLUMN SET DATA TYPE 共用,避免两处转换语义漂移。目标类型按 {@link ColumnType}
 * 分派(与 {@link ArrowTypes} 的类型映射一致),NULL 透传,源值读取用 {@code asLong/asDouble/asString/asBoolean} 做跨类型规约。
 */
public final class VectorCasts {

    private VectorCasts() {}

    /** 把源向量的每一行值转成目标类型的新向量(不关闭 src,调用方负责)。 */
    public static FieldVector cast(
            ValueVector src,
            ColumnType target,
            int precision,
            int scale,
            BufferAllocator allocator) {
        int rows = src.getValueCount();
        FieldVector out = null;
        try {
            switch (target) {
                case SMALLINT:
                    {
                        SmallIntVector v = new SmallIntVector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, (short) asLong(src, i));
                            }
                        }
                        break;
                    }
                case INTEGER:
                    {
                        IntVector v = new IntVector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, (int) asLong(src, i));
                            }
                        }
                        break;
                    }
                case BIGINT:
                    {
                        BigIntVector v = new BigIntVector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, asLong(src, i));
                            }
                        }
                        break;
                    }
                case REAL:
                case FLOAT:
                    {
                        Float4Vector v = new Float4Vector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, (float) asDouble(src, i));
                            }
                        }
                        break;
                    }
                case DOUBLE:
                    {
                        Float8Vector v = new Float8Vector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, asDouble(src, i));
                            }
                        }
                        break;
                    }
                case DECIMAL:
                case NUMERIC:
                    {
                        // Calcite 为 avg(sum(decimal)) over(...) 等窗口 AVG 生成
                        // CAST(除法结果 : DECIMAL(原scale)),把高精度除法结果截断回原 scale。
                        // 此处当源 DECIMAL scale 高于目标 scale 时保留源 scale,避免精度丢失
                        // (该 CAST 是 Calcite 自动插入的类型对齐,非用户显式截断意图)。
                        int effectiveScale = scale;
                        int effectivePrecision = precision;
                        if (src instanceof DecimalVector srcDv
                                && srcDv.getScale() > effectiveScale) {
                            effectiveScale = srcDv.getScale();
                            effectivePrecision =
                                    Math.min(effectivePrecision + (effectiveScale - scale), 38);
                        }
                        DecimalVector v =
                                (DecimalVector)
                                        ArrowTypes.field(
                                                        new ColumnMeta(
                                                                "cast",
                                                                target,
                                                                effectivePrecision,
                                                                effectiveScale))
                                                .createVector(allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, Kernels.scaleTo(v, new BigDecimal(asString(src, i))));
                            }
                        }
                        break;
                    }
                case VARCHAR:
                case CHAR:
                case NCHAR:
                case NVARCHAR:
                    {
                        VarCharVector v = new VarCharVector("cast", allocator);
                        out = v;
                        v.allocateNew();
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, asString(src, i).getBytes(StandardCharsets.UTF_8));
                            }
                        }
                        break;
                    }
                case BOOLEAN:
                    {
                        BitVector v = new BitVector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, asBoolean(src, i) ? 1 : 0);
                            }
                        }
                        break;
                    }
                case DATE:
                    {
                        DateDayVector v = new DateDayVector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else if (src instanceof DateDayVector ddv) {
                                v.setSafe(i, ddv.get(i));
                            } else {
                                v.setSafe(i, new DateString(asString(src, i)).getDaysSinceEpoch());
                            }
                        }
                        break;
                    }
                case TIME:
                    {
                        TimeMilliVector v = new TimeMilliVector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, (int) asLong(src, i));
                            }
                        }
                        break;
                    }
                case TIMESTAMP:
                    {
                        TimeStampMilliVector v = new TimeStampMilliVector("cast", allocator);
                        out = v;
                        v.allocateNew(rows);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else if (src instanceof TimeStampMilliVector tsv) {
                                v.setSafe(i, tsv.get(i));
                            } else {
                                v.setSafe(
                                        i,
                                        new TimestampString(asString(src, i))
                                                .getMillisSinceEpoch());
                            }
                        }
                        break;
                    }
                case BINARY:
                case VARBINARY:
                    {
                        VarBinaryVector v = new VarBinaryVector("cast", allocator);
                        out = v;
                        v.allocateNew();
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                v.setNull(i);
                            } else {
                                v.setSafe(i, asString(src, i).getBytes(StandardCharsets.UTF_8));
                            }
                        }
                        break;
                    }
                default:
                    throw new UnsupportedOperationException("unsupported CAST target: " + target);
            }
            out.setValueCount(rows);
            return out;
        } catch (RuntimeException e) {
            // 转换中途失败(如 'abc'→INT)时释放已分配的 out,避免 Arrow 内存泄漏。
            if (out != null) {
                out.close();
            }
            throw e;
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
            return Boolean.toString(bv.get(i) == 1);
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
