package com.minidb.server.exec.functions;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.sql.SqlKind;

public final class Kernels {
    private Kernels() {}

    public static void fillUnaryInt(IntVector in, IntVector out, ScalarKernels.IntUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryLong(BigIntVector in, BigIntVector out, ScalarKernels.LongUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryDouble(Float8Vector in, Float8Vector out, ScalarKernels.DoubleUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryString(VarCharVector in, VarCharVector out, ScalarKernels.StringUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(new String(in.get(i), StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8));
        }
    }
    public static void fillStringToInt(VarCharVector in, IntVector out, ScalarKernels.StringToInt op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(new String(in.get(i), StandardCharsets.UTF_8)));
        }
    }
    public static void fillUnaryShort(SmallIntVector in, SmallIntVector out, ScalarKernels.ShortUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryFloat(Float4Vector in, Float4Vector out, ScalarKernels.FloatUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryDecimal(DecimalVector in, DecimalVector out, ScalarKernels.DecimalUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, scaleTo(out, op.apply(in.getObject(i))));
        }
    }

    public static void fillBinaryInt(IntVector l, IntVector r, IntVector out, ScalarKernels.IntBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillBinaryLong(BigIntVector l, BigIntVector r, BigIntVector out, ScalarKernels.LongBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillBinaryDouble(Float8Vector l, Float8Vector r, Float8Vector out, ScalarKernels.DoubleBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillBinaryString(VarCharVector l, VarCharVector r, VarCharVector out, ScalarKernels.StringBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            String a = new String(l.get(i), StandardCharsets.UTF_8);
            String b = new String(r.get(i), StandardCharsets.UTF_8);
            out.setSafe(i, op.apply(a, b).getBytes(StandardCharsets.UTF_8));
        }
    }

    public static void fillCompareInt(IntVector l, IntVector r, BitVector out,
                                      ScalarKernels.IntCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareLong(BigIntVector l, BigIntVector r, BitVector out,
                                       ScalarKernels.LongCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareDouble(Float8Vector l, Float8Vector r, BitVector out,
                                         ScalarKernels.DoubleCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareString(VarCharVector l, VarCharVector r, BitVector out,
                                         ScalarKernels.StringCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            String a = new String(l.get(i), StandardCharsets.UTF_8);
            String b = new String(r.get(i), StandardCharsets.UTF_8);
            out.setSafe(i, compareToBool(cmp.apply(a, b), kind) ? 1 : 0);
        }
    }
    /** 跨型比较(INTEGER 列 vs BIGINT 列,Calcite 不强制 CAST):int 侧 promote 到 long。 */
    public static void fillCompareIntLong(IntVector l, BigIntVector r, BitVector out,
                                          ScalarKernels.LongCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply((long) l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareLongInt(BigIntVector l, IntVector r, BitVector out,
                                          ScalarKernels.LongCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), (long) r.get(i)), kind) ? 1 : 0);
        }
    }

    public static void fillBinaryShort(SmallIntVector l, SmallIntVector r, SmallIntVector out,
                                       ScalarKernels.ShortBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillCompareShort(SmallIntVector l, SmallIntVector r, BitVector out,
                                        ScalarKernels.ShortCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillBinaryFloat(Float4Vector l, Float4Vector r, Float4Vector out,
                                       ScalarKernels.FloatBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillCompareFloat(Float4Vector l, Float4Vector r, BitVector out,
                                        ScalarKernels.FloatCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillBinaryDecimal(DecimalVector l, DecimalVector r, DecimalVector out,
                                         ScalarKernels.DecimalBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, scaleTo(out, op.apply(l.getObject(i), r.getObject(i))));
        }
    }
    public static void fillCompareDecimal(DecimalVector l, DecimalVector r, BitVector out,
                                          ScalarKernels.DecimalCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.getObject(i), r.getObject(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareTime(TimeMilliVector l, TimeMilliVector r, BitVector out,
                                       ScalarKernels.IntCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareDate(DateDayVector l, DateDayVector r, BitVector out,
                                       ScalarKernels.IntCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareBytes(VarBinaryVector l, VarBinaryVector r, BitVector out,
                                        ScalarKernels.BytesCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }

    /** Arrow 要求写入 DecimalVector 的 BigDecimal 的 scale 与向量 scale 一致;统一缩放到目标 scale。 */
    public static BigDecimal scaleTo(DecimalVector out, BigDecimal value) {
        return value.setScale(out.getScale(), RoundingMode.HALF_UP);
    }

    /** 把 -1/0/1 比较结果按 SqlKind 转 bool。 */
    private static boolean compareToBool(int cmp, SqlKind kind) {
        return switch (kind) {
            case EQUALS -> cmp == 0;
            case NOT_EQUALS -> cmp != 0;
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUAL -> cmp <= 0;
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUAL -> cmp >= 0;
            default -> throw new IllegalArgumentException("not a comparison: " + kind);
        };
    }
}
