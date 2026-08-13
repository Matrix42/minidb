package com.minidb.server.exec.functions;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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
