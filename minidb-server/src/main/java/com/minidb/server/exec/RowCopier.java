package com.minidb.server.exec;

import com.minidb.server.exec.functions.Kernels;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
import org.apache.arrow.vector.util.Text;

public final class RowCopier {

    private RowCopier() {
    }

    public static FieldVector copyVector(FieldVector src, org.apache.arrow.memory.BufferAllocator allocator) {
        FieldVector dst = src.getField().createVector(allocator);
        dst.setInitialCapacity(src.getValueCount());
        dst.allocateNew();
        for (int i = 0; i < src.getValueCount(); i++) {
            dst.copyFromSafe(i, i, src);
        }
        dst.setValueCount(src.getValueCount());
        return dst;
    }

    public static void copyRow(FieldVector src, int srcRow, FieldVector dst, int dstRow) {
        if (src.getMinorType() == dst.getMinorType()) {
            // DecimalVector 的 copyFromSafe 要求 scale 精确一致,否则
            // DecimalUtility.checkPrecisionAndScale 抛异常。跨 scale 时走 writeValue
            // 的 scaleTo 转换。
            if (src instanceof DecimalVector && dst instanceof DecimalVector
                    && ((DecimalVector) src).getScale() != ((DecimalVector) dst).getScale()) {
                writeValue(dst, dstRow, src, srcRow);
                return;
            }
            dst.copyFromSafe(srcRow, dstRow, src);
        } else {
            writeValue(dst, dstRow, src, srcRow);
        }
    }

    public static void copyRow(VectorSchemaRoot src, int srcRow,
                               VectorSchemaRoot dst, int dstRow) {
        List<FieldVector> srcVectors = src.getFieldVectors();
        List<FieldVector> dstVectors = dst.getFieldVectors();
        for (int i = 0; i < srcVectors.size(); i++) {
            // 委托给 FieldVector 版本:它处理 DecimalVector 跨 scale 和跨类型复制,
            // 避免 scale=6 的值被 copyFromSafe 直接存到 scale=2 向量(放大 10^n)。
            copyRow(srcVectors.get(i), srcRow, dstVectors.get(i), dstRow);
        }
    }

    /**
     * Copy the value at {@code srcRow} from {@code src} into {@code dst} at
     * {@code dstRow}, coercing between compatible numeric/text types when the
     * source and destination vector types differ (e.g. BigIntVector literal
     * into an IntVector column).
     */
    public static void writeValue(FieldVector dst, int dstRow,
                                  ValueVector src, int srcRow) {
        if (src.isNull(srcRow)) {
            setNull(dst, dstRow);
            return;
        }
        if (dst instanceof SmallIntVector sv) {
            sv.setSafe(dstRow, (short) readLong(src, srcRow));
        } else if (dst instanceof IntVector iv) {
            iv.setSafe(dstRow, (int) readLong(src, srcRow));
        } else if (dst instanceof BigIntVector bv) {
            bv.setSafe(dstRow, readLong(src, srcRow));
        } else if (dst instanceof Float4Vector fv) {
            fv.setSafe(dstRow, (float) readDouble(src, srcRow));
        } else if (dst instanceof Float8Vector fv) {
            fv.setSafe(dstRow, readDouble(src, srcRow));
        } else if (dst instanceof DecimalVector dv) {
            dv.setSafe(dstRow, Kernels.scaleTo(dv, readDecimal(src, srcRow)));
        } else if (dst instanceof VarCharVector vv) {
            Object v = src.getObject(srcRow);
            byte[] bytes = v instanceof Text t ? t.copyBytes()
                    : v.toString().getBytes(StandardCharsets.UTF_8);
            vv.setSafe(dstRow, bytes);
        } else if (dst instanceof BitVector bv) {
            bv.setSafe(dstRow, (int) readLong(src, srcRow));
        } else if (dst instanceof TimeMilliVector tv) {
            tv.setSafe(dstRow, (int) readLong(src, srcRow));
        } else if (dst instanceof VarBinaryVector bv) {
            bv.setSafe(dstRow, (byte[]) src.getObject(srcRow));
        } else {
            // same-type vectors (Date, Timestamp, etc.): direct copy
            dst.copyFromSafe(srcRow, dstRow, src);
        }
    }

    private static void setNull(FieldVector dst, int row) {
        if (dst instanceof SmallIntVector sv) sv.setNull(row);
        else if (dst instanceof IntVector iv) iv.setNull(row);
        else if (dst instanceof BigIntVector bv) bv.setNull(row);
        else if (dst instanceof Float4Vector fv) fv.setNull(row);
        else if (dst instanceof Float8Vector fv) fv.setNull(row);
        else if (dst instanceof DecimalVector dv) dv.setNull(row);
        else if (dst instanceof VarCharVector vv) vv.setNull(row);
        else if (dst instanceof BitVector bv) bv.setNull(row);
        else if (dst instanceof DateDayVector dv) dv.setNull(row);
        else if (dst instanceof TimeMilliVector tv) tv.setNull(row);
        else if (dst instanceof TimeStampMilliVector tv) tv.setNull(row);
        else if (dst instanceof VarBinaryVector bv) bv.setNull(row);
        else throw new UnsupportedOperationException(
                "unsupported vector for null: " + dst.getClass());
    }

    private static long readLong(ValueVector v, int i) {
        if (v instanceof SmallIntVector sv) return sv.get(i);
        if (v instanceof IntVector iv) return iv.get(i);
        if (v instanceof BigIntVector bv) return bv.get(i);
        if (v instanceof Float4Vector fv) return (long) fv.get(i);
        if (v instanceof Float8Vector fv) return (long) fv.get(i);
        if (v instanceof DecimalVector dv) return dv.getObject(i).longValue();
        if (v instanceof BitVector bv) return bv.get(i);
        throw new IllegalArgumentException(
                "not a numeric vector: " + v.getClass());
    }

    private static double readDouble(ValueVector v, int i) {
        if (v instanceof SmallIntVector sv) return sv.get(i);
        if (v instanceof IntVector iv) return iv.get(i);
        if (v instanceof BigIntVector bv) return bv.get(i);
        if (v instanceof Float4Vector fv) return fv.get(i);
        if (v instanceof Float8Vector fv) return fv.get(i);
        if (v instanceof DecimalVector dv) return dv.getObject(i).doubleValue();
        throw new IllegalArgumentException(
                "not a numeric vector: " + v.getClass());
    }

    private static BigDecimal readDecimal(ValueVector v, int i) {
        if (v instanceof DecimalVector dv) return dv.getObject(i);
        if (v instanceof SmallIntVector sv) return BigDecimal.valueOf(sv.get(i));
        if (v instanceof IntVector iv) return BigDecimal.valueOf(iv.get(i));
        if (v instanceof BigIntVector bv) return BigDecimal.valueOf(bv.get(i));
        if (v instanceof Float4Vector fv) return BigDecimal.valueOf(fv.get(i));
        if (v instanceof Float8Vector fv) return BigDecimal.valueOf(fv.get(i));
        throw new IllegalArgumentException(
                "not a numeric vector: " + v.getClass());
    }
}
