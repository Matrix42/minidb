package com.minidb.server.exec;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import java.util.Arrays;

/**
 * 列式比较/hash:直接对 {@link ValueVector} 的原始值操作,不装箱。
 *
 * <p>VarChar/VarBinary 用字节比较({@code Arrays.compareUnsigned})而非 {@code new String}: UTF-8
 * 编码保序,字节字典序与 code point 序一致,故与 {@code String.compareTo} 语义等价, 但避免了每比较一次就分配两个 String 对象的装箱开销。
 *
 * <p>Decimal 仍走 {@code getObject}(装箱 BigDecimal)——其底层 little-endian 字节比较需处理 符号位且 hash/compare
 * 语义(scale 敏感 vs 不敏感)与既有行为易漂移,正确性优先、暂不列式。
 *
 * <p>本类方法都假设传入的行非 null;null 语义由调用方处理。
 */
public final class ValueComparators {

    private ValueComparators() {}

    /** 列式比较两行同一逻辑列的值(非 null)。返回 -1/0/1。 */
    public static int compare(ValueVector a, int rowA, ValueVector b, int rowB) {
        if (a instanceof SmallIntVector sv) {
            return Short.compare(sv.get(rowA), ((SmallIntVector) b).get(rowB));
        }
        if (a instanceof IntVector iv) {
            return Integer.compare(iv.get(rowA), ((IntVector) b).get(rowB));
        }
        if (a instanceof BigIntVector bv) {
            return Long.compare(bv.get(rowA), ((BigIntVector) b).get(rowB));
        }
        if (a instanceof Float4Vector fv) {
            return Float.compare(fv.get(rowA), ((Float4Vector) b).get(rowB));
        }
        if (a instanceof Float8Vector fv) {
            return Double.compare(fv.get(rowA), ((Float8Vector) b).get(rowB));
        }
        if (a instanceof VarCharVector vv) {
            return Arrays.compareUnsigned(vv.get(rowA), ((VarCharVector) b).get(rowB));
        }
        if (a instanceof VarBinaryVector bv) {
            return Arrays.compareUnsigned(bv.get(rowA), ((VarBinaryVector) b).get(rowB));
        }
        if (a instanceof BitVector bv) {
            return Integer.compare(bv.get(rowA), ((BitVector) b).get(rowB));
        }
        if (a instanceof DateDayVector dv) {
            return Integer.compare(dv.get(rowA), ((DateDayVector) b).get(rowB));
        }
        if (a instanceof TimeMilliVector tv) {
            return Integer.compare(tv.get(rowA), ((TimeMilliVector) b).get(rowB));
        }
        if (a instanceof TimeStampMilliVector tv) {
            return Long.compare(tv.get(rowA), ((TimeStampMilliVector) b).get(rowB));
        }
        if (a instanceof DecimalVector dv) {
            return dv.getObject(rowA).compareTo(((DecimalVector) b).getObject(rowB));
        }
        throw new UnsupportedOperationException("cannot compare column type: " + a.getMinorType());
    }

    /** 列式 hash(非 null)。与 {@link #compare} 一致:compare==0 的两行 hash 必相等。 */
    public static int hash(ValueVector v, int row) {
        if (v instanceof SmallIntVector sv) {
            return Integer.hashCode(sv.get(row));
        }
        if (v instanceof IntVector iv) {
            return Integer.hashCode(iv.get(row));
        }
        if (v instanceof BigIntVector bv) {
            return Long.hashCode(bv.get(row));
        }
        if (v instanceof Float4Vector fv) {
            return Float.hashCode(fv.get(row));
        }
        if (v instanceof Float8Vector fv) {
            return Double.hashCode(fv.get(row));
        }
        if (v instanceof VarCharVector vv) {
            return Arrays.hashCode(vv.get(row));
        }
        if (v instanceof VarBinaryVector bv) {
            return Arrays.hashCode(bv.get(row));
        }
        if (v instanceof BitVector bv) {
            return Integer.hashCode(bv.get(row));
        }
        if (v instanceof DateDayVector dv) {
            return Integer.hashCode(dv.get(row));
        }
        if (v instanceof TimeMilliVector tv) {
            return Integer.hashCode(tv.get(row));
        }
        if (v instanceof TimeStampMilliVector tv) {
            return Long.hashCode(tv.get(row));
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(row).hashCode();
        }
        throw new UnsupportedOperationException("cannot hash column type: " + v.getMinorType());
    }
}
