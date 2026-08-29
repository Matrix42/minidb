package com.minidb.server.plan.physical;

import com.minidb.server.exec.ValueComparators;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 列式行键:对 {@code root} 的 key 列在 {@code row} 行的值做 hash/equals,不装箱。
 *
 * <p>hash 只依赖 key 值(与列位置无关),故 build 侧与 probe 侧即使 key 列位置不同 (如 leftKeys vs rightKeys)也能相等;equals
 * 逐对比较 key 列的值。null-safe:null 与 null 相等、null 与非 null 不等,故既可用于 join(调用方已剔除 null 键)也可用于 窗口函数分区(null
 * 归入同一分区)。
 */
final class ColumnKey {
    private final VectorSchemaRoot root;
    private final int row;
    private final int[] cols;
    private final int hash;

    ColumnKey(VectorSchemaRoot root, int row, int[] cols) {
        this.root = root;
        this.row = row;
        this.cols = cols;
        int h = 1;
        for (int c : cols) {
            ValueVector v = root.getVector(c);
            h = 31 * h + (v.isNull(row) ? 0 : ValueComparators.hash(v, row));
        }
        this.hash = h;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    /** The root this key refers to (for output copy). */
    VectorSchemaRoot root() {
        return root;
    }

    /** The row within {@link #root()} this key refers to. */
    int row() {
        return row;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ColumnKey other)) {
            return false;
        }
        // 列数不同必不相等:否则下面按本对象 cols 长度迭代会越界或漏比较。
        if (cols.length != other.cols.length) {
            return false;
        }
        for (int k = 0; k < cols.length; k++) {
            ValueVector lv = root.getVector(cols[k]);
            ValueVector rv = other.root.getVector(other.cols[k]);
            boolean leftNull = lv.isNull(row);
            boolean rightNull = rv.isNull(other.row);
            if (leftNull || rightNull) {
                if (leftNull && rightNull) {
                    continue;
                }
                return false;
            }
            if (ValueComparators.compare(lv, row, rv, other.row) != 0) {
                return false;
            }
        }
        return true;
    }
}
