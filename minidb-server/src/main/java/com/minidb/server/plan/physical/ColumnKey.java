package com.minidb.server.plan.physical;

import com.minidb.server.exec.ValueComparators;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 列式行键:对 {@code root} 的 key 列在 {@code row} 行的值做 hash/equals,不装箱。
 *
 * <p>用于 join 的 hash 表。hash 只依赖 key 值(与列位置无关),故 build 侧与 probe 侧
 * 即使 key 列位置不同(如 leftKeys vs rightKeys)也能相等;equals 逐对比较 key 列的值。
 * 前提:调用方保证非 null(join 的 null 键永不匹配,已在调用前剔除)。
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
            h = 31 * h + ValueComparators.hash(root.getVector(c), row);
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
        for (int k = 0; k < cols.length; k++) {
            if (ValueComparators.compare(root.getVector(cols[k]), row,
                    other.root.getVector(other.cols[k]), other.row) != 0) {
                return false;
            }
        }
        return true;
    }
}
