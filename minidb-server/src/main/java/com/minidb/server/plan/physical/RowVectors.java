package com.minidb.server.plan.physical;

import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Row-oriented helpers for eager operators that normalize data to
 * {@code Object[]} (one element per column, null-safe) and rebuild Arrow
 * batches. Shared by MiniDbRepeatUnion and the transient-table read path in
 * MiniDbScan; the same conversions also exist privately in MiniDbJoin /
 * MiniDbUnion / WindowFunctions.
 */
public final class RowVectors {

    private RowVectors() {
    }

    /** Pulls every batch of {@code input} into normalized {@code Object[]} rows. */
    public static List<Object[]> materialize(RelNode input, ExecContext ctx) {
        List<Object[]> rows = new ArrayList<>();
        BatchIterator iterator = ((MiniDbRel) input).execute(ctx);
        try {
            while (iterator.hasNext()) {
                VectorSchemaRoot batch = iterator.next();
                for (int rowIdx = 0; rowIdx < batch.getRowCount(); rowIdx++) {
                    Object[] row = new Object[batch.getFieldVectors().size()];
                    for (int colIdx = 0; colIdx < row.length; colIdx++) {
                        row[colIdx] = readObject(batch.getVector(colIdx), rowIdx);
                    }
                    rows.add(row);
                }
            }
        } finally {
            iterator.close();
        }
        return rows;
    }

    /** Reads one cell as a boxed value (null for SQL NULL). */
    public static Object readObject(ValueVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        if (vector instanceof SmallIntVector sv) {
            return sv.get(row);
        }
        if (vector instanceof IntVector iv) {
            return iv.get(row);
        }
        if (vector instanceof BigIntVector bv) {
            return bv.get(row);
        }
        if (vector instanceof Float4Vector fv) {
            return fv.get(row);
        }
        if (vector instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (vector instanceof DecimalVector dv) {
            return dv.getObject(row);
        }
        if (vector instanceof VarCharVector vv) {
            return new String(vv.get(row), StandardCharsets.UTF_8);
        }
        if (vector instanceof BitVector bv) {
            return bv.get(row);
        }
        if (vector instanceof DateDayVector dv) {
            return dv.get(row);
        }
        if (vector instanceof TimeMilliVector tv) {
            return tv.get(row);
        }
        if (vector instanceof TimeStampMilliVector tv) {
            return tv.get(row);
        }
        if (vector instanceof VarBinaryVector bv) {
            return bv.get(row);
        }
        throw new UnsupportedOperationException(
                "cannot read column type: " + vector.getMinorType());
    }

    /** Writes one cell into a freshly allocated vector at {@code row}. */
    public static void writeObject(FieldVector vector, int row, Object value) {
        if (value == null) {
            vector.setNull(row);
            return;
        }
        if (vector instanceof SmallIntVector sv) {
            sv.setSafe(row, ((Number) value).shortValue());
        } else if (vector instanceof IntVector iv) {
            iv.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof BigIntVector bv) {
            bv.setSafe(row, ((Number) value).longValue());
        } else if (vector instanceof Float4Vector fv) {
            fv.setSafe(row, ((Number) value).floatValue());
        } else if (vector instanceof Float8Vector fv) {
            fv.setSafe(row, ((Number) value).doubleValue());
        } else if (vector instanceof DecimalVector dv) {
            dv.setSafe(row, ((BigDecimal) value).setScale(dv.getScale(), RoundingMode.HALF_UP));
        } else if (vector instanceof VarCharVector vv) {
            vv.setSafe(row, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof BitVector bv) {
            bv.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof DateDayVector dv) {
            dv.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof TimeMilliVector tv) {
            tv.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof TimeStampMilliVector tv) {
            tv.setSafe(row, ((Number) value).longValue());
        } else if (vector instanceof VarBinaryVector bv) {
            bv.setSafe(row, (byte[]) value);
        } else {
            throw new UnsupportedOperationException(
                    "cannot write value to " + vector.getMinorType());
        }
    }

    /** Builds a single batch whose columns follow {@code rowType}. */
    public static VectorSchemaRoot buildRoot(List<Object[]> rows,
                                             RelDataType rowType,
                                             BufferAllocator allocator) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            vectors.add(ArrowTypes.field(field).createVector(allocator));
        }
        for (FieldVector vector : vectors) {
            vector.setInitialCapacity(rows.size());
            vector.allocateNew();
        }
        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            Object[] row = rows.get(rowIdx);
            for (int colIdx = 0; colIdx < row.length; colIdx++) {
                writeObject(vectors.get(colIdx), rowIdx, row[colIdx]);
            }
        }
        for (FieldVector vector : vectors) {
            vector.setValueCount(rows.size());
        }
        // of() after setValueCount: the root's rowCount derives from the first
        // vector's valueCount.
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    /**
     * 把 RelNode 的输出物化成单个 VectorSchemaRoot(列式 copy,不装箱)。
     * 空输入返回 0 行 root(schema 从 rowType 推)。供 SetOp 等需要跨 batch
     * 稳定列式 key 的算子复用。
     */
    public static VectorSchemaRoot materializeToRoot(RelNode input, ExecContext ctx) {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        int total = 0;
        BatchIterator it = ((MiniDbRel) input).execute(ctx);
        VectorSchemaRoot merged = null;
        try {
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                batches.add(b);
                total += b.getRowCount();
            }
            if (batches.isEmpty()) {
                merged = emptyRoot(input.getRowType(), ctx.allocator());
            } else {
                merged = VectorSchemaRoot.create(batches.get(0).getSchema(), ctx.allocator());
                // 预分配 total,批量列拷贝(固定宽走无检查 copyFrom)的前提
                for (FieldVector v : merged.getFieldVectors()) {
                    v.setInitialCapacity(total);
                    v.allocateNew();
                }
                int dst = 0;
                for (VectorSchemaRoot batch : batches) {
                    RowCopier.copyRows(batch, 0, merged, dst, batch.getRowCount());
                    dst += batch.getRowCount();
                }
                merged.setRowCount(dst);
            }
            // 数据已 copy 到 merged。源 batches 的释放委托给 it.close()——迭代器知道
            // 自己输入的 batch 所有权(Scan 的 batch 归表所有、close 是 no-op;
            // Filter/Project 关 owned 队列),故不在此显式 close,避免误关表 owned batch。
            it.close();
            return merged;
        } catch (RuntimeException e) {
            // 异常路径:merged 可能已分配(allocateNew/copyRow OOM)或为 null。
            // 必须释放 merged 和迭代器,否则大数据量下中间结果泄漏撑爆 allocator
            // (TPC-DS 基准 query33 起 OOM 连锁即此路径未释放所致)。
            if (merged != null) {
                merged.close();
            }
            it.close();
            throw e;
        }
    }

    private static VectorSchemaRoot emptyRoot(RelDataType rowType, BufferAllocator allocator) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : rowType.getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(allocator));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(0);
            v.allocateNew();
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }
}
