package com.minidb.server.plan.physical;

import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.exec.ValueComparators;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

/**
 * Join base class. Subclasses implement one strategy (MiniDbHashJoin,
 * MiniDbSortMergeJoin, MiniDbNestedLoopJoin); this class owns columnar
 * materialization of both inputs (into a single {@link VectorSchemaRoot} each,
 * no per-cell boxing), streaming output (pairs are produced lazily and the
 * output is emitted in batches — memory O(batch size) instead of O(result)),
 * and output building. Join strategies work on row indices and columnar keys,
 * never on Object[].
 */
public abstract class MiniDbJoin extends Join implements MiniDbRel {

    /** 输出批次行数:与存储层 MAX_BATCH_ROWS 对齐,流式产批控制内存峰值。 */
    private static final int OUTPUT_BATCH = 4096;

    protected MiniDbJoin(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode left, RelNode right, RexNode condition,
                         JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, Set.of(), joinType);
    }

    @Override
    public final BatchIterator execute(ExecContext ctx) {
        JoinRelType type = getJoinType();
        if (type == JoinRelType.SEMI || type == JoinRelType.ANTI) {
            throw new UnsupportedOperationException("semi/anti join not supported");
        }
        VectorSchemaRoot left = materializeColumns(getLeft(), ctx);
        VectorSchemaRoot right = materializeColumns(getRight(), ctx);
        try {
            JoinInfo info = analyzeCondition();
            PairSource pairs = joinPairs(left, right, info, type, ctx);
            return new StreamingIterator(left, right, pairs, ctx);
        } catch (RuntimeException e) {
            left.close();
            right.close();
            throw e;
        }
    }

    /**
     * 流式行对源:join 策略按需产出 {@code {leftIdx, rightIdx}} 行对
     * (rightIdx = -1 表示左侧 null-pad,leftIdx = -1 表示右侧 null-pad)。
     * 替代原来的全量 {@code List<int[]>} 物化——大结果集内存从 O(输出行数) 降到
     * O(批大小),outer join 的 null-pad 行在两阶段产出(probe 完才知未匹配)。
     */
    protected interface PairSource {
        /**
         * 向 {@code leftRows/rightRows} 的 {@code [outPos, outPos+len)} 填行对;
         * 返回实际填充的终点下标(小于 outPos+len 即源耗尽)。
         */
        int fill(int[] leftRows, int[] rightRows, int outPos, int len);

        /** 释放源持有的资源(如逐对求值的 probe root);迭代器 close 时调用。 */
        default void close() {
        }
    }

    /** 拉模式多批迭代器:每批从 PairSource 填 OUTPUT_BATCH 行对,建一个输出 root。 */
    private final class StreamingIterator implements BatchIterator {
        private final VectorSchemaRoot left;
        private final VectorSchemaRoot right;
        private final PairSource pairs;
        private final ExecContext ctx;
        private final int[] leftRows = new int[OUTPUT_BATCH];
        private final int[] rightRows = new int[OUTPUT_BATCH];
        private VectorSchemaRoot current;
        private boolean exhausted;
        // 已通过 next() 返回的 batch。调用方不负责 close(所有权模型与
        // materializeColumns 一致:靠迭代器 close 统一释放),故累积在此,close 时全关。
        private final List<VectorSchemaRoot> emitted = new ArrayList<>();

        StreamingIterator(VectorSchemaRoot left, VectorSchemaRoot right,
                          PairSource pairs, ExecContext ctx) {
            this.left = left;
            this.right = right;
            this.pairs = pairs;
            this.ctx = ctx;
        }

        @Override
        public boolean hasNext() {
            if (current != null) {
                return true;
            }
            if (exhausted) {
                return false;
            }
            int n = pairs.fill(leftRows, rightRows, 0, OUTPUT_BATCH);
            if (n == 0) {
                exhausted = true;
                return false;
            }
            current = buildOutput(left, right, leftRows, rightRows, n, ctx);
            return true;
        }

        @Override
        public VectorSchemaRoot next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            VectorSchemaRoot out = current;
            current = null;
            emitted.add(out);
            return out;
        }

        @Override
        public void close() {
            if (current != null) {
                current.close();
                current = null;
            }
            for (VectorSchemaRoot b : emitted) {
                b.close();
            }
            emitted.clear();
            pairs.close();
            left.close();
            right.close();
        }
    }

    /**
     * Strategy-specific join. Returns a lazy pair source; strategies own their
     * internal cursors and outer-join phases, and must not materialize the
     * whole result.
     */
    protected abstract PairSource joinPairs(VectorSchemaRoot left, VectorSchemaRoot right,
                                            JoinInfo info, JoinRelType type, ExecContext ctx);

    /** Column count of the left input (from its row type, not the data). */
    protected final int leftColumnCount() {
        return getLeft().getRowType().getFieldCount();
    }

    /** Column count of the right input; see {@link #leftColumnCount()}. */
    protected final int rightColumnCount() {
        return getRight().getRowType().getFieldCount();
    }

    /** Pulls every batch of {@code input} into a single owned root, no per-cell boxing. */
    private VectorSchemaRoot materializeColumns(RelNode input, ExecContext ctx) {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        int total = 0;
        BatchIterator it = ((MiniDbRel) input).execute(ctx);
        VectorSchemaRoot merged = null;
        try {
            while (it.hasNext()) {
                ExecContext.checkInterrupted();
                VectorSchemaRoot b = it.next();
                batches.add(b);
                total += b.getRowCount();
            }
            if (batches.isEmpty()) {
                merged = emptyRoot(input, ctx);
            } else {
                merged = VectorSchemaRoot.create(batches.get(0).getSchema(), ctx.allocator());
                // 预分配: allocateNew 可能分配不足,显式按 total 分配并设 valueCount
                for (FieldVector v : merged.getFieldVectors()) {
                    v.setInitialCapacity(total);
                    v.allocateNew();
                    v.setValueCount(total);
                }
                int dst = 0;
                for (VectorSchemaRoot batch : batches) {
                    // 确保源 batch 的向量已正确分配(防御 copyFromSafe 读有效性缓冲区)
                    for (FieldVector sv : batch.getFieldVectors()) {
                        if (sv.getValueCount() == 0 && batch.getRowCount() > 0) {
                            sv.setValueCount(batch.getRowCount());
                        }
                    }
                    // 批量列拷贝(固定宽走无检查 copyFrom,merged 已预分配 total)
                    RowCopier.copyRows(batch, 0, merged, dst, batch.getRowCount());
                    dst += batch.getRowCount();
                }
                merged.setRowCount(dst);
            }
            // 源 batches 的释放委托给 it.close(迭代器按所有权决定:Scan no-op、
            // Filter/Project 关 owned)。不在此显式 close batches,避免误关表 owned batch。
            it.close();
            return merged;
        } catch (RuntimeException e) {
            // 异常路径必须释放 merged 和迭代器,否则大数据量下 join 物化两侧时
            // 中间结果泄漏撑爆 allocator(TPC-DS 基准 OOM 连锁)。
            if (merged != null) {
                merged.close();
            }
            it.close();
            throw e;
        }
    }

    private static VectorSchemaRoot emptyRoot(RelNode input, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        for (RelDataTypeField f : input.getRowType().getFieldList()) {
            vectors.add(ArrowTypes.field(f).createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(0);
            v.allocateNew();
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    /** Writes join output pairs into a columnar root (null side = setNull). */
    private VectorSchemaRoot buildOutput(VectorSchemaRoot left, VectorSchemaRoot right,
                                         int[] leftRows, int[] rightRows, int n, ExecContext ctx) {
        List<FieldVector> vectors = new ArrayList<>();
        // 用实际物化向量 schema(而非 plan rowType):子节点(Aggregate)可能在 buildOutput
        // 里提升了 DECIMAL scale,plan rowType 仍是原 scale,会导致输出截断。
        for (FieldVector v : left.getFieldVectors()) {
            vectors.add(v.getField().createVector(ctx.allocator()));
        }
        for (FieldVector v : right.getFieldVectors()) {
            vectors.add(v.getField().createVector(ctx.allocator()));
        }
        int total = n;
        for (FieldVector v : vectors) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }
        int leftCols = leftColumnCount();
        int rightCols = rightColumnCount();
        for (int c = 0; c < leftCols; c++) {
            RowCopier.copyRowsByIndex(left.getVector(c), leftRows, 0, vectors.get(c), 0, total);
        }
        for (int c = 0; c < rightCols; c++) {
            RowCopier.copyRowsByIndex(right.getVector(c), rightRows, 0,
                    vectors.get(leftCols + c), 0, total);
        }
        for (FieldVector v : vectors) {
            v.setValueCount(total);
        }
        // of() after setValueCount: rowCount derives from first vector's valueCount.
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    /** True if any of the join-key columns in {@code row} is null. */
    protected static boolean hasNullKey(VectorSchemaRoot root, int row, List<Integer> keyCols) {
        for (int colIdx : keyCols) {
            if (root.getVector(colIdx).isNull(row)) {
                return true;
            }
        }
        return false;
    }

    /** 建一个 1 行 probe root,列结构 = join 输出的行类型(左列 + 右列),供评估残留条件。 */
    protected final VectorSchemaRoot buildProbeRoot(ExecContext ctx, VectorSchemaRoot left,
                                                        VectorSchemaRoot right) {
        List<FieldVector> vectors = new ArrayList<>();
        // 用实际物化向量 schema(而非 plan rowType)建 probeRoot:子节点(Aggregate)可能
        // 在 buildOutput 里提升了 DECIMAL scale,plan rowType 仍是原 scale,若用 plan
        // rowType 建 probeRoot,copyRow 会把高 scale 值截断回低 scale。
        for (FieldVector v : left.getFieldVectors()) {
            vectors.add(v.getField().createVector(ctx.allocator()));
        }
        for (FieldVector v : right.getFieldVectors()) {
            vectors.add(v.getField().createVector(ctx.allocator()));
        }
        for (FieldVector v : vectors) {
            v.setInitialCapacity(1);
            v.allocateNew();
        }
        return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
    }

    /** 把 left/right 的某行拷进 1 行 probe root(左列在前、右列在后)。 */
    protected static void writeProbeRow(VectorSchemaRoot probeRoot, VectorSchemaRoot left,
                                        int leftIdx, VectorSchemaRoot right, int rightIdx) {
        List<FieldVector> vectors = probeRoot.getFieldVectors();
        for (int c = 0; c < left.getFieldVectors().size(); c++) {
            RowCopier.copyRow(left.getVector(c), leftIdx, vectors.get(c), 0);
        }
        for (int c = 0; c < right.getFieldVectors().size(); c++) {
            RowCopier.copyRow(right.getVector(c), rightIdx,
                    vectors.get(left.getFieldVectors().size() + c), 0);
        }
        probeRoot.setRowCount(1);
    }

    /** Row indices of {@code root} ordered by the key columns, nulls last. */
    protected static List<Integer> sortedIndices(VectorSchemaRoot root, List<Integer> keyCols) {
        List<Integer> order = new ArrayList<>(root.getRowCount());
        for (int rowIdx = 0; rowIdx < root.getRowCount(); rowIdx++) {
            order.add(rowIdx);
        }
        order.sort(Comparator.comparingInt((Integer rowIdx) -> nullKeyFlag(root, rowIdx, keyCols))
                .thenComparing((Integer a, Integer b) ->
                        compareKeys(root, a, keyCols, root, b, keyCols)));
        return order;
    }

    /** 1 when the row has a null key, 0 otherwise — lets null-keyed rows sort last. */
    protected static int nullKeyFlag(VectorSchemaRoot root, int row, List<Integer> keyCols) {
        return hasNullKey(root, row, keyCols) ? 1 : 0;
    }

    protected static int compareKeys(VectorSchemaRoot left, int leftRow, List<Integer> leftKeyCols,
                                     VectorSchemaRoot right, int rightRow, List<Integer> rightKeyCols) {
        for (int k = 0; k < leftKeyCols.size(); k++) {
            ValueVector lv = left.getVector(leftKeyCols.get(k));
            ValueVector rv = right.getVector(rightKeyCols.get(k));
            boolean leftNull = lv.isNull(leftRow);
            boolean rightNull = rv.isNull(rightRow);
            if (leftNull || rightNull) {
                if (leftNull && rightNull) {
                    continue;
                }
                return leftNull ? 1 : -1;
            }
            int cmp = ValueComparators.compare(lv, leftRow, rv, rightRow);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    protected static List<Integer> identity(int n) {
        List<Integer> order = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            order.add(i);
        }
        return order;
    }

    protected static int[] toIntArray(List<Integer> cols) {
        int[] arr = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            arr[i] = cols.get(i);
        }
        return arr;
    }

    /** True if any collation covers {@code keys} as an ascending prefix.
     *  Null collations (e.g. a table with no declared ordering) are treated
     *  as covering nothing. */
    public static boolean coversKeys(List<RelCollation> collations, List<Integer> keys) {
        if (collations == null || keys == null) {
            return false;
        }
        for (RelCollation collation : collations) {
            List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
            if (fieldCollations.size() < keys.size()) {
                continue;
            }
            boolean covers = true;
            for (int i = 0; i < keys.size(); i++) {
                RelFieldCollation fieldCollation = fieldCollations.get(i);
                RelFieldCollation.Direction direction = fieldCollation.getDirection();
                if (fieldCollation.getFieldIndex() != keys.get(i)
                        || (direction != RelFieldCollation.Direction.ASCENDING
                            && direction != RelFieldCollation.Direction.STRICTLY_ASCENDING)) {
                    covers = false;
                    break;
                }
            }
            if (covers) {
                return true;
            }
        }
        return false;
    }
}
