package com.minidb.server.plan.physical;

import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.InformationSchema;
import com.minidb.server.exec.RowCopier;
import com.minidb.storage.common.TableHandle;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

public class MiniDbScan extends TableScan implements MiniDbRel {

    /** 下推的列裁剪:null→全列;非null→只读这些列索引(0-based)。 */
    private final int[] projectedColumns;
    /** 下推的谓词:null→不过滤;非null→扫描时 eval 并过滤。 */
    private final RexNode pushedFilter;

    public MiniDbScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, table, null, null);
    }

    public MiniDbScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                      int[] projectedColumns, RexNode pushedFilter) {
        super(cluster, traitSet, List.of(), table);
        this.projectedColumns = projectedColumns;
        this.pushedFilter = pushedFilter;
    }

    /** 是否有下推优化(列裁剪 或 谓词下推)。 */
    public boolean hasPushdown() {
        return projectedColumns != null || pushedFilter != null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (pushedFilter != null) {
            pw.item("filter", pushedFilter);
        }
        if (projectedColumns != null) {
            pw.item("cols", java.util.Arrays.toString(projectedColumns));
        }
        return pw;
    }

    public int[] projectedColumns() {
        return projectedColumns;
    }

    public RexNode pushedFilter() {
        return pushedFilter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbScan(getCluster(), traitSet, table, projectedColumns, pushedFilter);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<String> qualified = table.getQualifiedName();
        int n = qualified.size();
        if (n == 1) {
            // A single-segment name is a recursive CTE transient table (real
            // tables come through as [minidb, t] or [minidb, schema, t]).
            List<Object[]> transientRows = ctx.transientTable(qualified.get(0));
            if (transientRows != null) {
                return applyPushdown(transientScan(transientRows, ctx), ctx);
            }
        }
        TableHandle tableHandle;
        if (n >= 3) {
            String schemaName = qualified.get(n - 2);
            String tableName = qualified.get(n - 1);
            if (InformationSchemaCatalog.isSystemSchema(schemaName)) {
                return applyPushdown(
                        singleBatch(InformationSchema.materialize(
                                ctx.storage().catalog(), tableName, ctx.allocator())),
                        ctx);
            }
            // qualified name like [minidb, other, t] — schema is second-to-last
            tableHandle = ctx.getTable(schemaName, tableName);
        } else {
            // promoted table like [minidb, t] — resolve via current schema
            tableHandle = ctx.getTable(qualified.get(n - 1));
        }
        return applyPushdown(
                    projectedColumns != null && pushedFilter == null
                            && projectedColumns.length == tableHandle.schema().columns().size()
                            ? tableHandle.scan(projectedColumns)
                            : tableHandle.scan(),
                    ctx);
    }

    /**
     * 对底层扫描迭代器套上列裁剪和/或谓词过滤。
     * 无下推时直接返回原迭代器。
     */
    private BatchIterator applyPushdown(BatchIterator source, ExecContext ctx) {
        if (projectedColumns == null && pushedFilter == null) {
            return source;
        }
        // 存储层已做列裁剪(source 只含投影列),只做谓词过滤,跳过 applyProject
        boolean sourceAlreadyProjected = projectedColumns != null && pushedFilter == null;
        Deque<VectorSchemaRoot> owned = new ArrayDeque<>();
        return BatchIterator.interruptible(new BatchIterator() {
            VectorSchemaRoot pending;

            @Override
            public boolean hasNext() {
                while (pending == null && source.hasNext()) {
                    VectorSchemaRoot batch = source.next();
                    VectorSchemaRoot filtered = null;
                    try {
                        filtered = applyFilter(batch, ctx);
                        if (filtered == null) {
                            continue; // 全批被过滤
                        }
                        VectorSchemaRoot result = sourceAlreadyProjected
                                ? filtered
                                : applyProject(filtered, ctx);
                        if (result != filtered) {
                            filtered.close();
                        }
                        owned.add(result);
                        pending = result;
                    } catch (RuntimeException e) {
                        // applyProject/applyFilter 失败时释放中间结果
                        if (filtered != null && filtered != pending) {
                            filtered.close();
                        }
                        throw e;
                    }
                }
                return pending != null;
            }

            @Override
            public VectorSchemaRoot next() {
                VectorSchemaRoot out = pending;
                pending = null;
                return out;
            }

            @Override
            public void close() {
                source.close();
                for (VectorSchemaRoot r : owned) {
                    r.close();
                }
                owned.clear();
            }
        });
    }

    /** 谓词过滤:null→原样返回;全匹配→原样返回;无匹配→null(释放原批)。 */
    private VectorSchemaRoot applyFilter(VectorSchemaRoot batch, ExecContext ctx) {
        if (pushedFilter == null) {
            return batch;
        }
        ValueVector condition = ctx.interpreter().eval(pushedFilter, batch);
        try {
            int kept = 0;
            for (int i = 0; i < batch.getRowCount(); i++) {
                if (!condition.isNull(i) && ((BitVector) condition).get(i) == 1) {
                    kept++;
                }
            }
            if (kept == 0) {
                return null;
            }
            if (kept == batch.getRowCount()) {
                return batch;
            }
            VectorSchemaRoot out = VectorSchemaRoot.create(batch.getSchema(), ctx.allocator());
            out.allocateNew();
            int dst = 0;
            for (int i = 0; i < batch.getRowCount(); i++) {
                if (!condition.isNull(i) && ((BitVector) condition).get(i) == 1) {
                    RowCopier.copyRow(batch, i, out, dst++);
                }
            }
            out.setRowCount(kept);
            return out;
        } finally {
            condition.close();
        }
    }

    /** 列裁剪:null→原样返回;全列→原样返回;否则拷出只含投影列的新批。 */
    private VectorSchemaRoot applyProject(VectorSchemaRoot batch, ExecContext ctx) {
        if (projectedColumns == null) {
            return batch;
        }
        List<FieldVector> outVectors = new java.util.ArrayList<>(projectedColumns.length);
        for (int col : projectedColumns) {
            outVectors.add(batch.getVector(col).getField().createVector(ctx.allocator()));
        }
        int rows = batch.getRowCount();
        for (FieldVector v : outVectors) {
            v.setInitialCapacity(rows);
            v.allocateNew();
        }
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < projectedColumns.length; c++) {
                RowCopier.copyRow(batch.getVector(projectedColumns[c]), r,
                        outVectors.get(c), r);
            }
        }
        for (FieldVector v : outVectors) {
            v.setValueCount(rows);
        }
        return VectorSchemaRoot.of(outVectors.toArray(new FieldVector[0]));
    }

    /**
     * 解析真实表(非瞬态/系统表)的 {@link TableHandle};瞬态表(单段名,递归 CTE)与
     * information_schema 系统表没有对应存储表,返回 null。供 COUNT(*) 短路直接读
     * {@code rowCount()} 而不扫描数据。
     */
    public TableHandle resolveTable(ExecContext ctx) {
        List<String> qualified = table.getQualifiedName();
        int n = qualified.size();
        if (n == 1) {
            return null;
        }
        if (n >= 3) {
            String schemaName = qualified.get(n - 2);
            if (InformationSchemaCatalog.isSystemSchema(schemaName)) {
                return null;
            }
            return ctx.getTable(schemaName, qualified.get(n - 1));
        }
        return ctx.getTable(qualified.get(n - 1));
    }

    private BatchIterator transientScan(List<Object[]> rows, ExecContext ctx) {
        VectorSchemaRoot root =
                RowVectors.buildRoot(rows, table.getRowType(), ctx.allocator());
        return singleBatch(root);
    }

    private BatchIterator singleBatch(VectorSchemaRoot root) {
        boolean[] done = {false};
        return BatchIterator.interruptible(new BatchIterator() {
            @Override
            public boolean hasNext() {
                return !done[0];
            }

            @Override
            public VectorSchemaRoot next() {
                done[0] = true;
                return root;
            }

            @Override
            public void close() {
                root.close();
            }
        });
    }
}
