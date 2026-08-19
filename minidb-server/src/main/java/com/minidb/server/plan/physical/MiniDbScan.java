package com.minidb.server.plan.physical;

import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.InformationSchema;
import com.minidb.storage.common.TableHandle;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

public class MiniDbScan extends TableScan implements MiniDbRel {

    public MiniDbScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, List.of(), table);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbScan(getCluster(), traitSet, table);
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
                return transientScan(transientRows, ctx);
            }
        }
        TableHandle tableHandle;
        if (n >= 3) {
            String schemaName = qualified.get(n - 2);
            String tableName = qualified.get(n - 1);
            if (InformationSchemaCatalog.isSystemSchema(schemaName)) {
                return singleBatch(InformationSchema.materialize(
                        ctx.storage().catalog(), tableName, ctx.allocator()));
            }
            // qualified name like [minidb, other, t] — schema is second-to-last
            tableHandle = ctx.getTable(schemaName, tableName);
        } else {
            // promoted table like [minidb, t] — resolve via current schema
            tableHandle = ctx.getTable(qualified.get(n - 1));
        }
        return tableHandle.scan();
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
