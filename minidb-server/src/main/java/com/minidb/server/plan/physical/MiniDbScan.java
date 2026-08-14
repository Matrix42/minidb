package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.InformationSchema;
import com.minidb.server.storage.ArrowTable;
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
        ArrowTable arrowTable;
        if (n >= 3) {
            String schemaName = qualified.get(n - 2);
            String tableName = qualified.get(n - 1);
            if (InformationSchema.isSystemSchema(schemaName)) {
                return singleBatch(InformationSchema.materialize(
                        ctx.storage().catalog(), tableName, ctx.allocator()));
            }
            // qualified name like [minidb, other, t] — schema is second-to-last
            arrowTable = ctx.getTable(schemaName, tableName);
        } else {
            // promoted table like [minidb, t] — resolve via current schema
            arrowTable = ctx.getTable(qualified.get(n - 1));
        }
        Iterator<VectorSchemaRoot> it = arrowTable.batches().iterator();
        return new BatchIterator() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public VectorSchemaRoot next() {
                return it.next();
            }

            @Override
            public void close() {
                // batches are owned by the table
            }
        };
    }

    private BatchIterator transientScan(List<Object[]> rows, ExecContext ctx) {
        VectorSchemaRoot root =
                RowVectors.buildRoot(rows, table.getRowType(), ctx.allocator());
        return singleBatch(root);
    }

    private BatchIterator singleBatch(VectorSchemaRoot root) {
        boolean[] done = {false};
        return new BatchIterator() {
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
        };
    }
}
