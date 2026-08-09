package com.minidb.server.plan;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.storage.ArrowTable;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;

public class MiniDbModify extends TableModify implements MiniDbRel {

    private long affected;

    public MiniDbModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                        org.apache.calcite.prepare.Prepare.CatalogReader catalogReader,
                        RelNode input, Operation operation,
                        List<String> updateColumnList,
                        List<org.apache.calcite.rex.RexNode> sourceExpressionList,
                        boolean flattened) {
        super(cluster, traitSet, table, catalogReader, input, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbModify(getCluster(), traitSet, table, getCatalogReader(),
                sole(inputs), getOperation(), getUpdateColumnList(),
                getSourceExpressionList(), isFlattened());
    }

    public long affected() {
        return affected;
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<String> qualified = table.getQualifiedName();
        String tableName = qualified.get(qualified.size() - 1);
        ArrowTable target = ctx.storage().getTable(tableName);
        BatchIterator input = ((MiniDbRel) getInput()).execute(ctx);
        affected = 0;
        while (input.hasNext()) {
            VectorSchemaRoot batch = input.next();
            affected += batch.getRowCount();
            target.appendBatch(batch);
        }
        input.close();
        ctx.storage().markDirty(tableName);
        return new BatchIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public VectorSchemaRoot next() {
                throw new java.util.NoSuchElementException();
            }

            @Override
            public void close() {
            }
        };
    }
}
