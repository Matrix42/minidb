package com.minidb.server.plan;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
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
        String tableName = qualified.get(qualified.size() - 1);
        ArrowTable arrowTable = ctx.storage().getTable(tableName);
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
}
