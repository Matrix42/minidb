package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableSpool;

/**
 * Recursive-CTE spool. The spooling (accumulating each iteration's rows into
 * the transient table) is driven by MiniDbRepeatUnion, which owns the working
 * table in ExecContext; the spool node itself just passes its input through.
 */
public class MiniDbTableSpool extends TableSpool implements MiniDbRel {

    public MiniDbTableSpool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                            Spool.Type readType, Spool.Type writeType, RelOptTable table) {
        super(cluster, traitSet, input, readType, writeType, table);
    }

    @Override
    protected Spool copy(RelTraitSet traitSet, RelNode input,
                         Spool.Type readType, Spool.Type writeType) {
        return new MiniDbTableSpool(getCluster(), traitSet, input, readType, writeType,
                table);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        return ((MiniDbRel) getInput()).execute(ctx);
    }
}
