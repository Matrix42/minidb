package com.minidb.server.exec;

import com.minidb.server.plan.MiniDbFilter;
import com.minidb.server.plan.MiniDbModify;
import com.minidb.server.plan.MiniDbProject;
import com.minidb.server.plan.MiniDbRel;
import com.minidb.server.plan.MiniDbScan;
import com.minidb.server.plan.MiniDbSort;
import com.minidb.server.plan.MiniDbValues;
import com.minidb.server.plan.Planner;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.Histogram;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.stats.TableStats;
import com.minidb.server.storage.ArrowTable;
import com.minidb.server.storage.StorageManager;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class ExplainExecutor {

    private final Planner planner;
    private final StatsManager stats;
    private final StorageManager storage;
    private final BufferAllocator allocator;

    public ExplainExecutor(Planner planner, StatsManager stats,
                           StorageManager storage, BufferAllocator allocator) {
        this.planner = planner;
        this.stats = stats;
        this.storage = storage;
        this.allocator = allocator;
    }

    public QueryResult.Rows explain(String innerSql) {
        return explain(innerSql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public QueryResult.Rows explain(String innerSql, String currentSchema) {
        RelNode plan = planner.plan(innerSql, currentSchema);
        if (plan instanceof MiniDbModify) {
            throw new IllegalArgumentException("EXPLAIN does not support DML");
        }
        List<Row> rows = new ArrayList<>();
        planRows(plan, null, rows);
        return new QueryResult.Rows(buildRoot(rows));
    }

    public QueryResult.Rows analyze(String innerSql) {
        return analyze(innerSql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public QueryResult.Rows analyze(String innerSql, String currentSchema) {
        RelNode plan = planner.plan(innerSql, currentSchema);
        if (plan instanceof MiniDbModify) {
            throw new IllegalArgumentException("EXPLAIN ANALYZE does not support DML");
        }
        Map<RelNode, NodeStats> sink = new IdentityHashMap<>();
        RelNode instrumented = Instrumenter.instrument(plan, sink);
        ExecContext ctx = new ExecContext(storage, allocator);
        List<Row> rows = new ArrayList<>();
        try (BatchIterator it = ((MiniDbRel) instrumented).execute(ctx)) {
            while (it.hasNext()) {
                it.next();
                // ANALYZE discards data, only measures; batch cleanup is handled
                // by the iterator close() cascade through the operator chain.
            }
        }
        analyzeRows(plan, null, rows, sink);
        return new QueryResult.Rows(buildRoot(rows));
    }

    private void analyzeRows(RelNode node, Integer parentId, List<Row> out,
                             Map<RelNode, NodeStats> sink) {
        // Trivial Project nodes (added by Calcite for column selection) are
        // collapsed into their parent to keep the ANALYZE tree consistent
        // with the EXPLAIN tree (see planRows).
        if (node instanceof MiniDbProject) {
            List<RelNode> inputs = node.getInputs();
            if (!inputs.isEmpty()) {
                analyzeRows(inputs.get(0), parentId, out, sink);
                return;
            }
        }
        int id = out.size() + 1;
        String op = operationName(node);
        NodeStats ns = sink.get(node);
        Long rows = ns == null ? null : ns.rows;
        Integer batches = ns == null ? null : ns.batches;
        Double elapsed = ns == null ? null : ns.elapsedMs;
        out.add(new Row(id, parentId, op, rows, batches, elapsed, null));
        for (RelNode input : node.getInputs()) {
            analyzeRows(input, id, out, sink);
        }
    }

    private int planRows(RelNode node, Integer parentId, List<Row> out) {
        // Trivial Project nodes (added by Calcite for column selection) are
        // collapsed into their parent to keep the EXPLAIN tree concise.
        if (node instanceof MiniDbProject) {
            List<RelNode> inputs = node.getInputs();
            if (!inputs.isEmpty()) {
                return planRows(inputs.get(0), parentId, out);
            }
        }
        int id = out.size() + 1;
        String op = operationName(node);
        Est est = estimate(node);
        out.add(new Row(id, parentId, op, est.rows, est.batches, null, est.remarks));
        for (RelNode input : node.getInputs()) {
            planRows(input, id, out);
        }
        return id;
    }

    private String operationName(RelNode node) {
        String name = node.getClass().getSimpleName();
        if (name.startsWith("MiniDb")) {
            name = name.substring("MiniDb".length());
        }
        if (node instanceof TableScan scan) {
            List<String> q = scan.getTable().getQualifiedName();
            String table = q.get(q.size() - 1);
            name = name + "(" + table + ")";
        }
        return name;
    }

    private Est estimate(RelNode node) {
        if (node instanceof MiniDbScan scan) {
            String table = tableName(scan);
            ArrowTable t = storage.getTable(table);
            return new Est((long) t.rowCount(), t.batches().size(), null);
        }
        if (node instanceof MiniDbProject) {
            return new Est(childRows(node), null, null);
        }
        if (node instanceof MiniDbSort sort) {
            long in = childRows(node);
            int offset = literalInt(sort.offset, 0);
            int fetch = literalInt(sort.fetch, Integer.MAX_VALUE);
            long r = Math.max(0, in - offset);
            r = Math.min(r, fetch);
            return new Est(r, null, null);
        }
        if (node instanceof MiniDbValues values) {
            return new Est((long) values.getTuples().size(), 1, null);
        }
        if (node instanceof MiniDbFilter filter) {
            long in = childRows(node);
            Sel s = filterSelectivity(filter);
            long est = Math.max(0, Math.round(in * s.selectivity));
            return new Est(est, null, s.remarks);
        }
        // default: passthrough
        return new Est(childRows(node), null, null);
    }

    private long childRows(RelNode node) {
        List<RelNode> inputs = node.getInputs();
        if (inputs.isEmpty()) {
            return 0L;
        }
        RelNode child = inputs.get(0);
        Long r = estimate(child).rows;
        return r == null ? 0L : r;
    }

    private record Sel(double selectivity, String remarks) {
    }

    private Sel filterSelectivity(MiniDbFilter filter) {
        RexNode cond = filter.getCondition();
        String table = scanTableOf(filter);
        if (table == null) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "no stats");
        }
        TableStats ts = stats.tableStats(table);
        if (ts == null) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "no stats");
        }
        if (ts.stale()) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "stats stale");
        }
        Histogram h = histogramForCondition(cond, table, ts);
        if (h == null) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "default selectivity");
        }
        return new Sel(h.selectivity(cond, h.totalRows()), "estimated");
    }

    /**
     * Resolve the histogram for the column referenced by the filter condition.
     * For a Scan->Filter the RexInputRef index maps directly to the table
     * column index; resolve the column name from the table schema, lowercase
     * it, and look up the histogram in TableStats.
     */
    private Histogram histogramForCondition(RexNode cond, String table, TableStats ts) {
        if (ts.columnHistograms().isEmpty()) {
            return null;
        }
        Integer colIndex = findFirstInputRef(cond);
        if (colIndex == null) {
            return null;
        }
        ArrowTable arrowTable = storage.getTable(table);
        List<com.minidb.server.catalog.ColumnMeta> columns =
                arrowTable.schema().columns();
        if (colIndex < 0 || colIndex >= columns.size()) {
            return null;
        }
        String colName = columns.get(colIndex).name().toLowerCase(Locale.ROOT);
        return ts.columnHistograms().get(colName);
    }

    private static Integer findFirstInputRef(RexNode node) {
        if (node instanceof RexInputRef ref) {
            return ref.getIndex();
        }
        if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                Integer idx = findFirstInputRef(operand);
                if (idx != null) {
                    return idx;
                }
            }
        }
        return null;
    }

    private String scanTableOf(RelNode node) {
        // walk down to the TableScan
        RelNode cur = node;
        while (cur != null && !(cur instanceof TableScan)) {
            List<RelNode> inputs = cur.getInputs();
            cur = inputs.isEmpty() ? null : inputs.get(0);
        }
        if (cur instanceof TableScan scan) {
            return tableName(scan);
        }
        return null;
    }

    private String tableName(TableScan scan) {
        List<String> q = scan.getTable().getQualifiedName();
        return q.get(q.size() - 1);
    }

    private static int literalInt(RexNode node, int defaultValue) {
        if (node instanceof RexLiteral lit) {
            return lit.getValueAs(BigDecimal.class).intValue();
        }
        return defaultValue;
    }

    private VectorSchemaRoot buildRoot(List<Row> rows) {
        IntVector id = new IntVector("id", allocator);
        IntVector parentId = new IntVector("parent_id", allocator);
        VarCharVector operation = new VarCharVector("operation", allocator);
        BigIntVector rowVec = new BigIntVector("rows", allocator);
        IntVector batches = new IntVector("batches", allocator);
        Float8Vector elapsed = new Float8Vector("elapsed_ms", allocator);
        VarCharVector remarks = new VarCharVector("remarks", allocator);
        List<FieldVector> vectors = List.of(id, parentId, operation, rowVec, batches, elapsed, remarks);
        int n = rows.size();
        for (FieldVector v : vectors) {
            v.setInitialCapacity(n);
            v.allocateNew();
        }
        try {
            for (int i = 0; i < n; i++) {
                Row r = rows.get(i);
                id.setSafe(i, r.id);
                if (r.parentId == null) {
                    parentId.setNull(i);
                } else {
                    parentId.setSafe(i, r.parentId);
                }
                operation.setSafe(i, r.operation.getBytes());
                if (r.rows == null) {
                    rowVec.setNull(i);
                } else {
                    rowVec.setSafe(i, r.rows);
                }
                if (r.batches == null) {
                    batches.setNull(i);
                } else {
                    batches.setSafe(i, r.batches);
                }
                if (r.elapsedMs == null) {
                    elapsed.setNull(i);
                } else {
                    elapsed.setSafe(i, r.elapsedMs);
                }
                if (r.remarks == null) {
                    remarks.setNull(i);
                } else {
                    remarks.setSafe(i, r.remarks.getBytes());
                }
            }
            for (FieldVector v : vectors) {
                v.setValueCount(n);
            }
            return VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
        } catch (RuntimeException e) {
            for (FieldVector v : vectors) {
                v.close();
            }
            throw e;
        }
    }

    private record Row(Integer id, Integer parentId, String operation,
                       Long rows, Integer batches, Double elapsedMs, String remarks) {
    }

    private record Est(Long rows, Integer batches, String remarks) {
    }
}
