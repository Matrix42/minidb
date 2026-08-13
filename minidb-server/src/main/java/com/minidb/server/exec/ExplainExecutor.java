package com.minidb.server.exec;

import com.minidb.server.plan.physical.MiniDbAggregate;
import com.minidb.server.plan.physical.MiniDbCalc;
import com.minidb.server.plan.physical.MiniDbFilter;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbModify;
import com.minidb.server.plan.physical.MiniDbProject;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.plan.physical.MiniDbScan;
import com.minidb.server.plan.physical.MiniDbSetOp;
import com.minidb.server.plan.physical.MiniDbSort;
import com.minidb.server.plan.physical.MiniDbUnion;
import com.minidb.server.plan.physical.MiniDbValues;
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
import org.apache.calcite.rex.RexLocalRef;
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
        // with the EXPLAIN tree (see planRows). Window projects are NOT
        // trivial and are kept.
        if (isTrivialProject(node)) {
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
        if (isTrivialProject(node)) {
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

    private static boolean isTrivialProject(RelNode node) {
        if (node instanceof MiniDbProject p) {
            return isIdentity(p.getProjects());
        }
        if (node instanceof MiniDbCalc calc) {
            // A Calc with a condition is a filter, not a trivial projection.
            if (calc.getProgram().getCondition() != null) {
                return false;
            }
            List<RexNode> projects = new ArrayList<>();
            for (RexLocalRef ref : calc.getProgram().getProjectList()) {
                projects.add(calc.getProgram().expandLocalRef(ref));
            }
            return isIdentity(projects);
        }
        return false;
    }

    private static boolean isIdentity(List<? extends RexNode> projects) {
        for (int i = 0; i < projects.size(); i++) {
            if (!(projects.get(i) instanceof RexInputRef ref)
                    || ref.getIndex() != i) {
                return false;
            }
        }
        return true;
    }

    private String operationName(RelNode node) {
        String name;
        if (node instanceof MiniDbCalc calc) {
            // A Calc is a merged Project+Filter; report what it actually does
            // so EXPLAIN stays readable. With a condition it filters, without
            // one it only projects.
            name = calc.getProgram().getCondition() == null ? "Project" : "Filter";
        } else {
            name = node.getClass().getSimpleName();
            if (name.startsWith("MiniDb")) {
                name = name.substring("MiniDb".length());
            }
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
        if (node instanceof MiniDbCalc calc) {
            RexLocalRef condRef = calc.getProgram().getCondition();
            if (condRef == null) {
                return new Est(childRows(node), null, null);
            }
            RexNode condition = calc.getProgram().expandLocalRef(condRef);
            long in = childRows(node);
            Sel s = filterSelectivity(condition, calc);
            long est = Math.max(0, Math.round(in * s.selectivity));
            return new Est(est, null, s.remarks);
        }
        if (node instanceof MiniDbJoin) {
            long l = childRows(node);
            Long r = estimate(node.getInputs().get(1)).rows;
            long rv = r == null ? 0 : r;
            long est = (long) (l * rv * 0.1); // loose join selectivity
            return new Est(Math.max(0, est), null, "estimated");
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
            Sel s = filterSelectivity(filter.getCondition(), filter);
            long est = Math.max(0, Math.round(in * s.selectivity));
            return new Est(est, null, s.remarks);
        }
        if (node instanceof MiniDbAggregate agg) {
            long in = childRows(node);
            if (agg.getGroupSet().isEmpty()) {
                return new Est(1L, 1, "estimated");
            }
            Long distinct = groupDistinct(agg);
            long est = distinct == null ? in : Math.min(in, Math.max(1, distinct));
            return new Est(est, 1, "estimated");
        }
        if (node instanceof MiniDbUnion union) {
            long sum = 0;
            for (RelNode in : node.getInputs()) {
                Long r = estimate(in).rows;
                sum += r == null ? 0 : r;
            }
            if (union.all) {
                return new Est(sum, null, null);
            }
            Long distinct = firstColumnDistinct(union);
            long est = distinct == null ? Math.max(1, sum / 2)
                    : Math.min(sum, Math.max(1, distinct));
            return new Est(est, null, "estimated");
        }
        if (node instanceof MiniDbSetOp setOp) {
            long est = childRows(node);
            for (int i = 1; i < node.getInputs().size(); i++) {
                Long r = estimate(node.getInputs().get(i)).rows;
                long c = r == null ? 0 : r;
                if (setOp.isIntersect()) {
                    est = Math.min(est, c);
                } else {
                    est = Math.max(0, est - c);
                }
            }
            return new Est(est, null, "estimated");
        }
        // default: passthrough
        return new Est(childRows(node), null, null);
    }

    private Long groupDistinct(MiniDbAggregate agg) {
        if (agg.getGroupSet().isEmpty()) {
            return null;
        }
        int firstCol = agg.getGroupSet().nextSetBit(0);
        String table = scanTableOf(agg);
        if (table == null) {
            return null;
        }
        TableStats ts = stats.tableStats(table);
        if (ts == null || ts.stale()) {
            return null;
        }
        ArrowTable arrowTable = storage.getTable(table);
        List<com.minidb.server.catalog.ColumnMeta> columns =
                arrowTable.schema().columns();
        if (firstCol < 0 || firstCol >= columns.size()) {
            return null;
        }
        String colName = columns.get(firstCol).name().toLowerCase(Locale.ROOT);
        Histogram h = ts.columnHistograms().get(colName);
        return h == null ? null : h.distinctCount();
    }

    private Long firstColumnDistinct(RelNode node) {
        String table = scanTableOf(node);
        if (table == null) {
            return null;
        }
        TableStats ts = stats.tableStats(table);
        if (ts == null || ts.stale()) {
            return null;
        }
        ArrowTable arrowTable = storage.getTable(table);
        List<com.minidb.server.catalog.ColumnMeta> columns =
                arrowTable.schema().columns();
        if (columns.isEmpty()) {
            return null;
        }
        String colName = columns.get(0).name().toLowerCase(Locale.ROOT);
        Histogram h = ts.columnHistograms().get(colName);
        return h == null ? null : h.distinctCount();
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

    private Sel filterSelectivity(RexNode cond, RelNode node) {
        String table = scanTableOf(node);
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
