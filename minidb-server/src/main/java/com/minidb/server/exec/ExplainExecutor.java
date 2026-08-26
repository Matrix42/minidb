package com.minidb.server.exec;
import com.minidb.storage.common.BatchIterator;

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
import com.minidb.server.stats.StatsEstimator;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.stats.TableStats;
import com.minidb.storage.common.TableHandle;
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
        planRows(plan, null, rows, currentSchema);
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

    private int planRows(RelNode node, Integer parentId, List<Row> out, String currentSchema) {
        // Trivial Project nodes (added by Calcite for column selection) are
        // collapsed into their parent to keep the EXPLAIN tree concise.
        if (isTrivialProject(node)) {
            List<RelNode> inputs = node.getInputs();
            if (!inputs.isEmpty()) {
                return planRows(inputs.get(0), parentId, out, currentSchema);
            }
        }
        int id = out.size() + 1;
        String op = operationName(node);
        Est est = estimate(node, currentSchema);
        out.add(new Row(id, parentId, op, est.rows, est.batches, null, est.remarks));
        for (RelNode input : node.getInputs()) {
            planRows(input, id, out, currentSchema);
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
            name = calc.getProgram().getCondition() == null ? "Project" : "Filter";
        } else if (node instanceof MiniDbScan scan && scan.pushedFilter() != null) {
            // 谓词下推:Filter 折叠进 Scan,EXPLAIN 仍显示为 Filter 以保持可读
            name = "Filter";
        } else {
            name = node.getClass().getSimpleName();
            if (name.startsWith("MiniDb")) {
                name = name.substring("MiniDb".length());
            }
        }
        if (node instanceof TableScan scan) {
            List<String> q = scan.getTable().getQualifiedName();
            String table = q.get(q.size() - 1);
            // 二级索引:EXPLAIN 显示 index=<name>
            if (node instanceof MiniDbScan mScan && mScan.usedIndex() != null) {
                table += " index=" + mScan.usedIndex();
            }
            name = name + "(" + table + ")";
        }
        return name;
    }

    private Est estimate(RelNode node, String currentSchema) {
        if (node instanceof MiniDbScan scan) {
            String[] st = resolveTable(scan, currentSchema);
            TableHandle t = storage.getTable(st[0], st[1]);
            long rows = t.rowCount();
            // 谓词下推:Scan 携带过滤条件,估算选择率
            String remarks = null;
            if (scan.pushedFilter() != null) {
                Sel s = filterSelectivity(scan.pushedFilter(), scan, currentSchema);
                rows = Math.max(0, Math.round(rows * s.selectivity));
                remarks = s.remarks;
            }
            return new Est(rows, t.partCount(), remarks);
        }
        if (node instanceof MiniDbProject) {
            return new Est(childRows(node, currentSchema), null, null);
        }
        if (node instanceof MiniDbCalc calc) {
            RexLocalRef condRef = calc.getProgram().getCondition();
            if (condRef == null) {
                return new Est(childRows(node, currentSchema), null, null);
            }
            RexNode condition = calc.getProgram().expandLocalRef(condRef);
            long in = childRows(node, currentSchema);
            Sel s = filterSelectivity(condition, calc, currentSchema);
            long est = Math.max(0, Math.round(in * s.selectivity));
            return new Est(est, null, s.remarks);
        }
        if (node instanceof MiniDbJoin) {
            long l = childRows(node, currentSchema);
            Long r = estimate(node.getInputs().get(1), currentSchema).rows;
            long rv = r == null ? 0 : r;
            long est = (long) (l * rv * 0.1); // loose join selectivity
            return new Est(Math.max(0, est), null, "estimated");
        }
        if (node instanceof MiniDbSort sort) {
            long in = childRows(node, currentSchema);
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
            long in = childRows(node, currentSchema);
            Sel s = filterSelectivity(filter.getCondition(), filter, currentSchema);
            long est = Math.max(0, Math.round(in * s.selectivity));
            return new Est(est, null, s.remarks);
        }
        if (node instanceof MiniDbAggregate agg) {
            long in = childRows(node, currentSchema);
            if (agg.getGroupSet().isEmpty()) {
                return new Est(1L, 1, "estimated");
            }
            Long distinct = groupDistinct(agg, currentSchema);
            long est = distinct == null ? in : Math.min(in, Math.max(1, distinct));
            return new Est(est, 1, "estimated");
        }
        if (node instanceof MiniDbUnion union) {
            long sum = 0;
            for (RelNode in : node.getInputs()) {
                Long r = estimate(in, currentSchema).rows;
                sum += r == null ? 0 : r;
            }
            if (union.all) {
                return new Est(sum, null, null);
            }
            Long distinct = firstColumnDistinct(union, currentSchema);
            long est = distinct == null ? Math.max(1, sum / 2)
                    : Math.min(sum, Math.max(1, distinct));
            return new Est(est, null, "estimated");
        }
        if (node instanceof MiniDbSetOp setOp) {
            long est = childRows(node, currentSchema);
            for (int i = 1; i < node.getInputs().size(); i++) {
                Long r = estimate(node.getInputs().get(i), currentSchema).rows;
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
        return new Est(childRows(node, currentSchema), null, null);
    }

    private Long groupDistinct(MiniDbAggregate agg, String currentSchema) {
        if (agg.getGroupSet().isEmpty()) {
            return null;
        }
        int firstCol = agg.getGroupSet().nextSetBit(0);
        String[] st = scanTableOf(agg, currentSchema);
        if (st == null) {
            return null;
        }
        TableStats ts = stats.tableStats(st[0] + "." + st[1]);
        if (ts == null || ts.stale()) {
            return null;
        }
        TableHandle arrowTable = storage.getTable(st[0], st[1]);
        List<com.minidb.storage.common.ColumnMeta> columns =
                arrowTable.schema().columns();
        if (firstCol < 0 || firstCol >= columns.size()) {
            return null;
        }
        String colName = columns.get(firstCol).name().toLowerCase(Locale.ROOT);
        Histogram h = ts.columnHistograms().get(colName);
        return h == null ? null : h.distinctCount();
    }

    private Long firstColumnDistinct(RelNode node, String currentSchema) {
        String[] st = scanTableOf(node, currentSchema);
        if (st == null) {
            return null;
        }
        TableStats ts = stats.tableStats(st[0] + "." + st[1]);
        if (ts == null || ts.stale()) {
            return null;
        }
        TableHandle arrowTable = storage.getTable(st[0], st[1]);
        List<com.minidb.storage.common.ColumnMeta> columns =
                arrowTable.schema().columns();
        if (columns.isEmpty()) {
            return null;
        }
        String colName = columns.get(0).name().toLowerCase(Locale.ROOT);
        Histogram h = ts.columnHistograms().get(colName);
        return h == null ? null : h.distinctCount();
    }

    private long childRows(RelNode node, String currentSchema) {
        List<RelNode> inputs = node.getInputs();
        if (inputs.isEmpty()) {
            return 0L;
        }
        RelNode child = inputs.get(0);
        Long r = estimate(child, currentSchema).rows;
        return r == null ? 0L : r;
    }

    private record Sel(double selectivity, String remarks) {
    }

    private Sel filterSelectivity(RexNode cond, RelNode node, String currentSchema) {
        String[] st = scanTableOf(node, currentSchema);
        if (st == null) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "no stats");
        }
        TableStats ts = stats.tableStats(st[0] + "." + st[1]);
        if (ts == null) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "no stats");
        }
        if (ts.stale()) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "stats stale");
        }
        Histogram h = StatsEstimator.histogramForCondition(
                cond, storage.getTable(st[0], st[1]).schema(), ts);
        if (h == null) {
            return new Sel(Histogram.DEFAULT_SELECTIVITY, "default selectivity");
        }
        return new Sel(h.selectivity(cond, h.totalRows()), "estimated");
    }

    /** 从节点的第一个 TableScan 解析出 {schema, table};无 TableScan 返回 null。 */
    private String[] scanTableOf(RelNode node, String currentSchema) {
        RelNode cur = node;
        while (cur != null && !(cur instanceof TableScan)) {
            List<RelNode> inputs = cur.getInputs();
            cur = inputs.isEmpty() ? null : inputs.get(0);
        }
        if (cur instanceof TableScan scan) {
            return resolveTable(scan, currentSchema);
        }
        return null;
    }

    /** 把 TableScan 的限定名解析成 {schema, table}:3 段用倒数第二段作 schema,2 段(裸名)用 currentSchema。 */
    private static String[] resolveTable(TableScan scan, String currentSchema) {
        List<String> q = scan.getTable().getQualifiedName();
        String table = q.get(q.size() - 1);
        String schema = q.size() >= 3 ? q.get(q.size() - 2) : currentSchema;
        return new String[]{schema, table};
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
