package com.minidb.server.plan.physical;
import com.google.common.collect.Range;
import com.minidb.server.calcite.MiniDbCalciteTable;
import com.minidb.server.storage.IndexManager;
import com.minidb.storage.common.IndexDef;
import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.server.exec.ExecContext;
import com.minidb.server.exec.InformationSchema;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.transaction.TransactionManager;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.RowValue;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.common.TableSchema;
import com.minidb.storage.lsm.SSTableWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;


public class MiniDbScan extends TableScan implements MiniDbRel {

    /** 下推的列裁剪:null→全列;非null→只读这些列索引(0-based)。 */
    private final int[] projectedColumns;
    /** 下推的谓词:null→不过滤;非null→扫描时 eval 并过滤。 */
    private final RexNode pushedFilter;
    private final String usedIndex;

    public MiniDbScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, table, null, null);
    }

    public MiniDbScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                      int[] projectedColumns, RexNode pushedFilter) {
        super(cluster, traitSet, List.of(), table);
        this.projectedColumns = projectedColumns;
        this.pushedFilter = pushedFilter;
        this.usedIndex = selectIndex(table, pushedFilter);
        if (projectedColumns != null && !isIdentityProjection(projectedColumns,
                table.getRowType().getFieldCount())) {
            // 投影后 rowType = 投影列子集:上层算子的表达式/行类型分析按 rowType 取列,
            // 若 rowType 仍是全表列,投影列非前缀时索引错位(列裁剪的 Planners pass 会触发)。
            this.rowType = subsetRowType(cluster.getTypeFactory(), table.getRowType(),
                    projectedColumns);
        }
    }

    private static boolean isIdentityProjection(int[] projectedColumns, int fieldCount) {
        if (projectedColumns.length != fieldCount) {
            return false;
        }
        for (int i = 0; i < fieldCount; i++) {
            if (projectedColumns[i] != i) {
                return false;
            }
        }
        return true;
    }

    private static RelDataType subsetRowType(RelDataTypeFactory typeFactory,
                                             RelDataType full, int[] projectedColumns) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int p : projectedColumns) {
            RelDataTypeField field = full.getFieldList().get(p);
            builder.add(field.getName(), field.getType());
        }
        return builder.build();
    }

    /** 是否有下推优化(列裁剪 或 谓词下推)。 */
    public boolean hasPushdown() {
        return projectedColumns != null || pushedFilter != null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (usedIndex != null) pw.item("index", usedIndex);
        if (pushedFilter != null) {
            pw.item("filter", pushedFilter);
        }
        if (projectedColumns != null) {
            pw.item("cols", Arrays.toString(projectedColumns));
        }
        return pw;
    }

    public int[] projectedColumns() {
        return projectedColumns;
    }

    public RexNode pushedFilter() {
        return pushedFilter;
    }

    public String usedIndex() {
        return usedIndex;
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
        String schemaName;
        String tableName;
        if (n >= 3) {
            schemaName = qualified.get(n - 2);
            tableName = qualified.get(n - 1);
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
            schemaName = ctx.currentSchema();
            tableName = qualified.get(n - 1);
            tableHandle = ctx.getTable(tableName);
        }
        // 快照读:事务中跳过 PointLookup/RangeBounds/indexLookup 优化,
        // 因为它们没有快照感知变体,直接用 snapshot scan 后再 applyPushdown 过滤/投影。
        if (ctx.inTransaction() && ctx.tx().snapshotTxId() >= 0) {
            BatchIterator source = tableHandle.scan(ctx.tx().snapshotTxId());
            BatchIterator result = applyPushdown(source, ctx);
            // Serializable:记录读集(按 schema.table.column 粒度)
            recordReadSet(ctx, schemaName, tableName);
            return result;
        }
        // 主键等值点查:WHERE pk = literal / pk IN (...) / pk = v1 OR pk = v2
        // (全部主键列绑定)时走 LSM 的 Bloom + getByKey,避免全表扫描 + 逐行
        // 求值(TPC-DS 维度点查主路径)。
        if (pushedFilter != null) {
            PointLookup lookup = PointLookup.extract(pushedFilter, tableHandle.schema(),
                    getCluster().getRexBuilder());
            if (lookup != null) {
                return pointLookup(tableHandle, lookup, ctx);
            }
            // 范围裁剪:WHERE pk > X AND pk < Y 只读相交的 SSTable 文件/块
            // (超集语义,区间外行由 pushedFilter 逐行过滤兜底)。
            RangeBounds range = RangeBounds.extract(pushedFilter, tableHandle.schema());
            if (range != null) {
                return applyPushdown(tableHandle.scan(range.lo, range.hi), ctx);
            }
            if (usedIndex != null) {
                BatchIterator indexed = indexLookup(tableHandle, ctx);
                if (indexed != null) return indexed;
            }
        }
        return applyPushdown(
                    projectedColumns != null
                            ? tableHandle.scan(projectedColumns)
                            : tableHandle.scan(),
                    ctx);
    }

    /**
     * 主键点查:对每个候选键 getByKey,命中的行收集进一个批,残留条件
     * (非主键绑定)在整批上求值,再按投影列裁剪。批所有权沿链传递:
     * root → (filtered) → (projected) → 最终批,每步换批就关掉旧批,
     * 最终批交给 singleBatch 持有。
     */
    private BatchIterator pointLookup(TableHandle table, PointLookup lookup, ExecContext ctx) {
        List<Object[]> matched = new ArrayList<>();
        for (List<Object> key : lookup.keys) {
            RowValue rv = table.getByKey(key);
            if (rv != null && rv.kind() == RowValue.INSERT) {
                matched.add(rv.values());
            }
        }
        if (matched.isEmpty()) {
            return BatchIterator.interruptible(BatchIterator.empty());
        }
        VectorSchemaRoot owned = rowsToRoot(matched, table.schema(), ctx);
        VectorSchemaRoot out = null;
        try {
            // 残留条件在单批上求值(applyFilter:null→原批,全保留→原批,无匹配→null)
            VectorSchemaRoot filtered = applyFilter(owned, lookup.residual, ctx);
            if (filtered == null) {
                return BatchIterator.interruptible(BatchIterator.empty());
            }
            if (filtered != owned) {
                owned.close();
                owned = filtered;
            }
            if (projectedColumns != null) {
                VectorSchemaRoot projected = applyProject(owned, ctx);
                if (projected != owned) {
                    owned.close();
                    owned = projected;
                }
            }
            out = owned;
            return singleBatch(out);
        } finally {
            // 未交给 singleBatch 的批(空结果/异常路径)在此释放
            if (out == null) {
                owned.close();
            }
        }
    }

    /** 行集 → 全列 VectorSchemaRoot(表 schema,残留条件/投影按原列索引)。 */
    private static VectorSchemaRoot rowsToRoot(List<Object[]> rows, TableSchema schema,
                                               ExecContext ctx) {
        VectorSchemaRoot root = VectorSchemaRoot.create(
                ArrowTypes.arrowSchema(schema), ctx.allocator());
        root.allocateNew();
        if (!rows.isEmpty()) {
            // 先 setValueCount 保证有效性缓冲区已分配(writeRow 前)
            root.setRowCount(rows.size());
        }
        for (int i = 0; i < rows.size(); i++) {
            SSTableWriter.writeRow(root, i, rows.get(i), schema);
        }
        root.setRowCount(rows.size());
        return root;
    }

    /** AND 链拆成级联;其余节点原样。 */
    private static void splitConjuncts(RexNode node, List<RexNode> out) {
        if (node instanceof RexCall call && call.getKind() == SqlKind.AND) {
            for (RexNode o : call.getOperands()) {
                splitConjuncts(o, out);
            }
        } else {
            out.add(node);
        }
    }

    private BatchIterator indexLookup(TableHandle dataTable, ExecContext ctx) {
        List<String> qualified = table.getQualifiedName();
        String schemaName = qualified.size() >= 3 ? qualified.get(qualified.size() - 2) : ctx.currentSchema();
        String tableName = qualified.get(qualified.size() - 1);
        MiniDbCalciteTable calciteTable = table.unwrap(MiniDbCalciteTable.class);
        if (calciteTable == null) return null;
        TableSchema dataSchema = calciteTable.tableSchema();
        IndexDef indexDef = null;
        for (IndexDef def : dataSchema.indexes())
            if (def.name().equals(usedIndex)) { indexDef = def; break; }
        if (indexDef == null) return null;
        TableHandle indexTable = ctx.storage().indexManager().getIndex(schemaName, tableName, usedIndex);
        if (indexTable == null) return null;

        List<RexNode> conjuncts = new ArrayList<>();
        splitConjuncts(pushedFilter, conjuncts);
        List<Integer> idxPositions = new ArrayList<>(indexDef.columns().size());
        for (String col : indexDef.columns()) idxPositions.add(dataSchema.columnIndex(col));
        List<Object> loPrefix = new ArrayList<>();
        for (int colIdx : idxPositions) {
            Object bound = null;
            for (RexNode c : conjuncts) {
                IndexBound b = extractIndexBound(c);
                if (b != null && b.colIndex == colIdx) { bound = b.value; break; }
            }
            if (bound == null || "SEARCH".equals(bound) || "OR".equals(bound)) break;
            loPrefix.add(bound);
        }
        if (loPrefix.isEmpty()) return null;

        Set<List<Object>> matchedPks = new HashSet<>();
        int pkCount = dataSchema.primaryKey().size();
        try (BatchIterator it = indexTable.scan(loPrefix, loPrefix)) {
            while (it.hasNext()) {
                VectorSchemaRoot b = it.next();
                for (int r = 0; r < b.getRowCount(); r++) {
                    boolean prefixMatch = true;
                    for (int c = 0; c < loPrefix.size(); c++) {
                        Object v = b.getVector(c).getObject(r);
                        if (v == null || !v.equals(loPrefix.get(c))) { prefixMatch = false; break; }
                    }
                    if (!prefixMatch) continue;
                    List<Object> pk = new ArrayList<>(pkCount);
                    for (int c = 0; c < pkCount; c++)
                        pk.add(b.getVector(loPrefix.size() + c).getObject(r));
                    matchedPks.add(pk);
                }
            }
        }
        if (matchedPks.isEmpty()) return BatchIterator.interruptible(BatchIterator.empty());

        List<Object[]> rows = new ArrayList<>();
        for (List<Object> pk : matchedPks) {
            RowValue rv = dataTable.getByKey(pk);
            if (rv != null && rv.kind() == RowValue.INSERT) rows.add(rv.values());
        }
        if (rows.isEmpty()) return BatchIterator.interruptible(BatchIterator.empty());
        VectorSchemaRoot owned = rowsToRoot(rows, dataSchema, ctx);
        VectorSchemaRoot out = null;
        try {
            List<RexNode> residual = new ArrayList<>();
            for (RexNode c : conjuncts) {
                IndexBound b = extractIndexBound(c);
                if (b == null || !idxPositions.contains(b.colIndex)) residual.add(c);
            }
            RexNode residualFilter = residual.isEmpty() ? null
                    : (residual.size() == 1 ? residual.get(0)
                    : getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND, residual));
            VectorSchemaRoot filtered = applyFilter(owned, residualFilter, ctx);
            if (filtered == null) return BatchIterator.interruptible(BatchIterator.empty());
            if (filtered != owned) { owned.close(); owned = filtered; }
            if (projectedColumns != null) {
                VectorSchemaRoot projected = applyProject(owned, ctx);
                if (projected != owned) { owned.close(); owned = projected; }
            }
            out = owned;
            return singleBatch(out);
        } finally { if (out == null) owned.close(); }
    }

    private static String selectIndex(RelOptTable relTable, RexNode pushedFilter) {
        if (pushedFilter == null) return null;
        MiniDbCalciteTable t = relTable.unwrap(MiniDbCalciteTable.class);
        if (t == null) return null;
        TableSchema schema = t.tableSchema();
        if (schema.indexes().isEmpty()) return null;
        List<RexNode> conjuncts = new ArrayList<>();
        splitConjuncts(pushedFilter, conjuncts);
        Set<Integer> boundCols = new HashSet<>();
        for (RexNode c : conjuncts) {
            IndexBound bound = extractIndexBound(c);
            if (bound != null) boundCols.add(bound.colIndex);
        }
        if (boundCols.isEmpty()) return null;
        String best = null;
        int bestCount = 0;
        for (IndexDef def : schema.indexes()) {
            int count = 0;
            for (String col : def.columns())
                if (boundCols.contains(schema.columnIndex(col))) count++;
            if (count > 0 && count == def.columns().size() && count > bestCount) {
                best = def.name(); bestCount = count;
            }
        }
        return best;
    }

    private static IndexBound extractIndexBound(RexNode node) {
        if (node instanceof RexCall call) {
            if (call.getKind() == SqlKind.EQUALS) return boundEqualityGeneral(call);
            if (call.getKind() == SqlKind.SEARCH) return boundSearch(call);
            if (call.getKind() == SqlKind.OR) return boundOrChain(call);
        }
        return null;
    }

    private static IndexBound boundEqualityGeneral(RexCall call) {
        RexNode l = call.getOperands().get(0), r = call.getOperands().get(1);
        RexInputRef ref; RexLiteral lit;
        if (l instanceof RexInputRef a && r instanceof RexLiteral b) { ref = a; lit = b; }
        else if (l instanceof RexLiteral a && r instanceof RexInputRef b) { ref = b; lit = a; }
        else return null;
        Object value = literalValue(lit);
        return value == null ? null : new IndexBound(ref.getIndex(), value);
    }

    private static IndexBound boundSearch(RexCall call) {
        RexNode l = call.getOperands().get(0), r = call.getOperands().get(1);
        if (l instanceof RexInputRef ref && r instanceof RexLiteral lit) {
            Sarg<?> sarg = lit.getValueAs(Sarg.class);
            if (sarg != null && sarg.isPoints()) return new IndexBound(ref.getIndex(), "SEARCH");
        }
        return null;
    }

    private static IndexBound boundOrChain(RexCall call) {
        Integer colIndex = null;
        for (RexNode o : call.getOperands()) {
            if (!(o instanceof RexCall c) || c.getKind() != SqlKind.EQUALS) return null;
            IndexBound b = boundEqualityGeneral(c);
            if (b == null) return null;
            if (colIndex == null) colIndex = b.colIndex;
            else if (colIndex != b.colIndex) return null;
        }
        return colIndex == null ? null : new IndexBound(colIndex, "OR");
    }

    private static Object literalValue(RexLiteral lit) {
        // VARCHAR columns: LSM key encoding uses Comparable, but Arrow VarCharVector.getObject()
        // returns org.apache.arrow.vector.util.Text which does NOT implement Comparable,
        // so LSM range scan(lo,hi) would throw ClassCastException on Text comparison.
        // Return null for VARCHAR to skip index selection — the query still works correctly
        // via full scan. A future fix would encode VARCHAR keys as String in LSM.
        SqlTypeName tn = lit.getTypeName();
        if (tn == SqlTypeName.VARCHAR || tn == SqlTypeName.CHAR) return null;
        // Calcite stores INTEGER literals as DECIMAL type. Use Number to extract,
        // then convert to Integer to match IntVector.getObject(r) return types.
        Number n = lit.getValueAs(Number.class);
        if (n != null) return n.intValue();
        return null;
    }

    private record IndexBound(int colIndex, Object value) {}

    /** 整数型字面量取数值(INT→Integer、BIGINT→Long);非整数型返回 null(回退扫描)。 */
    private static Object numericLiteralValue(RexLiteral lit) {
        SqlTypeName tn = lit.getTypeName();
        if (tn != SqlTypeName.TINYINT && tn != SqlTypeName.SMALLINT
                && tn != SqlTypeName.INTEGER && tn != SqlTypeName.BIGINT) {
            return null;
        }
        Object v = lit.getValueAs(Number.class);
        if (v == null) {
            return null;
        }
        return tn == SqlTypeName.BIGINT ? ((Number) v).longValue() : ((Number) v).intValue();
    }

    /**
     * 对底层扫描迭代器套上列裁剪和/或谓词过滤。
     * 无下推时直接返回原迭代器。
     */
    private BatchIterator applyPushdown(BatchIterator source, ExecContext ctx) {
        if (projectedColumns == null && pushedFilter == null) {
            return source;
        }
        // 存储层已做列裁剪(source 只含投影列),filter 需映射到投影位置后 eval
        boolean sourceAlreadyProjected = projectedColumns != null;
        final RexNode effectiveFilter = sourceAlreadyProjected && pushedFilter != null
                ? remapToProjected(pushedFilter, projectedColumns)
                : pushedFilter;
        Deque<VectorSchemaRoot> owned = new ArrayDeque<>();
        return BatchIterator.interruptible(new BatchIterator() {
            VectorSchemaRoot pending;

            @Override
            public boolean hasNext() {
                while (pending == null && source.hasNext()) {
                    VectorSchemaRoot batch = source.next();
                    VectorSchemaRoot filtered = null;
                    try {
                        filtered = applyFilter(batch, effectiveFilter, ctx);
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
        return applyFilter(batch, pushedFilter, ctx);
    }

    private VectorSchemaRoot applyFilter(VectorSchemaRoot batch, RexNode filter, ExecContext ctx) {
        if (filter == null) {
            return batch;
        }
        ValueVector condition = ctx.interpreter().eval(filter, batch);
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
            // 预分配 kept,先收集保留行号再按列批量拷贝(固定宽走无检查 copyFrom)
            for (FieldVector v : out.getFieldVectors()) {
                v.setInitialCapacity(kept);
                v.allocateNew();
            }
            int[] keptRows = new int[kept];
            int dst = 0;
            for (int i = 0; i < batch.getRowCount(); i++) {
                if (!condition.isNull(i) && ((BitVector) condition).get(i) == 1) {
                    keptRows[dst++] = i;
                }
            }
            RowCopier.copyRowsByIndex(batch, keptRows, 0, out, 0, kept);
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
        List<FieldVector> outVectors = new ArrayList<>(projectedColumns.length);
        for (int col : projectedColumns) {
            outVectors.add(batch.getVector(col).getField().createVector(ctx.allocator()));
        }
        int rows = batch.getRowCount();
        for (FieldVector v : outVectors) {
            v.setInitialCapacity(rows);
            v.allocateNew();
        }
        for (int c = 0; c < projectedColumns.length; c++) {
            // 列重排批量拷贝:src 列 projectedColumns[c] → dst 列 c,行连续
            RowCopier.copyRows(batch.getVector(projectedColumns[c]), 0,
                    outVectors.get(c), 0, rows);
        }
        for (FieldVector v : outVectors) {
            v.setValueCount(rows);
        }
        return VectorSchemaRoot.of(outVectors.toArray(new FieldVector[0]));
    }

    /**
     * 将 filter 条件中的列索引从「原表索引」映射到「投影后的位置」。
     * 存储层已做列裁剪,source 只含投影列,filter 需要引用投影位置而非原位置。
     */
    private static RexNode remapToProjected(RexNode node, int[] projectedColumns) {
        int maxCol = 0;
        for (int c : projectedColumns) maxCol = Math.max(maxCol, c);
        int[] inverse = new int[maxCol + 1];
        Arrays.fill(inverse, -1);
        for (int i = 0; i < projectedColumns.length; i++) {
            inverse[projectedColumns[i]] = i;
        }
        return node.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                int orig = inputRef.getIndex();
                int proj = orig < inverse.length ? inverse[orig] : -1;
                return proj >= 0 ? new RexInputRef(proj, inputRef.getType()) : inputRef;
            }
        });
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

    /**
     * 从下推 filter 提取主键点查。仅当:
     * <ul>
     *   <li>每个主键列都被 AND 级联的绑定条件覆盖:单等值 `col = literal`、
     *       IN 点集 `col SEARCH {v1,v2,...}`、或同列等值 OR 链(顺序任意,字面量
     *       在左/右均可);</li>
     *   <li>主键列类型为 INTEGER/BIGINT——LSM 的零填充十进制 key 编码只对
     *       Integer/Long 往返保类型,其余类型(如 VARCHAR 的 Text key)比较会错;</li>
     *   <li>候选键数(各列值集合的笛卡尔积)不超过 {@link #MAX_POINT_LOOKUP_KEYS}——
     *       防多列 IN 的候选爆炸(超阈值回退扫描,正确性不受影响);</li>
     *   <li>主键列未被重复绑定(如 `id=1 AND id=2`,重复条件保留为 residual 避免丢语义)。</li>
     * </ul>
     * 其余条件保留为 {@link #residual},在命中的行上求值。返回 null 表示不可点查
     * (回退全表扫描 + 逐行过滤)。
     */
    private static final class PointLookup {
        /** 候选主键(各列候选值的笛卡尔积);空或超阈值时不点查。 */
        static final int MAX_POINT_LOOKUP_KEYS = 256;

        final List<List<Object>> keys;
        final RexNode residual;

        private PointLookup(List<List<Object>> keys, RexNode residual) {
            this.keys = keys;
            this.residual = residual;
        }

        static PointLookup extract(RexNode filter, TableSchema schema, RexBuilder rexBuilder) {
            List<String> pk = schema.primaryKey();
            if (pk.isEmpty() || filter == null) {
                return null;
            }
            for (String col : pk) {
                ColumnType t = schema.column(col).type();
                if (t != ColumnType.INTEGER && t != ColumnType.BIGINT) {
                    return null;
                }
            }
            List<RexNode> conjuncts = new ArrayList<>();
            splitConjuncts(filter, conjuncts);
            Map<Integer, List<Object>> valuesByCol = new HashMap<>();
            List<RexNode> residual = new ArrayList<>();
            for (RexNode c : conjuncts) {
                BoundVals bv = boundValues(c);
                if (bv != null && pk.stream().anyMatch(p -> schema.columnIndex(p) == bv.colIndex)) {
                    if (valuesByCol.putIfAbsent(bv.colIndex, bv.values) != null) {
                        // 主键列重复绑定(矛盾条件如 id=1 AND id=2):保留为 residual,不能丢
                        residual.add(c);
                    }
                } else {
                    residual.add(c);
                }
            }
            if (valuesByCol.size() != pk.size()) {
                return null; // 主键未完全绑定,不是点查
            }
            List<List<Object>> keys = new ArrayList<>();
            cartesianKeys(valuesByCol, schema, 0, new ArrayList<>(), keys);
            if (keys.isEmpty() || keys.size() > MAX_POINT_LOOKUP_KEYS) {
                return null;
            }
            return new PointLookup(keys, andOf(residual, rexBuilder));
        }

        /** 按 PK 列顺序对各列候选值做笛卡尔积,超阈值剪枝(由调用方判定回退)。 */
        private static void cartesianKeys(Map<Integer, List<Object>> valuesByCol,
                                          TableSchema schema, int pkPos, List<Object> prefix,
                                          List<List<Object>> out) {
            if (out.size() > MAX_POINT_LOOKUP_KEYS) {
                return;
            }
            List<String> pk = schema.primaryKey();
            if (pkPos == pk.size()) {
                out.add(List.copyOf(prefix));
                return;
            }
            for (Object v : valuesByCol.get(schema.columnIndex(pk.get(pkPos)))) {
                prefix.add(v);
                cartesianKeys(valuesByCol, schema, pkPos + 1, prefix, out);
                prefix.remove(prefix.size() - 1);
            }
        }

        /**
         * 提取「单列绑定值集」:单等值、SEARCH 点集(IN)、同列等值 OR 链。
         * 含范围/多列 OR/非等值 → 返回 null(留给 residual)。
         * (splitConjuncts/numericLiteralValue 与外层共用)。
         */
        private static BoundVals boundValues(RexNode node) {
            BoundEq eq = boundEquality(node);
            if (eq != null) {
                return new BoundVals(eq.colIndex, List.of(eq.value));
            }
            if (node instanceof RexCall call && call.getKind() == SqlKind.SEARCH) {
                RexNode l = call.getOperands().get(0);
                RexNode r = call.getOperands().get(1);
                if (l instanceof RexInputRef ref && r instanceof RexLiteral lit) {
                    Sarg<?> sarg = lit.getValueAs(Sarg.class);
                    if (sarg != null && sarg.isPoints()) {
                        List<Object> values = new ArrayList<>();
                        for (Range<?> range : sarg.rangeSet.asRanges()) {
                            Object v = pointValue(range.lowerEndpoint());
                            if (v == null) {
                                return null; // 非数值点(字符串等)→ 回退扫描
                            }
                            values.add(v);
                        }
                        return values.isEmpty() ? null : new BoundVals(ref.getIndex(), values);
                    }
                }
                return null;
            }
            if (node instanceof RexCall call && call.getKind() == SqlKind.OR) {
                // 同列等值 OR 链:col = v1 OR col = v2 ...;多列 OR 或含非等值 → 不可提取
                List<Object> values = new ArrayList<>();
                Integer colIndex = null;
                for (RexNode o : call.getOperands()) {
                    BoundEq e = boundEquality(o);
                    if (e == null) {
                        return null;
                    }
                    if (colIndex == null) {
                        colIndex = e.colIndex;
                    } else if (colIndex != e.colIndex) {
                        return null;
                    }
                    values.add(e.value);
                }
                return colIndex == null ? null : new BoundVals(colIndex, values);
            }
            return null;
        }

        /** 若条件是 `col = literal` 或 `literal = col`(整数型字面量),返回绑定;否则 null。 */
        private static BoundEq boundEquality(RexNode node) {
            if (!(node instanceof RexCall call) || call.getKind() != SqlKind.EQUALS) {
                return null;
            }
            RexNode l = call.getOperands().get(0);
            RexNode r = call.getOperands().get(1);
            RexInputRef ref;
            RexLiteral lit;
            if (l instanceof RexInputRef a && r instanceof RexLiteral b) {
                ref = a;
                lit = b;
            } else if (l instanceof RexLiteral a && r instanceof RexInputRef b) {
                ref = b;
                lit = a;
            } else {
                return null;
            }
            Object value = numericLiteralValue(lit);
            return value == null ? null : new BoundEq(ref.getIndex(), value);
        }

        /** Sarg 点值 → Integer/Long;非数值返回 null。 */
        private static Object pointValue(Object point) {
            if (!(point instanceof Number n)) {
                return null;
            }
            return n instanceof Long ? n.longValue() : n.intValue();
        }

        /** 残余条件合成 AND(0 条→null,1 条→原样)。 */
        private static RexNode andOf(List<RexNode> conjuncts, RexBuilder rexBuilder) {
            if (conjuncts.isEmpty()) {
                return null;
            }
            if (conjuncts.size() == 1) {
                return conjuncts.get(0);
            }
            // 复用 scan 的 cluster RexBuilder,保证类型工厂与计划一致
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, conjuncts);
        }
    }

    /** {@code col = literal} 的绑定结果:列索引 + 数值。 */
    private record BoundEq(int colIndex, Object value) {
    }

    /** 单列的绑定值集(等值/IN 点集/OR 链):列索引 + 候选值列表。 */
    private record BoundVals(int colIndex, List<Object> values) {
    }

    /**
     * 主键范围裁剪:从下推 filter 提取主键序上的闭区间 [lo, hi](元素 null = 该列无界),
     * 只读与区间相交的 SSTable 文件/块(超集语义:区间外行由原条件 residual 过滤,
     * 正确性不依赖本裁剪)。约束按主键列顺序消费:每列取等值(优先,与范围并存时以
     * 等值为准,范围由 residual 兜底)或下/上界的最大/最小值;遇到无约束列即停止
     * (后续列的约束无法表示为前缀区间)。仅 INTEGER/BIGINT 主键(与点查同因:
     * 块索引零填充编码只对 Integer/Long 保序)。
     */
    private static final class RangeBounds {
        final List<Object> lo;
        final List<Object> hi;

        private RangeBounds(List<Object> lo, List<Object> hi) {
            this.lo = lo;
            this.hi = hi;
        }

        static RangeBounds extract(RexNode filter, TableSchema schema) {
            List<String> pk = schema.primaryKey();
            if (pk.isEmpty() || filter == null) {
                return null;
            }
            for (String col : pk) {
                ColumnType t = schema.column(col).type();
                if (t != ColumnType.INTEGER && t != ColumnType.BIGINT) {
                    return null;
                }
            }
            List<RexNode> conjuncts = new ArrayList<>();
            splitConjuncts(filter, conjuncts);
            List<Object> lo = new ArrayList<>(Collections.nCopies(pk.size(), null));
            List<Object> hi = new ArrayList<>(Collections.nCopies(pk.size(), null));
            boolean anyBound = false;
            for (int i = 0; i < pk.size(); i++) {
                int colIdx = schema.columnIndex(pk.get(i));
                Object lower = null;
                Object upper = null;
                // 等值优先:该列若有等值,范围约束并入 residual(与等值矛盾时由 residual 兜底)
                for (RexNode c : conjuncts) {
                    BoundCmp bc = boundComparison(c);
                    if (bc != null && bc.colIndex == colIdx && bc.kind == SqlKind.EQUALS) {
                        lower = upper = bc.value;
                        break;
                    }
                }
                if (lower == null) {
                    for (RexNode c : conjuncts) {
                        BoundCmp bc = boundComparison(c);
                        if (bc == null || bc.colIndex != colIdx) {
                            continue;
                        }
                        switch (bc.kind) {
                            case GREATER_THAN -> lower = maxBound(lower, inc(bc.value));
                            case GREATER_THAN_OR_EQUAL -> lower = maxBound(lower, bc.value);
                            case LESS_THAN -> upper = minBound(upper, dec(bc.value));
                            case LESS_THAN_OR_EQUAL -> upper = minBound(upper, bc.value);
                            default -> {
                            }
                        }
                    }
                }
                if (lower == null && upper == null) {
                    break; // 该列无约束:后续列无法用于前缀区间裁剪
                }
                lo.set(i, lower);
                hi.set(i, upper);
                anyBound = true;
            }
            return anyBound ? new RangeBounds(lo, hi) : null;
        }

        /** 比较条件提取:`col OP literal`(支持反序,规范化为列在前)。返回 null 表示不可用。 */
        private static BoundCmp boundComparison(RexNode node) {
            if (!(node instanceof RexCall call) || call.getOperands().size() != 2) {
                // 一元/多参 call(IS NULL、NOT、CAST 等)不是范围比较
                return null;
            }
            SqlKind kind = call.getKind();
            RexNode l = call.getOperands().get(0);
            RexNode r = call.getOperands().get(1);
            RexInputRef ref;
            RexLiteral lit;
            boolean reversed;
            if (l instanceof RexInputRef a && r instanceof RexLiteral b) {
                ref = a;
                lit = b;
                reversed = false;
            } else if (l instanceof RexLiteral a && r instanceof RexInputRef b) {
                ref = b;
                lit = a;
                reversed = true;
            } else {
                return null;
            }
            SqlKind k = switch (kind) {
                case EQUALS -> SqlKind.EQUALS;
                case GREATER_THAN -> reversed ? SqlKind.LESS_THAN : SqlKind.GREATER_THAN;
                case GREATER_THAN_OR_EQUAL -> reversed
                        ? SqlKind.LESS_THAN_OR_EQUAL : SqlKind.GREATER_THAN_OR_EQUAL;
                case LESS_THAN -> reversed ? SqlKind.GREATER_THAN : SqlKind.LESS_THAN;
                case LESS_THAN_OR_EQUAL -> reversed
                        ? SqlKind.GREATER_THAN_OR_EQUAL : SqlKind.LESS_THAN_OR_EQUAL;
                default -> null;
            };
            if (k == null) {
                return null;
            }
            Object v = numericLiteralValue(lit);
            if (v == null) {
                return null;
            }
            return new BoundCmp(ref.getIndex(), k, v);
        }

        private static Object inc(Object v) {
            return v instanceof Long l ? l + 1 : ((Integer) v) + 1;
        }

        private static Object dec(Object v) {
            return v instanceof Long l ? l - 1 : ((Integer) v) - 1;
        }

        /** 下界取更严格(大)者;null 视为无约束。 */
        private static Object maxBound(Object a, Object b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return ((Number) a).longValue() >= ((Number) b).longValue() ? a : b;
        }

        /** 上界取更严格(小)者;null 视为无约束。 */
        private static Object minBound(Object a, Object b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return ((Number) a).longValue() <= ((Number) b).longValue() ? a : b;
        }
    }

    /** 比较条件:列索引 + 规范化 kind + 数值。 */
    private record BoundCmp(int colIndex, SqlKind kind, Object value) {
    }

    /**
     * Serializable 隔离级别:记录扫描的列到读集。
     * 键格式为 schema.table.column,按列粒度记录以支持 SSI 冲突检测。
     * 非事务或非 Serializable 时 TransactionManager.recordRead 内部短路,无开销。
     */
    private void recordReadSet(ExecContext ctx, String schemaName, String tableName) {
        if (!ctx.inTransaction()) return;
        MiniDbCalciteTable calciteTable = table.unwrap(MiniDbCalciteTable.class);
        if (calciteTable == null) return;
        TableSchema dataSchema = calciteTable.tableSchema();
        TransactionManager tm = ctx.storage().transactionManager();
        if (projectedColumns != null) {
            // 只记录实际读的列
            for (int colIdx : projectedColumns) {
                if (colIdx < dataSchema.columns().size()) {
                    String colName = dataSchema.columns().get(colIdx).name();
                    tm.recordRead(ctx.tx().txId(), schemaName + "." + tableName + "." + colName);
                }
            }
        } else {
            // 全列扫描:记录所有列
            for (ColumnMeta col : dataSchema.columns()) {
                tm.recordRead(ctx.tx().txId(), schemaName + "." + tableName + "." + col.name());
            }
        }
    }
}
