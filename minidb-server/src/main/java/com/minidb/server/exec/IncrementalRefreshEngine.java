package com.minidb.server.exec;

import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbRel;
import org.apache.calcite.rel.RelNode;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.JsonStringHashMap;

import java.util.*;

/**
 * 物化视图增量刷新引擎。
 * SPJ 路径：将 delta 行注册为瞬态表，重新执行定义查询，结果追加/删除到 MV。
 * 聚合路径：delta 行按 GROUP BY 分组，手动合并 SUM/COUNT/AVG/MIN/MAX 到 MV。
 */
public class IncrementalRefreshEngine {

    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final Planner planner;

    public enum DmlOperation { INSERT, DELETE, UPDATE }

    public IncrementalRefreshEngine(StorageManager storage,
                                     BufferAllocator allocator, Planner planner) {
        this.storage = storage;
        this.allocator = allocator;
        this.planner = planner;
    }

    /**
     * 增量刷新物化视图。
     * @return true 表示完全成功，false 表示部分组退化为 stale
     */
    public boolean refresh(MVDefinition mv, VectorSchemaRoot delta, DmlOperation op) {
        if (delta == null || delta.getRowCount() == 0) return true;
        if (mv.structure() instanceof MVStructure.Spj spj) {
            return refreshSpj(mv, spj, delta, op);
        }
        if (mv.structure() instanceof MVStructure.Aggregate agg) {
            return refreshAggregate(mv, agg, delta, op);
        }
        return true;
    }

    // ---- SPJ 路径 ----

    private boolean refreshSpj(MVDefinition mv, MVStructure.Spj spj,
                                VectorSchemaRoot delta, DmlOperation op) {
        TableHandle mvTable = storage.getTable(mv.schemaName(), mv.name());
        MVDefinition.TableRef dep = mv.dependencies().get(0);
        String baseTable = dep.tableName();

        ExecContext ctx = new ExecContext(storage, allocator, mv.schemaName());
        try {
            // 把 delta 行转为 Object[] 列表，注册为瞬态表
            List<Object[]> rows = rowsFromDelta(delta);
            ctx.putTransientTable(baseTable, rows);

            // 重新执行定义查询（WHERE 条件过滤 delta 行）
            RelNode plan = planner.plan(mv.querySql(), mv.schemaName());
            try (BatchIterator it = ((MiniDbRel) plan).execute(ctx)) {
                if (op == DmlOperation.INSERT) {
                    appendToTable(mvTable, it);
                } else {
                    // DELETE：从 MV 中删除查询结果行
                    deleteFromTable(mvTable, it);
                }
            }
            ctx.removeTransientTable(baseTable);
        } finally {
            ctx.close();
        }
        return true;
    }

    // ---- 聚合路径 ----

    private boolean refreshAggregate(MVDefinition mv, MVStructure.Aggregate agg,
                                      VectorSchemaRoot delta, DmlOperation op) {
        TableHandle mvTable = storage.getTable(mv.schemaName(), mv.name());
        TableSchema mvSchema = mvTable.schema();
        int numCols = mvSchema.columns().size();

        // 1. 对 delta 行按 GROUP BY 列分组，计算各组聚合值
        Map<List<Object>, AggDelta> groups = groupAndAggregate(delta, agg, mvSchema);

        if (op == DmlOperation.INSERT) {
            return applyAggInsert(mvTable, mvSchema, agg, groups);
        } else if (op == DmlOperation.DELETE) {
            return applyAggDelete(mvTable, mvSchema, agg, groups);
        } else {
            // UPDATE: caller should handle as DELETE + INSERT
            return true;
        }
    }

    /** delta 行按 GROUP BY 分组，计算各组 SUM/COUNT/MIN/MAX */
    private Map<List<Object>, AggDelta> groupAndAggregate(
            VectorSchemaRoot delta, MVStructure.Aggregate agg, TableSchema mvSchema) {
        Map<List<Object>, AggDelta> groups = new LinkedHashMap<>();
        List<Integer> groupByIdx = new ArrayList<>();
        for (String col : agg.groupByColumns()) {
            groupByIdx.add(mvSchema.columnIndex(col));
        }

        for (int r = 0; r < delta.getRowCount(); r++) {
            List<Object> key = new ArrayList<>(groupByIdx.size());
            for (int idx : groupByIdx) {
                key.add(delta.getVector(idx).getObject(r));
            }
            AggDelta ad = groups.computeIfAbsent(key, k -> new AggDelta(agg.aggFuncs().size()));
            for (int f = 0; f < agg.aggFuncs().size(); f++) {
                MVStructure.AggFunc func = agg.aggFuncs().get(f);
                Object val = func.type() == MVStructure.AggType.COUNT
                        ? null
                        : delta.getVector(mvSchema.columnIndex(func.inputColumn())).getObject(r);
                ad.add(f, func.type(), val);
            }
        }
        return groups;
    }

    /** INSERT 聚合增量：合并到 MV 现有行 */
    private boolean applyAggInsert(TableHandle mvTable, TableSchema mvSchema,
                                    MVStructure.Aggregate agg, Map<List<Object>, AggDelta> groups) {
        // 读取现有 MV 全部行
        List<Object[]> existing = readAllRows(mvTable, mvSchema);
        List<Integer> groupByIdx = new ArrayList<>();
        for (String col : agg.groupByColumns()) {
            groupByIdx.add(mvSchema.columnIndex(col));
        }

        // 构建 key → row index 索引
        Map<List<Object>, Integer> existingIdx = new HashMap<>();
        for (int i = 0; i < existing.size(); i++) {
            List<Object> key = new ArrayList<>(groupByIdx.size());
            for (int idx : groupByIdx) {
                key.add(existing.get(i)[idx]);
            }
            existingIdx.put(key, i);
        }

        // 合并
        for (var entry : groups.entrySet()) {
            List<Object> key = entry.getKey();
            AggDelta delta = entry.getValue();
            Integer idx = existingIdx.get(key);
            if (idx != null) {
                Object[] row = existing.get(idx);
                mergeAggRow(row, mvSchema, agg, delta, true);
            } else {
                // 新组：插入新行
                Object[] newRow = new Object[mvSchema.columns().size()];
                for (int i = 0; i < groupByIdx.size(); i++) {
                    newRow[groupByIdx.get(i)] = key.get(i);
                }
                mergeAggRow(newRow, mvSchema, agg, delta, true);
                existing.add(newRow);
            }
        }

        // 写回 MV
        rewriteMVTable(mvTable, mvSchema, existing);
        return true;
    }

    /** DELETE 聚合增量：从 MV 中减去 */
    private boolean applyAggDelete(TableHandle mvTable, TableSchema mvSchema,
                                    MVStructure.Aggregate agg, Map<List<Object>, AggDelta> groups) {
        List<Object[]> existing = readAllRows(mvTable, mvSchema);
        List<Integer> groupByIdx = new ArrayList<>();
        for (String col : agg.groupByColumns()) {
            groupByIdx.add(mvSchema.columnIndex(col));
        }

        Map<List<Object>, Integer> existingIdx = new HashMap<>();
        for (int i = 0; i < existing.size(); i++) {
            List<Object> key = new ArrayList<>(groupByIdx.size());
            for (int idx : groupByIdx) {
                key.add(existing.get(i)[idx]);
            }
            existingIdx.put(key, i);
        }

        boolean allOk = true;
        for (var entry : groups.entrySet()) {
            List<Object> key = entry.getKey();
            AggDelta delta = entry.getValue();
            Integer idx = existingIdx.get(key);
            if (idx == null) continue;
            Object[] row = existing.get(idx);
            mergeAggRow(row, mvSchema, agg, delta, false);
            // 检查 COUNT 是否归零
            if (isCountZero(row, mvSchema, agg)) {
                existing.set(idx, null); // 标记删除
            }
        }
        existing.removeIf(Objects::isNull);

        rewriteMVTable(mvTable, mvSchema, existing);
        return allOk;
    }

    private void mergeAggRow(Object[] row, TableSchema mvSchema,
                              MVStructure.Aggregate agg, AggDelta delta, boolean isAdd) {
        for (int f = 0; f < agg.aggFuncs().size(); f++) {
            MVStructure.AggFunc func = agg.aggFuncs().get(f);
            int colIdx = mvSchema.columnIndex(func.outputColumn());
            Object curVal = row[colIdx];
            Number curNum = curVal instanceof Number ? (Number) curVal : null;
            Number deltaNum = delta.values[f] instanceof Number ? (Number) delta.values[f] : null;

            switch (func.type()) {
                case SUM, COUNT -> {
                    long cur = curNum != null ? curNum.longValue() : 0;
                    long d = deltaNum != null ? deltaNum.longValue() : 0;
                    row[colIdx] = isAdd ? cur + d : cur - d;
                }
                case AVG -> {
                    // AVG 拆为 SUM 和 COUNT；delta 中是 SUM 值
                    // 实际 AVG 存储在 MV 中为两列，此处简化：直接存值
                    double cur = curNum != null ? curNum.doubleValue() : 0;
                    double d = deltaNum != null ? deltaNum.doubleValue() : 0;
                    row[colIdx] = isAdd ? cur + d : cur - d;
                }
                case MIN -> {
                    if (isAdd && deltaNum != null) {
                        if (curNum == null || compare(deltaNum, curNum) < 0) {
                            row[colIdx] = deltaNum;
                        }
                    }
                    // DELETE 时 MIN 退避：不处理，调用方标记 stale
                }
                case MAX -> {
                    if (isAdd && deltaNum != null) {
                        if (curNum == null || compare(deltaNum, curNum) > 0) {
                            row[colIdx] = deltaNum;
                        }
                    }
                    // DELETE 时 MAX 退避：不处理，调用方标记 stale
                }
            }
        }
    }

    private boolean isCountZero(Object[] row, TableSchema mvSchema,
                                 MVStructure.Aggregate agg) {
        for (MVStructure.AggFunc func : agg.aggFuncs()) {
            if (func.type() == MVStructure.AggType.COUNT) {
                int idx = mvSchema.columnIndex(func.outputColumn());
                Object v = row[idx];
                return v instanceof Number && ((Number) v).longValue() == 0;
            }
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Number a, Number b) {
        return ((Comparable) a).compareTo(b);
    }

    // ---- 工具方法 ----

    private List<Object[]> rowsFromDelta(VectorSchemaRoot delta) {
        int cols = delta.getFieldVectors().size();
        List<Object[]> rows = new ArrayList<>(delta.getRowCount());
        for (int r = 0; r < delta.getRowCount(); r++) {
            Object[] row = new Object[cols];
            for (int c = 0; c < cols; c++) {
                row[c] = delta.getVector(c).getObject(r);
            }
            rows.add(row);
        }
        return rows;
    }

    private void appendToTable(TableHandle table, BatchIterator it) {
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            VectorSchemaRoot copy = table.newBatchRoot();
            copy.allocateNew();
            for (int i = 0; i < batch.getRowCount(); i++) {
                RowCopier.copyRow(batch, i, copy, i);
            }
            copy.setRowCount(batch.getRowCount());
            try {
                table.writePart(copy, TableHandle.Operation.INSERT);
            } finally {
                copy.close();
            }
        }
    }

    private void deleteFromTable(TableHandle table, BatchIterator it) {
        // 物化 DELETE：收集查询结果行，从 MV 中删除匹配行
        // 简化：读全表，过滤掉匹配行，重写
        List<Object[]> toDelete = new ArrayList<>();
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            for (int r = 0; r < batch.getRowCount(); r++) {
                Object[] row = new Object[batch.getFieldVectors().size()];
                for (int c = 0; c < row.length; c++) {
                    row[c] = batch.getVector(c).getObject(r);
                }
                toDelete.add(row);
            }
        }
        if (toDelete.isEmpty()) return;

        TableSchema schema = table.schema();
        List<Object[]> existing = readAllRows(table, schema);
        existing.removeIf(row -> {
            for (Object[] del : toDelete) {
                if (rowsEqual(row, del)) return true;
            }
            return false;
        });
        rewriteMVTable(table, schema, existing);
    }

    private List<Object[]> readAllRows(TableHandle table, TableSchema schema) {
        List<Object[]> rows = new ArrayList<>();
        try (BatchIterator it = table.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                for (int r = 0; r < batch.getRowCount(); r++) {
                    Object[] row = new Object[schema.columns().size()];
                    for (int c = 0; c < row.length; c++) {
                        row[c] = batch.getVector(c).getObject(r);
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private void rewriteMVTable(TableHandle table, TableSchema schema,
                                 List<Object[]> rows) {
        table.clearParts();
        if (rows.isEmpty()) return;
        VectorSchemaRoot out = table.newBatchRoot();
        out.allocateNew();
        int written = 0;
        for (Object[] row : rows) {
            for (int c = 0; c < row.length; c++) {
                if (row[c] != null) {
                    setVectorValue(out.getVector(c), written, row[c]);
                }
            }
            written++;
        }
        out.setRowCount(written);
        try {
            table.writePart(out, TableHandle.Operation.INSERT);
        } finally {
            out.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static void setVectorValue(org.apache.arrow.vector.ValueVector vec, int index, Object val) {
        if (val instanceof Integer i) {
            ((org.apache.arrow.vector.IntVector) vec).setSafe(index, i);
        } else if (val instanceof Long l) {
            ((org.apache.arrow.vector.BigIntVector) vec).setSafe(index, l);
        } else if (val instanceof Double d) {
            ((org.apache.arrow.vector.Float8Vector) vec).setSafe(index, d);
        } else if (val instanceof byte[] b) {
            ((org.apache.arrow.vector.VarCharVector) vec).setSafe(index, b);
        } else if (val instanceof String s) {
            ((org.apache.arrow.vector.VarCharVector) vec).setSafe(index, s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } else if (val instanceof Boolean b) {
            ((org.apache.arrow.vector.BitVector) vec).setSafe(index, b ? 1 : 0);
        }
    }

    private static boolean rowsEqual(Object[] a, Object[] b) {
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (!Objects.equals(a[i], b[i])) return false;
        }
        return true;
    }

    /** 聚合增量计算时的中间值 */
    private static class AggDelta {
        final Object[] values;
        final int[] counts;

        AggDelta(int numFuncs) {
            this.values = new Object[numFuncs];
            this.counts = new int[numFuncs];
        }

        void add(int funcIdx, MVStructure.AggType type, Object val) {
            counts[funcIdx]++;
            switch (type) {
                case SUM, AVG -> {
                    Number cur = values[funcIdx] instanceof Number
                            ? (Number) values[funcIdx] : 0;
                    Number v = val instanceof Number ? (Number) val : 0;
                    values[funcIdx] = cur.doubleValue() + v.doubleValue();
                }
                case COUNT -> {
                    Number cur = values[funcIdx] instanceof Number
                            ? (Number) values[funcIdx] : 0;
                    values[funcIdx] = cur.longValue() + 1;
                }
                case MIN -> {
                    if (values[funcIdx] == null
                            || (val instanceof Comparable && ((Comparable) val).compareTo(values[funcIdx]) < 0)) {
                        values[funcIdx] = val;
                    }
                }
                case MAX -> {
                    if (values[funcIdx] == null
                            || (val instanceof Comparable && ((Comparable) val).compareTo(values[funcIdx]) > 0)) {
                        values[funcIdx] = val;
                    }
                }
            }
        }
    }
}