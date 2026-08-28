package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbAggregate;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.plan.physical.MiniDbScan;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.*;

public class MVManager {

    private final MiniDbCatalog catalog;
    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final Planner planner;

    public MVManager(MiniDbCatalog catalog, StorageManager storage,
                     BufferAllocator allocator, Planner planner) {
        this.catalog = catalog;
        this.storage = storage;
        this.allocator = allocator;
        this.planner = planner;
    }

    public Set<String> getDependentMVs(String schemaName, String tableName) {
        return catalog.getDependentMVs(schemaName, tableName);
    }

    /** 创建物化视图：plan → 提取结构 → 建表 → 全量填充 → 写 catalog */
    public MVDefinition createMV(String schemaName, String mvName, String querySql) {
        // 1. 规划查询，提取结构
        MVStructure structure = extractMVStructure(querySql, schemaName);
        RelNode plan = planner.plan(querySql, schemaName);

        // 2. 提取依赖表
        List<MVDefinition.TableRef> deps = extractDependencies(plan);

        // 3. 提取输出列
        List<ColumnMeta> columns = columnsFromRowType(plan.getRowType());

        // 4. 构建 MVDefinition
        MVDefinition mvDef = new MVDefinition(schemaName, mvName, querySql,
                columns, deps, structure);

        // 5. 创建存储表
        TableSchema ts = new TableSchema(schemaName, mvName, columns,
                List.of(), List.of(), List.of(), StorageFormat.DEFAULT,
                TableType.MATERIALIZED_VIEW, null, mvDef);
        storage.createTable(ts);

        // 6. 全量填充
        ExecContext ctx = new ExecContext(storage, allocator, schemaName);
        try {
            TableHandle target = storage.getTable(schemaName, mvName);
            try (BatchIterator it = ((MiniDbRel) plan).execute(ctx)) {
                while (it.hasNext()) {
                    VectorSchemaRoot batch = it.next();
                    VectorSchemaRoot copy = target.newBatchRoot();
                    copy.allocateNew();
                    for (int i = 0; i < batch.getRowCount(); i++) {
                        RowCopier.copyRow(batch, i, copy, i);
                    }
                    copy.setRowCount(batch.getRowCount());
                    try {
                        target.writePart(copy, TableHandle.Operation.INSERT);
                    } finally {
                        copy.close();
                    }
                }
            }
        } finally {
            ctx.close();
        }

        // 7. 写 catalog
        catalog.createMaterializedView(mvDef);
        return mvDef;
    }

    /** 删除物化视图 */
    public void dropMV(String schemaName, String mvName) {
        storage.dropTable(schemaName, mvName);
        catalog.dropMaterializedView(schemaName, mvName);
    }

    /** 从 RelNode 树提取 MVStructure */
    public MVStructure extractMVStructure(String sql, String currentSchema) {
        RelNode plan = planner.plan(sql, currentSchema);

        // 聚合路径
        if (plan instanceof MiniDbAggregate agg) {
            RelNode input = agg.getInput();
            if (findSingleScan(input) == null) {
                throw new UnsupportedOperationException(
                        "物化视图仅支持单表聚合，不支持 JOIN");
            }

            List<String> outputCols = new ArrayList<>();
            for (RelDataTypeField f : plan.getRowType().getFieldList()) {
                outputCols.add(f.getName());
            }

            List<String> groupByCols = new ArrayList<>();
            for (int g : agg.getGroupSet()) {
                groupByCols.add(agg.getInput().getRowType()
                        .getFieldList().get(g).getName());
            }

            List<MVStructure.AggFunc> funcs = new ArrayList<>();
            for (AggregateCall call : agg.getAggCallList()) {
                String funcName = call.getAggregation().getKind().name();
                MVStructure.AggType aggType = switch (funcName) {
                    case "SUM" -> MVStructure.AggType.SUM;
                    case "COUNT" -> MVStructure.AggType.COUNT;
                    case "AVG" -> MVStructure.AggType.AVG;
                    case "MIN" -> MVStructure.AggType.MIN;
                    case "MAX" -> MVStructure.AggType.MAX;
                    default -> throw new UnsupportedOperationException(
                            "不支持的聚合函数: " + funcName);
                };
                String inputCol = agg.getInput().getRowType()
                        .getFieldList().get(call.getArgList().get(0)).getName();
                funcs.add(new MVStructure.AggFunc(call.name, aggType, inputCol));
            }

            return new MVStructure.Aggregate(sql, outputCols, groupByCols, funcs);
        }

        // SPJ 路径
        MiniDbScan scan = findSingleScan(plan);
        if (scan == null) {
            throw new UnsupportedOperationException(
                    "物化视图仅支持单表 SPJ 或聚合查询");
        }

        List<String> outputCols = new ArrayList<>();
        for (RelDataTypeField f : plan.getRowType().getFieldList()) {
            outputCols.add(f.getName());
        }

        return new MVStructure.Spj(sql, outputCols);
    }

    /** 在 RelNode 树中找到唯一的 MiniDbScan，null 表示没有或存在多个 */
    private static MiniDbScan findSingleScan(RelNode node) {
        if (node instanceof MiniDbScan scan) return scan;
        List<MiniDbScan> scans = new ArrayList<>();
        collectScans(node, scans);
        return scans.size() == 1 ? scans.get(0) : null;
    }

    private static void collectScans(RelNode node, List<MiniDbScan> out) {
        if (node instanceof MiniDbScan scan) {
            out.add(scan);
            return;
        }
        for (RelNode input : node.getInputs()) {
            collectScans(input, out);
        }
    }

    /** 提取依赖表列表 */
    private static List<MVDefinition.TableRef> extractDependencies(RelNode plan) {
        List<MVDefinition.TableRef> deps = new ArrayList<>();
        List<MiniDbScan> scans = new ArrayList<>();
        collectScans(plan, scans);
        for (MiniDbScan scan : scans) {
            List<String> qualified = scan.getTable().getQualifiedName();
            int n = qualified.size();
            String schemaName = n >= 3 ? qualified.get(n - 2) : "public";
            String tableName = qualified.get(n - 1);
            deps.add(new MVDefinition.TableRef(schemaName, tableName));
        }
        return deps;
    }

    private static List<ColumnMeta> columnsFromRowType(
            org.apache.calcite.rel.type.RelDataType rowType) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            ColumnType type = ArrowTypes.fromSqlTypeName(
                    field.getType().getSqlTypeName().getName());
            columns.add(new ColumnMeta(field.getName(), type));
        }
        return columns;
    }
}