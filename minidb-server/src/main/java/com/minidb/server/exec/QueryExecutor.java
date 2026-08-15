package com.minidb.server.exec;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.catalog.ViewDefinition;
import com.minidb.server.plan.physical.MiniDbModify;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.plan.Planner;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlDropView;
import org.apache.calcite.sql.ddl.SqlTruncateTable;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

public class QueryExecutor {

    private final MiniDbCatalog catalog;
    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final Planner planner;
    private final CalciteContext calcite;
    private final StatsManager stats;

    public QueryExecutor(MiniDbCatalog catalog, StorageManager storage,
                         BufferAllocator allocator, StatsManager stats) {
        this.catalog = catalog;
        this.storage = storage;
        this.allocator = allocator;
        this.stats = stats;
        this.planner = new Planner(catalog);
        this.calcite = new CalciteContext(catalog);
    }

    public QueryResult execute(String sql) {
        return execute(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public QueryResult execute(String sql, String currentSchema) {
        String trimmed = sql.strip();
        String upper = trimmed.toUpperCase(Locale.ROOT);
        if (upper.equals("ANALYZE")) {
            stats.analyzeAll();
            return new QueryResult.Update(0);
        }
        if (upper.startsWith("ANALYZE ")) {
            String table = trimmed.substring("ANALYZE ".length()).strip();
            stats.analyze(table, currentSchema);
            return new QueryResult.Update(0);
        }
        if (upper.startsWith("EXPLAIN ANALYZE ")) {
            String inner = trimmed.substring("EXPLAIN ANALYZE ".length());
            return new ExplainExecutor(planner, stats, storage, allocator)
                    .analyze(inner, currentSchema);
        }
        if (upper.startsWith("EXPLAIN ")) {
            String inner = trimmed.substring("EXPLAIN ".length());
            return new ExplainExecutor(planner, stats, storage, allocator)
                    .explain(inner, currentSchema);
        }
        if (upper.startsWith("USE SCHEMA ")) {
            String name = trimmed.substring("USE SCHEMA ".length()).strip();
            String resolved = name.toLowerCase(Locale.ROOT);
            if (!catalog.schemaNames().contains(resolved)) {
                throw new IllegalArgumentException("schema not found: " + name);
            }
            return new QueryResult.UseSchema(resolved);
        }
        SqlNode parsed = calcite.parse(sql);
        if (parsed instanceof SqlCreateSchema create) {
            return handleCreateSchema(create);
        }
        if (parsed instanceof SqlDropSchema drop) {
            return handleDropSchema(drop);
        }
        if (parsed instanceof SqlCreateTable create) {
            return handleCreate(create, currentSchema);
        }
        if (parsed instanceof SqlCreateView create) {
            return handleCreateView(create, currentSchema);
        }
        if (parsed instanceof SqlDropTable drop) {
            return handleDrop(drop, currentSchema);
        }
        if (parsed instanceof SqlDropView drop) {
            return handleDropView(drop, currentSchema);
        }
        if (parsed instanceof SqlTruncateTable truncate) {
            return handleTruncate(truncate, currentSchema);
        }
        RelNode plan = planner.plan(sql, currentSchema);
        ExecContext ctx = new ExecContext(storage, allocator, currentSchema);
        if (plan instanceof MiniDbModify modify) {
            try (BatchIterator it = modify.execute(ctx)) {
                while (it.hasNext()) {
                    it.next();
                }
                return new QueryResult.Update(modify.affected());
            }
        }
        try (BatchIterator it = ((MiniDbRel) plan).execute(ctx)) {
            return new QueryResult.Rows(materialize(it, plan));
        }
    }

    private QueryResult handleCreateSchema(SqlCreateSchema create) {
        String name = create.name.getSimple();
        if (InformationSchemaCatalog.SCHEMA_NAME.equalsIgnoreCase(name)) {
            throw new IllegalArgumentException("reserved schema name: " + name);
        }
        if (create.ifNotExists
                && catalog.schemaNames().contains(name.toLowerCase(Locale.ROOT))) {
            return new QueryResult.Update(0);
        }
        catalog.createSchema(name);
        return new QueryResult.Update(0);
    }

    private QueryResult handleDropSchema(SqlDropSchema drop) {
        String name = drop.name.getSimple();
        if (InformationSchemaCatalog.SCHEMA_NAME.equalsIgnoreCase(name)) {
            throw new IllegalArgumentException("reserved schema name: " + name);
        }
        if (drop.ifExists
                && !catalog.schemaNames().contains(name.toLowerCase(Locale.ROOT))) {
            return new QueryResult.Update(0);
        }
        storage.dropSchema(name);
        return new QueryResult.Update(0);
    }

    private QueryResult handleCreate(SqlCreateTable create, String currentSchema) {
        List<String> parts = create.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        List<ColumnMeta> columns = new ArrayList<>();
        for (SqlNode columnNode : create.columnList) {
            SqlColumnDeclaration column = (SqlColumnDeclaration) columnNode;
            String typeName = column.dataType.getTypeName().getSimple();
            ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
            int precision = ColumnMeta.PRECISION_UNSET;
            int scale = ColumnMeta.SCALE_UNSET;
            // precision/scale 只对 DECIMAL/NUMERIC 有意义(见 ColumnMeta 文档);
            // 其余类型(如 VARCHAR(20) 的 20 是长度、TIME(3) 的 3 是秒精度)保持 -1。
            if (type == ColumnType.DECIMAL || type == ColumnType.NUMERIC) {
                // Calcite 1.42 把 precision/scale 挂在 SqlBasicTypeNameSpec 上
                // (SqlDataTypeSpec 无 getPrecision/getScale);未指定时 precision 返回 -1、
                // scale 返回 RelDataType.SCALE_NOT_SPECIFIED(Integer.MIN_VALUE),
                // 这里按 <0 统一归一为 ColumnMeta 的 -1 哨兵,避免泄露 Calcite 的哨兵值。
                if (column.dataType.getTypeNameSpec() instanceof SqlBasicTypeNameSpec basicSpec) {
                    if (basicSpec.getPrecision() >= 0) {
                        precision = basicSpec.getPrecision();
                    }
                    if (basicSpec.getScale() >= 0) {
                        scale = basicSpec.getScale();
                    }
                }
            }
            columns.add(new ColumnMeta(column.name.getSimple(), type, precision, scale));
        }
        TableSchema schema = new TableSchema(schemaName, tableName, columns);
        storage.createTable(schema);
        return new QueryResult.Update(0);
    }

    private QueryResult handleCreateView(SqlCreateView create, String currentSchema) {
        List<String> parts = create.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String viewName = parts.get(parts.size() - 1);
        // 把定义 SQL 规范化为 Calcite 方言文本(可被重新 parse,ViewTable 展开用)。
        String querySql = create.query.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
        // 在视图所在 schema 上下文 plan 定义 SQL,得到结果列名+类型存入 ViewDefinition。
        // 注:CREATE VIEW v(a,b) 的显式列名列表暂不支持(直接忽略,取查询输出列名)。
        RelNode plan = planner.plan(querySql, schemaName);
        ViewDefinition view = new ViewDefinition(
                schemaName, viewName, querySql, columnsFromRowType(plan.getRowType()));
        if (create.getReplace()) {
            catalog.replaceView(view);
        } else {
            catalog.createView(view);
        }
        return new QueryResult.Update(0);
    }

    private QueryResult handleDropView(SqlDropView drop, String currentSchema) {
        List<String> parts = drop.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String viewName = parts.get(parts.size() - 1);
        if (!catalog.hasView(schemaName, viewName)) {
            if (drop.ifExists) {
                return new QueryResult.Update(0);
            }
            throw new IllegalArgumentException("view not found: " + viewName);
        }
        catalog.dropView(schemaName, viewName);
        return new QueryResult.Update(0);
    }

    /** 从查询结果的 rowType 提取列定义(视图存储用)。DECIMAL/NUMERIC 保留 precision/scale。 */
    private static List<ColumnMeta> columnsFromRowType(RelDataType rowType) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            ColumnType type = ArrowTypes.fromSqlTypeName(
                    field.getType().getSqlTypeName().getName());
            if (type == ColumnType.DECIMAL || type == ColumnType.NUMERIC) {
                int precision = field.getType().getPrecision();
                int scale = field.getType().getScale();
                columns.add(new ColumnMeta(field.getName(), type,
                        precision >= 0 ? precision : ColumnMeta.PRECISION_UNSET,
                        scale >= 0 ? scale : ColumnMeta.SCALE_UNSET));
            } else {
                columns.add(new ColumnMeta(field.getName(), type));
            }
        }
        return columns;
    }

    private QueryResult handleDrop(SqlDropTable drop, String currentSchema) {
        List<String> parts = drop.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        if (!catalog.hasTable(schemaName, tableName)) {
            if (drop.ifExists) {
                return new QueryResult.Update(0);
            }
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        storage.dropTable(schemaName, tableName);
        return new QueryResult.Update(0);
    }

    private QueryResult handleTruncate(SqlTruncateTable truncate, String currentSchema) {
        List<String> parts = truncate.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        storage.truncateTable(schemaName, tableName);
        return new QueryResult.Update(0);
    }

    private VectorSchemaRoot materialize(BatchIterator it, RelNode plan) {
        VectorSchemaRoot merged = null;
        int dst = 0;
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            if (merged == null) {
                merged = VectorSchemaRoot.create(batch.getSchema(), allocator);
                merged.allocateNew();
            }
            for (int i = 0; i < batch.getRowCount(); i++) {
                RowCopier.copyRow(batch, i, merged, dst++);
            }
        }
        if (merged == null) {
            // No batches were produced (e.g. SELECT over an empty table, or a
            // filter that matched nothing). Build an empty root carrying the
            // plan's schema so the result still describes its columns.
            return emptyRoot(plan);
        }
        merged.setRowCount(dst);
        return merged;
    }

    private VectorSchemaRoot emptyRoot(RelNode plan) {
        List<Field> fields = new ArrayList<>();
        for (RelDataTypeField f : plan.getRowType().getFieldList()) {
            fields.add(ArrowTypes.field(f));
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(fields), allocator);
        root.allocateNew();
        root.setRowCount(0);
        return root;
    }
}
