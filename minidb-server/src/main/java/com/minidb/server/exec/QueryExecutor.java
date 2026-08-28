package com.minidb.server.exec;
import com.minidb.storage.common.BatchIterator;

import com.minidb.parser.ddl.SqlAlterTable;
import com.minidb.parser.ddl.SqlCreateIndex;
import com.minidb.parser.ddl.SqlDropIndex;
import com.minidb.parser.ddl.SqlForeignKeyConstraint;
import com.minidb.parser.ddl.SqlTableOptions;
import com.minidb.server.calcite.CalciteContext;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.ForeignKey;
import com.minidb.storage.common.IndexDef;
import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableHandle;
import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.storage.common.TableType;
import com.minidb.storage.common.MVDefinition;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.common.TableSchema;
import com.minidb.server.catalog.ViewDefinition;
import com.minidb.server.exec.RowCopier;
import com.minidb.server.plan.physical.MiniDbModify;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.plan.Planner;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.transaction.TxHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateTableLike;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.apache.calcite.sql.ddl.SqlDropView;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.ddl.SqlTruncateTable;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

public class QueryExecutor {

    private final MiniDbCatalog catalog;
    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final Planner planner;
    private final CalciteContext calcite;
    private final StatsManager stats;

    private final MVManager mvManager;

    public QueryExecutor(MiniDbCatalog catalog, StorageManager storage,
                         BufferAllocator allocator, StatsManager stats) {
        this.catalog = catalog;
        this.storage = storage;
        this.allocator = allocator;
        this.stats = stats;
        this.planner = new Planner(catalog);
        this.calcite = new CalciteContext(catalog);
        this.mvManager = new MVManager(catalog, storage, allocator, planner);
    }

    public MVManager mvManager() { return mvManager; }

    public StorageManager storage() {
        return storage;
    }

    public QueryResult execute(String sql) {
        return execute(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public QueryResult execute(String sql, String currentSchema) {
        QueryResult result = executeCursor(sql, currentSchema);
        if (result instanceof QueryResult.Cursor cursor) {
            return new QueryResult.Rows(cursor.handle().materialize());
        }
        return result;
    }

    /** Like {@link #execute}, but leaves SELECT results as an unmaterialized cursor for paging. */
    public QueryResult executeCursor(String sql, String currentSchema) {
        String trimmed = sql.strip();
        // ① 命令:ANALYZE/EXPLAIN/USE SCHEMA,Calcite 不解析,parse 前前缀拦截。
        QueryResult command = tryHandleCommand(trimmed, currentSchema);
        if (command != null) {
            return command;
        }
        SqlNode parsed = calcite.parse(trimmed);
        // ② DDL:CREATE/DROP/TRUNCATE,统一挂在 SqlDdl 基类下。
        if (parsed instanceof SqlDdl ddl) {
            return handleDdl(ddl, currentSchema);
        }
        // ③ DQL/DML:SELECT/INSERT/UPDATE/DELETE。
        return executeQuery(trimmed, currentSchema);
    }

    public QueryResult executeCursor(String sql) {
        return executeCursor(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    /** Like {@link #executeCursor(String, String)}, but runs inside a transaction. */
    public QueryResult executeCursor(String sql, String currentSchema, TxHandle tx) {
        String trimmed = sql.strip();
        // ① 命令:ANALYZE/EXPLAIN/USE SCHEMA,Calcite 不解析,parse 前前缀拦截。
        QueryResult command = tryHandleCommand(trimmed, currentSchema);
        if (command != null) {
            return command;
        }
        SqlNode parsed = calcite.parse(trimmed);
        // ② DDL:CREATE/DROP/TRUNCATE,统一挂在 SqlDdl 基类下。
        if (parsed instanceof SqlDdl ddl) {
            return handleDdl(ddl, currentSchema);
        }
        // ③ DQL/DML:SELECT/INSERT/UPDATE/DELETE — 走事务感知路径。
        return executeQuery(trimmed, currentSchema, tx);
    }

    /** 命令:Calcite 不解析(ANALYZE/EXPLAIN/USE SCHEMA),parse 前前缀拦截;非命令返回 null。 */
    private QueryResult tryHandleCommand(String trimmed, String currentSchema) {
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
        if (upper.startsWith("REFRESH MATERIALIZED VIEW ")) {
            String mvName = trimmed.substring("REFRESH MATERIALIZED VIEW ".length()).strip();
            int dot = mvName.indexOf('.');
            String schemaName = dot >= 0 ? mvName.substring(0, dot).strip() : currentSchema;
            String name = dot >= 0 ? mvName.substring(dot + 1).strip() : mvName;
            if (!catalog.hasMaterializedView(schemaName, name)) {
                throw new IllegalArgumentException("materialized view not found: " + mvName);
            }
            // 全量刷新：重新执行定义查询，TRUNCATE + INSERT
            MVDefinition mvDef = catalog.getMaterializedView(schemaName, name);
            RelNode plan = planner.plan(mvDef.querySql(), schemaName);
            TableHandle target = storage.getTable(schemaName, name);
            target.clearParts();
            ExecContext ctx = new ExecContext(storage, allocator, schemaName);
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
            } finally {
                ctx.close();
            }
            return new QueryResult.Update(0);
        }
        if (upper.startsWith("COMPACT TABLE ")) {
            String table = trimmed.substring("COMPACT TABLE ".length()).strip();
            int dot = table.indexOf('.');
            String schemaName = dot >= 0 ? table.substring(0, dot).strip() : currentSchema;
            String tableName = dot >= 0 ? table.substring(dot + 1).strip() : table;
            storage.compactTable(schemaName, tableName);
            return new QueryResult.Update(0);
        }
        return null;
    }

    /** DDL:CREATE/DROP/TRUNCATE 各类,统一挂在 Calcite 的 SqlDdl 基类下。 */
    private QueryResult handleDdl(SqlDdl ddl, String currentSchema) {
        if (ddl instanceof SqlCreateSchema create) {
            return handleCreateSchema(create);
        }
        if (ddl instanceof SqlDropSchema drop) {
            return handleDropSchema(drop);
        }
        if (ddl instanceof SqlCreateTable create) {
            return handleCreate(create, currentSchema);
        }
        if (ddl instanceof SqlCreateTableLike like) {
            return handleCreateLike(like, currentSchema);
        }
        if (ddl instanceof SqlCreateView create) {
            return handleCreateView(create, currentSchema);
        }
        if (ddl instanceof SqlCreateMaterializedView create) {
            return handleCreateMaterializedView(create, currentSchema);
        }
        if (ddl instanceof SqlDropTable drop) {
            return handleDrop(drop, currentSchema);
        }
        if (ddl instanceof SqlDropView drop) {
            return handleDropView(drop, currentSchema);
        }
        if (ddl instanceof SqlDropMaterializedView drop) {
            return handleDropMaterializedView(drop, currentSchema);
        }
        if (ddl instanceof SqlTruncateTable truncate) {
            return handleTruncate(truncate, currentSchema);
        }
        if (ddl instanceof SqlAlterTable alter) {
            return new AlterTableHandler(storage, allocator).handle(alter, currentSchema);
        }
        if (ddl instanceof SqlCreateIndex create) {
            return handleCreateIndex(create, currentSchema);
        }
        if (ddl instanceof SqlDropIndex drop) {
            return handleDropIndex(drop, currentSchema);
        }
        throw new IllegalArgumentException("unsupported DDL: " + ddl.getKind());
    }

    /** DQL/DML:SELECT/INSERT/UPDATE/DELETE,走 planner 规划 + 执行。 */
    private QueryResult executeQuery(String sql, String currentSchema) {
        return executeQuery(sql, currentSchema, null);
    }

    /** DQL/DML with transaction handle — used when running inside a transaction. */
    private QueryResult executeQuery(String sql, String currentSchema, TxHandle tx) {
        RelNode plan = planner.plan(sql, currentSchema);
        ExecContext ctx = new ExecContext(storage, allocator, currentSchema, tx);
        if (plan instanceof MiniDbModify modify) {
            try (BatchIterator it = modify.execute(ctx)) {
                while (it.hasNext()) {
                    it.next();
                }
                return new QueryResult.Update(modify.affected());
            }
        }
        BatchIterator it = ((MiniDbRel) plan).execute(ctx);
        return new QueryResult.Cursor(new CursorHandle(it, ctx, schemaFromRowType(plan.getRowType())));
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
        List<String> primaryKey = List.of();
        List<List<String>> uniqueKeys = new ArrayList<>();
        List<ForeignKey> foreignKeys = new ArrayList<>();
        StorageFormat storageFormat = StorageFormat.DEFAULT;
        TableType tableType = null;
        for (SqlNode node : create.columnList) {
            if (node instanceof SqlTableOptions options) {
                TableOptions opts = tableOptionsFromWith(options);
                storageFormat = opts.format();
                tableType = opts.tableType();
                continue;
            }
            if (node instanceof SqlKeyConstraint key) {
                // 表级/列级 PRIMARY KEY (col, ...) / UNIQUE (col, ...)
                SqlNodeList keyCols = (SqlNodeList) key.getOperandList().get(1);
                List<String> cols = new ArrayList<>();
                for (SqlNode col : keyCols) {
                    cols.add(((SqlIdentifier) col).getSimple());
                }
                if (key.getOperator().getKind() == SqlKind.PRIMARY_KEY) {
                    primaryKey = cols;
                } else {
                    uniqueKeys.add(cols);
                }
                continue;
            }
            if (node instanceof SqlForeignKeyConstraint fk) {
                // 表级 FOREIGN KEY / 列级 REFERENCES,由 minidb-parser 原生解析。
                List<String> cols = new ArrayList<>();
                for (SqlNode col : fk.getColumnList()) {
                    cols.add(((SqlIdentifier) col).getSimple());
                }
                List<String> refNames = fk.getRefTable().names;
                String refTable = refNames.get(refNames.size() - 1);
                String refSchema = refNames.size() > 1 ? refNames.get(0) : schemaName;
                List<String> refCols = new ArrayList<>();
                if (fk.getRefColumns() != null) {
                    for (SqlNode col : fk.getRefColumns()) {
                        refCols.add(((SqlIdentifier) col).getSimple());
                    }
                }
                foreignKeys.add(new ForeignKey(cols, refSchema, refTable, refCols));
                continue;
            }
            SqlColumnDeclaration column = (SqlColumnDeclaration) node;
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
            // dataType.getNullable() 为 FALSE 表示列级 NOT NULL;null/TRUE 均可空。
            boolean nullable = !Boolean.FALSE.equals(column.dataType.getNullable());
            columns.add(new ColumnMeta(column.name.getSimple(), type, precision, scale, nullable));
        }
        TableSchema schema = new TableSchema(schemaName, tableName, columns,
                primaryKey, uniqueKeys, foreignKeys, storageFormat, tableType, null, null);
        storage.createTable(schema);
        return new QueryResult.Update(0);
    }

    /** CREATE TABLE t LIKE src:复制源表的列与约束,可选 WITH 覆盖存储格式。 */
    private QueryResult handleCreateLike(SqlCreateTableLike like, String currentSchema) {
        List<String> targetParts = like.name.names;
        String targetSchema = targetParts.size() > 1 ? targetParts.get(0) : currentSchema;
        String targetName = targetParts.get(targetParts.size() - 1);
        List<String> sourceParts = like.sourceTable.names;
        String sourceSchema = sourceParts.size() > 1 ? sourceParts.get(0) : currentSchema;
        String sourceName = sourceParts.get(sourceParts.size() - 1);
        TableSchema source = catalog.getTable(sourceSchema, sourceName);
        if (source == null) {
            throw new IllegalArgumentException("source table not found: " + like.sourceTable);
        }
        // WITH 选项通过 includingOptions 列表传递(parser 借用该字段)。
        StorageFormat storageFormat = source.storageFormat();
        TableType tableType = source.tableType();
        for (SqlNode node : like.includingOptions) {
            if (node instanceof SqlTableOptions options) {
                TableOptions opts = tableOptionsFromWith(options);
                storageFormat = opts.format();
                tableType = opts.tableType();
            }
        }
        TableSchema target = new TableSchema(targetSchema, targetName,
                source.columns(), source.primaryKey(), source.uniqueKeys(),
                source.foreignKeys(), storageFormat, tableType, null, null);
        storage.createTable(target);
        return new QueryResult.Update(0);
    }

    /**
     * 从 WITH 子句取存储格式和表类型。支持 {@code format} 和 {@code type} 键,
     * 其余键明确拒绝。
     */
    private TableOptions tableOptionsFromWith(SqlTableOptions options) {
        StorageFormat format = StorageFormat.DEFAULT;
        TableType tableType = null;
        for (Map.Entry<String, SqlNode> e : options.options().entrySet()) {
            String key = e.getKey();
            if ("format".equalsIgnoreCase(key)) {
                if (!(e.getValue() instanceof SqlLiteral literal)
                        || literal.getTypeName() != SqlTypeName.CHAR) {
                    throw new IllegalArgumentException("format 值必须是字符串字面量");
                }
                format = StorageFormat.fromString(literal.getValueAs(String.class));
            } else if ("type".equalsIgnoreCase(key)) {
                if (!(e.getValue() instanceof SqlLiteral literal)
                        || literal.getTypeName() != SqlTypeName.CHAR) {
                    throw new IllegalArgumentException("type 值必须是字符串字面量");
                }
                String typeStr = literal.getValueAs(String.class);
                tableType = switch (typeStr.toLowerCase(java.util.Locale.ROOT)) {
                    case "lsm" -> TableType.LSM;
                    case "simple" -> TableType.SIMPLE;
                    default -> throw new IllegalArgumentException(
                            "unknown table type: " + typeStr + " (支持 lsm/simple)");
                };
            } else {
                throw new IllegalArgumentException(
                        "unknown table option: " + key + " (支持 format/type)");
            }
        }
        return new TableOptions(format, tableType);
    }

    private record TableOptions(StorageFormat format, TableType tableType) {}

    private QueryResult handleCreateView(SqlCreateView create, String currentSchema) {
        List<String> parts = create.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String viewName = parts.get(parts.size() - 1);
        // 把定义 SQL 规范化为 Calcite 方言文本(可被重新 parse,ViewTable 展开用)。
        String querySql = create.query.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
        // 在视图所在 schema 上下文 plan 定义 SQL,得到结果列名+类型存入 ViewDefinition。
        RelNode plan = planner.plan(querySql, schemaName);
        List<ColumnMeta> columns = columnsFromRowType(plan.getRowType());
        if (create.columnList != null && !create.columnList.isEmpty()) {
            columns = applyColumnList(columns, create.columnList);
        }
        ViewDefinition view = new ViewDefinition(
                schemaName, viewName, querySql, columns);
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

    private QueryResult handleCreateMaterializedView(SqlCreateMaterializedView create, String currentSchema) {
        List<String> parts = create.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String mvName = parts.get(parts.size() - 1);
        String querySql = create.query.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
        mvManager.createMV(schemaName, mvName, querySql);
        return new QueryResult.Update(0);
    }

    private QueryResult handleDropMaterializedView(SqlDropMaterializedView drop, String currentSchema) {
        List<String> parts = drop.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String mvName = parts.get(parts.size() - 1);
        if (!catalog.hasMaterializedView(schemaName, mvName)) {
            if (drop.ifExists) {
                return new QueryResult.Update(0);
            }
            throw new IllegalArgumentException("materialized view not found: " + mvName);
        }
        mvManager.dropMV(schemaName, mvName);
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

    /** CREATE VIEW v(a,b) 的显式列名列表:数量必须匹配查询输出列,按列表重命名(类型不变)。 */
    private static List<ColumnMeta> applyColumnList(List<ColumnMeta> columns,
                                                    List<SqlNode> columnList) {
        List<String> names = new ArrayList<>();
        for (SqlNode node : columnList) {
            names.add(((SqlIdentifier) node).getSimple());
        }
        if (names.size() != columns.size()) {
            throw new IllegalArgumentException("view column list has " + names.size()
                    + " columns but query produces " + columns.size());
        }
        List<ColumnMeta> renamed = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            ColumnMeta original = columns.get(i);
            renamed.add(new ColumnMeta(
                    names.get(i), original.type(), original.precision(), original.scale()));
        }
        return renamed;
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

    private QueryResult handleCreateIndex(SqlCreateIndex ddl, String currentSchema) {
        List<String> parts = ddl.table().names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        String indexName = ddl.indexName().getSimple();
        TableSchema data = catalog.getTable(schemaName, tableName);
        if (data == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        // 1. 主键必须存在
        if (data.primaryKey().isEmpty()) {
            throw new IllegalArgumentException(
                    "table without primary key cannot have index: " + tableName);
        }
        // 2. 表类型不能是 SIMPLE(SimpleTable 无点查能力,索引无意义)
        if (data.tableType() == TableType.SIMPLE) {
            throw new IllegalArgumentException(
                    "table type SIMPLE cannot have index: " + tableName);
        }
        // 3. 规范化列名(大小写不敏感)并校验列存在、类型、重复
        List<String> colNames = new ArrayList<>();
        for (SqlNode node : ddl.columnList()) {
            String raw = ((SqlIdentifier) node).getSimple();
            ColumnMeta cm = data.column(raw); // 不匹配会抛异常
            colNames.add(cm.name()); // 用规范名
        }
        // 4. 列类型白名单
        for (String col : colNames) {
            ColumnType type = data.column(col).type();
            if (type != ColumnType.SMALLINT && type != ColumnType.INTEGER
                    && type != ColumnType.BIGINT && type != ColumnType.VARCHAR) {
                throw new IllegalArgumentException(
                        "column type " + type + " not supported for index: " + col);
            }
        }
        // 5. 列不能重复
        if (colNames.stream().distinct().count() != colNames.size()) {
            throw new IllegalArgumentException(
                    "duplicate columns in index definition");
        }
        // 6. 索引名表内唯一(大小写不敏感)
        for (IndexDef existing : data.indexes()) {
            if (existing.name().equalsIgnoreCase(indexName)) {
                throw new IllegalArgumentException(
                        "index already exists: " + indexName + " on table " + tableName);
            }
        }
        // 7. 创建句柄 → 灌数据 → 落元数据;失败时清理半成品
        IndexDef def = new IndexDef(indexName, ddl.unique(), colNames);
        TableHandle dataTable = storage.getTable(schemaName, tableName);
        TableHandle indexTable = storage.indexManager().createIndex(
                schemaName, tableName, def, data);
        try {
            storage.indexManager().populateFromTable(
                    def, dataTable, indexTable);
        } catch (RuntimeException e) {
            // populate 失败(如存量数据违反 UNIQUE 约束),清理半成品再 rethrow
            storage.indexManager().dropIndex(schemaName, tableName, indexName);
            throw e;
        }
        // 追加索引到元数据并持久化
        List<IndexDef> updated = new ArrayList<>(data.indexes());
        updated.add(def);
        catalog.alterTable(schemaName, tableName, data.withIndexes(updated));
        return new QueryResult.Update(0);
    }

    private QueryResult handleDropIndex(SqlDropIndex ddl, String currentSchema) {
        List<String> parts = ddl.table().names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        String indexName = ddl.indexName().getSimple();
        TableSchema data = catalog.getTable(schemaName, tableName);
        if (data == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        // 按名(大小写不敏感)找 def
        IndexDef target = null;
        for (IndexDef def : data.indexes()) {
            if (def.name().equalsIgnoreCase(indexName)) {
                target = def;
                break;
            }
        }
        if (target == null) {
            if (ddl.ifExists()) {
                return new QueryResult.Update(0);
            }
            throw new IllegalArgumentException(
                    "index not found: " + indexName + " on table " + tableName);
        }
        // 删句柄 + 目录
        storage.indexManager().dropIndex(schemaName, tableName, target.name());
        // 从元数据去除此索引
        List<IndexDef> updated = new ArrayList<>(data.indexes());
        updated.remove(target);
        catalog.alterTable(schemaName, tableName, data.withIndexes(updated));
        return new QueryResult.Update(0);
    }

    private Schema schemaFromRowType(RelDataType rowType) {
        List<Field> fields = new ArrayList<>();
        for (RelDataTypeField f : rowType.getFieldList()) {
            fields.add(ArrowTypes.field(f));
        }
        return new Schema(fields);
    }

}
