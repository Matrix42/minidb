package com.minidb.server.exec;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** 只读 information_schema 系统表:从内存 MiniDbCatalog 物化 schemata/tables/columns。 */
public final class InformationSchema {

    public static final String SCHEMA_NAME = "information_schema";

    private InformationSchema() {
    }

    public static boolean isSystemSchema(String name) {
        return SCHEMA_NAME.equalsIgnoreCase(name);
    }

    public static TableSchema schemataSchema() {
        return new TableSchema(SCHEMA_NAME, "schemata", List.of(
                new ColumnMeta("CATALOG_NAME", ColumnType.VARCHAR),
                new ColumnMeta("SCHEMA_NAME", ColumnType.VARCHAR),
                new ColumnMeta("SCHEMA_OWNER", ColumnType.VARCHAR),
                new ColumnMeta("DEFAULT_CHARACTER_SET_CATALOG", ColumnType.VARCHAR),
                new ColumnMeta("DEFAULT_CHARACTER_SET_SCHEMA", ColumnType.VARCHAR),
                new ColumnMeta("DEFAULT_CHARACTER_SET_NAME", ColumnType.VARCHAR),
                new ColumnMeta("SQL_PATH", ColumnType.VARCHAR)));
    }

    public static TableSchema tablesSchema() {
        return new TableSchema(SCHEMA_NAME, "tables", List.of(
                new ColumnMeta("TABLE_CATALOG", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_SCHEMA", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_NAME", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_TYPE", ColumnType.VARCHAR)));
    }

    public static TableSchema columnsSchema() {
        return new TableSchema(SCHEMA_NAME, "columns", List.of(
                new ColumnMeta("TABLE_CATALOG", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_SCHEMA", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_NAME", ColumnType.VARCHAR),
                new ColumnMeta("COLUMN_NAME", ColumnType.VARCHAR),
                new ColumnMeta("ORDINAL_POSITION", ColumnType.INTEGER),
                new ColumnMeta("DATA_TYPE", ColumnType.VARCHAR),
                new ColumnMeta("NUMERIC_PRECISION", ColumnType.INTEGER),
                new ColumnMeta("NUMERIC_SCALE", ColumnType.INTEGER)));
    }

    public static VectorSchemaRoot materialize(MiniDbCatalog catalog, String tableName,
                                               BufferAllocator allocator) {
        if (tableName.equalsIgnoreCase("tables")) {
            return materializeTables(catalog, allocator);
        }
        if (tableName.equalsIgnoreCase("columns")) {
            return materializeColumns(catalog, allocator);
        }
        if (tableName.equalsIgnoreCase("schemata")) {
            return materializeSchemata(catalog, allocator);
        }
        throw new IllegalArgumentException("unknown information_schema table: " + tableName);
    }

    private static VectorSchemaRoot materializeSchemata(MiniDbCatalog catalog, BufferAllocator allocator) {
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        int n = schemas.size();
        VarCharVector catalogName = vc("CATALOG_NAME", n, allocator);
        VarCharVector schemaName = vc("SCHEMA_NAME", n, allocator);
        VarCharVector schemaOwner = vc("SCHEMA_OWNER", n, allocator);
        VarCharVector charsetCatalog = vc("DEFAULT_CHARACTER_SET_CATALOG", n, allocator);
        VarCharVector charsetSchema = vc("DEFAULT_CHARACTER_SET_SCHEMA", n, allocator);
        VarCharVector charsetName = vc("DEFAULT_CHARACTER_SET_NAME", n, allocator);
        VarCharVector sqlPath = vc("SQL_PATH", n, allocator);
        for (int i = 0; i < n; i++) {
            schemaName.setSafe(i, schemas.get(i).getBytes(StandardCharsets.UTF_8));
        }
        // 其余列保持 null(不写值)
        for (VarCharVector v : new VarCharVector[]{catalogName, schemaName, schemaOwner,
                charsetCatalog, charsetSchema, charsetName, sqlPath}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(catalogName, schemaName, schemaOwner,
                charsetCatalog, charsetSchema, charsetName, sqlPath);
    }

    private static VectorSchemaRoot materializeTables(MiniDbCatalog catalog, BufferAllocator allocator) {
        List<String[]> rows = new ArrayList<>(); // [schema, table]
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        for (String schema : schemas) {
            List<String> names = new ArrayList<>(catalog.tableNames(schema));
            names.sort(String::compareTo);
            for (String name : names) {
                rows.add(new String[]{schema, name});
            }
        }
        int n = rows.size();
        VarCharVector tableCatalog = vc("TABLE_CATALOG", n, allocator);
        VarCharVector tableSchema = vc("TABLE_SCHEMA", n, allocator);
        VarCharVector tableName = vc("TABLE_NAME", n, allocator);
        VarCharVector tableType = vc("TABLE_TYPE", n, allocator);
        for (int i = 0; i < n; i++) {
            tableSchema.setSafe(i, rows.get(i)[0].getBytes(StandardCharsets.UTF_8));
            tableName.setSafe(i, rows.get(i)[1].getBytes(StandardCharsets.UTF_8));
            // 现在只有 base table;视图落地后在此按 kind 分支报 'VIEW'。
            tableType.setSafe(i, "BASE TABLE".getBytes(StandardCharsets.UTF_8));
        }
        for (VarCharVector v : new VarCharVector[]{tableCatalog, tableSchema, tableName, tableType}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(tableCatalog, tableSchema, tableName, tableType);
    }

    private static VectorSchemaRoot materializeColumns(MiniDbCatalog catalog, BufferAllocator allocator) {
        List<Object[]> rows = new ArrayList<>(); // [schema, table, col, ordinal, ColumnMeta]
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        for (String schema : schemas) {
            List<String> names = new ArrayList<>(catalog.tableNames(schema));
            names.sort(String::compareTo);
            for (String name : names) {
                TableSchema ts = catalog.getTable(schema, name);
                int ordinal = 1;
                for (ColumnMeta col : ts.columns()) {
                    rows.add(new Object[]{schema, name, col.name(), ordinal, col});
                    ordinal++;
                }
            }
        }
        int n = rows.size();
        VarCharVector tableCatalog = vc("TABLE_CATALOG", n, allocator);
        VarCharVector tableSchema = vc("TABLE_SCHEMA", n, allocator);
        VarCharVector tableName = vc("TABLE_NAME", n, allocator);
        VarCharVector columnName = vc("COLUMN_NAME", n, allocator);
        IntVector ordinal = new IntVector("ORDINAL_POSITION", allocator);
        ordinal.setInitialCapacity(n);
        ordinal.allocateNew();
        VarCharVector dataType = vc("DATA_TYPE", n, allocator);
        IntVector numericPrecision = new IntVector("NUMERIC_PRECISION", allocator);
        numericPrecision.setInitialCapacity(n);
        numericPrecision.allocateNew();
        IntVector numericScale = new IntVector("NUMERIC_SCALE", allocator);
        numericScale.setInitialCapacity(n);
        numericScale.allocateNew();
        for (int i = 0; i < n; i++) {
            Object[] r = rows.get(i);
            ColumnMeta col = (ColumnMeta) r[4];
            tableSchema.setSafe(i, ((String) r[0]).getBytes(StandardCharsets.UTF_8));
            tableName.setSafe(i, ((String) r[1]).getBytes(StandardCharsets.UTF_8));
            columnName.setSafe(i, col.name().getBytes(StandardCharsets.UTF_8));
            ordinal.setSafe(i, (Integer) r[3]);
            dataType.setSafe(i, ArrowTypes.toSqlTypeName(col.type())
                    .getBytes(StandardCharsets.UTF_8));
            if (col.type() == ColumnType.DECIMAL || col.type() == ColumnType.NUMERIC) {
                numericPrecision.setSafe(i, col.precision());
                numericScale.setSafe(i, col.scale());
            }
            // 非 decimal 列 precision/scale 保持 null
        }
        for (VarCharVector v : new VarCharVector[]{tableCatalog, tableSchema, tableName, columnName, dataType}) {
            v.setValueCount(n);
        }
        for (IntVector v : new IntVector[]{ordinal, numericPrecision, numericScale}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(tableCatalog, tableSchema, tableName, columnName,
                ordinal, dataType, numericPrecision, numericScale);
    }

    private static VarCharVector vc(String name, int capacity, BufferAllocator allocator) {
        VarCharVector v = new VarCharVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }
}
