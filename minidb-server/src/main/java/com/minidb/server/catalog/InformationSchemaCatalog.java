package com.minidb.server.catalog;

import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.TableSchema;

import java.util.List;

/** information_schema 系统 schema 的元数据:固定 schema 名 + 三张系统表定义。 */
public final class InformationSchemaCatalog {

    public static final String SCHEMA_NAME = "information_schema";

    private InformationSchemaCatalog() {}

    public static boolean isSystemSchema(String name) {
        return SCHEMA_NAME.equalsIgnoreCase(name);
    }

    public static List<TableSchema> tables() {
        return List.of(
                schemataSchema(), tablesSchema(), columnsSchema(), materializedViewsSchema());
    }

    public static TableSchema schemataSchema() {
        return new TableSchema(
                SCHEMA_NAME,
                "schemata",
                List.of(
                        new ColumnMeta("CATALOG_NAME", ColumnType.VARCHAR),
                        new ColumnMeta("SCHEMA_NAME", ColumnType.VARCHAR),
                        new ColumnMeta("SCHEMA_OWNER", ColumnType.VARCHAR),
                        new ColumnMeta("DEFAULT_CHARACTER_SET_CATALOG", ColumnType.VARCHAR),
                        new ColumnMeta("DEFAULT_CHARACTER_SET_SCHEMA", ColumnType.VARCHAR),
                        new ColumnMeta("DEFAULT_CHARACTER_SET_NAME", ColumnType.VARCHAR),
                        new ColumnMeta("SQL_PATH", ColumnType.VARCHAR)));
    }

    public static TableSchema tablesSchema() {
        return new TableSchema(
                SCHEMA_NAME,
                "tables",
                List.of(
                        new ColumnMeta("TABLE_CATALOG", ColumnType.VARCHAR),
                        new ColumnMeta("TABLE_SCHEMA", ColumnType.VARCHAR),
                        new ColumnMeta("TABLE_NAME", ColumnType.VARCHAR),
                        new ColumnMeta("TABLE_TYPE", ColumnType.VARCHAR)));
    }

    public static TableSchema columnsSchema() {
        return new TableSchema(
                SCHEMA_NAME,
                "columns",
                List.of(
                        new ColumnMeta("TABLE_CATALOG", ColumnType.VARCHAR),
                        new ColumnMeta("TABLE_SCHEMA", ColumnType.VARCHAR),
                        new ColumnMeta("TABLE_NAME", ColumnType.VARCHAR),
                        new ColumnMeta("COLUMN_NAME", ColumnType.VARCHAR),
                        new ColumnMeta("ORDINAL_POSITION", ColumnType.INTEGER),
                        new ColumnMeta("DATA_TYPE", ColumnType.VARCHAR),
                        new ColumnMeta("NUMERIC_PRECISION", ColumnType.INTEGER),
                        new ColumnMeta("NUMERIC_SCALE", ColumnType.INTEGER)));
    }

    public static TableSchema materializedViewsSchema() {
        return new TableSchema(
                SCHEMA_NAME,
                "materialized_views",
                List.of(
                        new ColumnMeta("MV_CATALOG", ColumnType.VARCHAR),
                        new ColumnMeta("MV_SCHEMA", ColumnType.VARCHAR),
                        new ColumnMeta("MV_NAME", ColumnType.VARCHAR),
                        new ColumnMeta("DEFINITION", ColumnType.VARCHAR),
                        new ColumnMeta("DEPENDENCIES", ColumnType.VARCHAR),
                        new ColumnMeta("IS_STALE", ColumnType.VARCHAR)));
    }
}
