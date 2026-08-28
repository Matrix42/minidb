package com.minidb.parser.ddl;

import java.util.List;

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER TABLE}. Calcite 无 ALTER TABLE 的 SqlNode 且
 * {@link SqlAlter} 不继承 {@link SqlDdl},故自定义并直接继承 {@link SqlDdl}
 * (这样 {@code QueryExecutor.handleDdl} 的 {@code instanceof SqlDdl} 能命中)。
 *
 * <p>一条语句只承载一个操作({@link AlterKind}),非 SqlNode 字段(nullable/constraintKind)
 * 不进 {@link #getOperandList}(该列表仅用于 unparse 序列化,DDL 节点不重新序列化)。
 */
public class SqlAlterTable extends SqlDdl {

    public enum AlterKind {
        ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, RENAME_TABLE, ALTER_TYPE,
        SET_NOT_NULL, DROP_NOT_NULL, ADD_CONSTRAINT, DROP_CONSTRAINT
    }

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("ALTER TABLE", SqlKind.OTHER);

    private final AlterKind kind;
    private final SqlIdentifier table;
    private final SqlIdentifier column;         // ADD/DROP/RENAME/ALTER 的目标列
    private final SqlIdentifier newColumn;      // RENAME COLUMN 的新列名
    private final SqlIdentifier newTable;       // RENAME TABLE 的新表名
    private final SqlDataTypeSpec dataType;     // ADD COLUMN / ALTER TYPE 的类型
    private final SqlNode defaultExpr;          // ADD COLUMN DEFAULT 常量(可 null)
    private final Boolean nullable;             // ADD COLUMN / SET/DROP NOT NULL 的可空性
    private final SqlIdentifier constraintName; // ADD/DROP CONSTRAINT 名(可 null)
    private final SqlKind constraintKind;       // ADD_CONSTRAINT: PRIMARY_KEY/UNIQUE/OTHER(外键)
    private final SqlNodeList columns;          // ADD_CONSTRAINT 本表列
    private final SqlIdentifier refTable;       // 外键引用表
    private final SqlNodeList refColumns;       // 外键引用列(可 null)

    public SqlAlterTable(SqlParserPos pos, AlterKind kind, SqlIdentifier table,
                         SqlIdentifier column, SqlIdentifier newColumn, SqlIdentifier newTable,
                         SqlDataTypeSpec dataType, SqlNode defaultExpr, Boolean nullable,
                         SqlIdentifier constraintName, SqlKind constraintKind,
                         SqlNodeList columns, SqlIdentifier refTable, SqlNodeList refColumns) {
        super(OPERATOR, pos);
        this.kind = kind;
        this.table = table;
        this.column = column;
        this.newColumn = newColumn;
        this.newTable = newTable;
        this.dataType = dataType;
        this.defaultExpr = defaultExpr;
        this.nullable = nullable;
        this.constraintName = constraintName;
        this.constraintKind = constraintKind;
        this.columns = columns;
        this.refTable = refTable;
        this.refColumns = refColumns;
    }

    public AlterKind kind() {
        return kind;
    }

    public SqlIdentifier table() {
        return table;
    }

    public SqlIdentifier column() {
        return column;
    }

    public SqlIdentifier newColumn() {
        return newColumn;
    }

    public SqlIdentifier newTable() {
        return newTable;
    }

    public SqlDataTypeSpec dataType() {
        return dataType;
    }

    public SqlNode defaultExpr() {
        return defaultExpr;
    }

    public Boolean nullable() {
        return nullable;
    }

    public SqlIdentifier constraintName() {
        return constraintName;
    }

    public SqlKind constraintKind() {
        return constraintKind;
    }

    public SqlNodeList columns() {
        return columns;
    }

    public SqlIdentifier refTable() {
        return refTable;
    }

    public SqlNodeList refColumns() {
        return refColumns;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(table, column, newColumn, newTable,
                dataType, defaultExpr, columns, refTable, refColumns, constraintName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER TABLE");
        table.unparse(writer, 0, 0);
        switch (kind) {
            case ADD_COLUMN -> {
                writer.keyword("ADD COLUMN");
                column.unparse(writer, 0, 0);
                dataType.unparse(writer, 0, 0);
                if (Boolean.FALSE.equals(nullable)) {
                    writer.keyword("NOT NULL");
                }
                if (defaultExpr != null) {
                    writer.keyword("DEFAULT");
                    defaultExpr.unparse(writer, 0, 0);
                }
            }
            case DROP_COLUMN -> {
                writer.keyword("DROP COLUMN");
                column.unparse(writer, 0, 0);
            }
            case RENAME_COLUMN -> {
                writer.keyword("RENAME COLUMN");
                column.unparse(writer, 0, 0);
                writer.keyword("TO");
                newColumn.unparse(writer, 0, 0);
            }
            case RENAME_TABLE -> {
                writer.keyword("RENAME TO");
                newTable.unparse(writer, 0, 0);
            }
            case ALTER_TYPE -> {
                writer.keyword("ALTER COLUMN");
                column.unparse(writer, 0, 0);
                writer.keyword("SET DATA TYPE");
                dataType.unparse(writer, 0, 0);
            }
            case SET_NOT_NULL -> {
                writer.keyword("ALTER COLUMN");
                column.unparse(writer, 0, 0);
                writer.keyword("SET NOT NULL");
            }
            case DROP_NOT_NULL -> {
                writer.keyword("ALTER COLUMN");
                column.unparse(writer, 0, 0);
                writer.keyword("DROP NOT NULL");
            }
            case ADD_CONSTRAINT -> {
                writer.keyword("ADD");
                if (constraintName != null) {
                    writer.keyword("CONSTRAINT");
                    constraintName.unparse(writer, 0, 0);
                }
                if (constraintKind == SqlKind.PRIMARY_KEY) {
                    writer.keyword("PRIMARY KEY");
                    columns.unparse(writer, 1, 1);
                } else if (constraintKind == SqlKind.UNIQUE) {
                    writer.keyword("UNIQUE");
                    columns.unparse(writer, 1, 1);
                } else {
                    writer.keyword("FOREIGN KEY");
                    columns.unparse(writer, 1, 1);
                    writer.keyword("REFERENCES");
                    refTable.unparse(writer, 0, 0);
                    if (refColumns != null) {
                        refColumns.unparse(writer, 1, 1);
                    }
                }
            }
            case DROP_CONSTRAINT -> {
                writer.keyword("DROP");
                if (constraintKind == SqlKind.PRIMARY_KEY) {
                    writer.keyword("PRIMARY KEY");
                } else {
                    writer.keyword("CONSTRAINT");
                    constraintName.unparse(writer, 0, 0);
                }
            }
        }
    }

    @Override
    public SqlCall clone(SqlParserPos pos) {
        return new SqlAlterTable(pos, kind, table, column, newColumn, newTable,
                dataType, defaultExpr, nullable, constraintName, constraintKind,
                columns, refTable, refColumns);
    }
}
