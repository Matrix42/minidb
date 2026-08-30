package com.minidb.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code FOREIGN KEY (col, ...) REFERENCES refTable(refCol, ...)} 或列级 {@code col
 * TYPE REFERENCES refTable(refCol)} 约束。Calcite 无外键 DDL SqlNode,故自定义之。
 */
public class SqlForeignKeyConstraint extends SqlCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("FOREIGN KEY", SqlKind.OTHER);

    private final SqlIdentifier name; // 可为 null
    private final SqlNodeList columnList; // 本表列
    private final SqlIdentifier refTable; // 引用表名(可为 schema.table)
    private final SqlNodeList refColumns; // 引用表列,可为 null(引用主键)

    public SqlForeignKeyConstraint(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlNodeList columnList,
            SqlIdentifier refTable,
            SqlNodeList refColumns) {
        super(pos);
        this.name = name;
        this.columnList = columnList;
        this.refTable = refTable;
        this.refColumns = refColumns;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public SqlIdentifier getRefTable() {
        return refTable;
    }

    public SqlNodeList getRefColumns() {
        return refColumns;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, refTable, refColumns);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (name != null) {
            writer.keyword("CONSTRAINT");
            name.unparse(writer, 0, 0);
        }
        writer.keyword("FOREIGN KEY");
        columnList.unparse(writer, 1, 1);
        writer.keyword("REFERENCES");
        refTable.unparse(writer, 0, 0);
        if (refColumns != null) {
            refColumns.unparse(writer, 1, 1);
        }
    }
}
