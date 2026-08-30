package com.minidb.parser.ddl;

import org.apache.calcite.sql.SqlCreate;
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
 * Parse tree for {@code CREATE [UNIQUE] INDEX}.
 *
 * <p>Extends {@link SqlCreate} so the FMPP template's {@code createStatementParserMethods} (which
 * returns {@code SqlCreate}) can dispatch to this node. {@link SqlCreate} extends {@link
 * org.apache.calcite.sql.SqlDdl}, so {@code QueryExecutor.handleDdl} picks it up via {@code
 * instanceof SqlDdl}.
 */
public class SqlCreateIndex extends SqlCreate {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE INDEX", SqlKind.OTHER);

    private final boolean unique;
    private final SqlIdentifier indexName;
    private final SqlIdentifier table;
    private final SqlNodeList columnList;

    public SqlCreateIndex(
            SqlParserPos pos,
            boolean unique,
            SqlIdentifier indexName,
            SqlIdentifier table,
            SqlNodeList columnList) {
        super(OPERATOR, pos, false, false);
        this.unique = unique;
        this.indexName = indexName;
        this.table = table;
        this.columnList = columnList;
    }

    public boolean unique() {
        return unique;
    }

    public SqlIdentifier indexName() {
        return indexName;
    }

    public SqlIdentifier table() {
        return table;
    }

    public SqlNodeList columnList() {
        return columnList;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(indexName, table, columnList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (unique) {
            writer.keyword("UNIQUE");
        }
        writer.keyword("INDEX");
        indexName.unparse(writer, 0, 0);
        writer.keyword("ON");
        table.unparse(writer, 0, 0);
        columnList.unparse(writer, 1, 1);
    }

    @Override
    public SqlCreateIndex clone(SqlParserPos pos) {
        return new SqlCreateIndex(pos, unique, indexName, table, columnList);
    }
}
