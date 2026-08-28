package com.minidb.parser.ddl;

import java.util.List;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code DROP INDEX [IF EXISTS]}.
 *
 * <p>Extends {@link SqlDrop} so the FMPP template's
 * {@code dropStatementParserMethods} (which returns {@code SqlDrop}) can
 * dispatch to this node.  {@link SqlDrop} extends {@link org.apache.calcite.sql.SqlDdl},
 * so {@code QueryExecutor.handleDdl} picks it up via {@code instanceof SqlDdl}.
 */
public class SqlDropIndex extends SqlDrop {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP INDEX", SqlKind.OTHER);

    private final boolean ifExists;
    private final SqlIdentifier indexName;
    private final SqlIdentifier table;

    public SqlDropIndex(SqlParserPos pos, boolean ifExists, SqlIdentifier indexName,
                        SqlIdentifier table) {
        super(OPERATOR, pos, ifExists);
        this.ifExists = ifExists;
        this.indexName = indexName;
        this.table = table;
    }

    public boolean ifExists() {
        return ifExists;
    }

    public SqlIdentifier indexName() {
        return indexName;
    }

    public SqlIdentifier table() {
        return table;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(indexName, table);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("INDEX");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        indexName.unparse(writer, 0, 0);
        writer.keyword("ON");
        table.unparse(writer, 0, 0);
    }

    @Override
    public SqlDropIndex clone(SqlParserPos pos) {
        return new SqlDropIndex(pos, ifExists, indexName, table);
    }
}