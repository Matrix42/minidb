package com.minidb.parser.ddl;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Parse tree for {@code FORMAT arrow|parquet} table option. */
public class SqlStorageFormat extends SqlCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("FORMAT", SqlKind.OTHER);

    private final String format;

    public SqlStorageFormat(SqlParserPos pos, String format) {
        super(pos);
        this.format = format;
    }

    public String getFormat() {
        return format;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("FORMAT");
        writer.print(format);
    }
}
