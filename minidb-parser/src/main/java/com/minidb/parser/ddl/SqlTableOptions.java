package com.minidb.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CREATE TABLE 的 {@code WITH} 子句(Flink 风格表选项)。key 是字符串字面量,value 是 字符串/布尔/数字字面量。SQL 层目前只消费 {@code
 * format},其余 key 由执行器校验拒绝。
 */
public class SqlTableOptions extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("WITH", SqlKind.OTHER);

    private final List<SqlNode> entries;

    public SqlTableOptions(SqlParserPos pos, List<SqlNode> entries) {
        super(pos);
        this.entries = entries;
    }

    /** key → value 的保序映射(entries 是 [key, value, key, value, ...])。 */
    public Map<String, SqlNode> options() {
        Map<String, SqlNode> options = new LinkedHashMap<>();
        for (int i = 0; i + 1 < entries.size(); i += 2) {
            String key = ((SqlLiteral) entries.get(i)).getValueAs(String.class);
            options.put(key, entries.get(i + 1));
        }
        return options;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return entries;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("WITH");
        writer.print("(");
        for (int i = 0; i < entries.size(); i += 2) {
            if (i > 0) {
                writer.print(",");
            }
            entries.get(i).unparse(writer, 0, 0);
            writer.print("=");
            entries.get(i + 1).unparse(writer, 0, 0);
        }
        writer.print(")");
    }
}
