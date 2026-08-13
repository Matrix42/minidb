package com.minidb.server.exec.functions;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlOperator;

/** 按 SqlOperator 分发的函数表;可变,供 UDF 挂载。 */
public final class FunctionRegistry {
    private final Map<SqlOperator, Function> byOperator = new HashMap<>();

    public Function lookup(SqlOperator op) {
        return byOperator.get(op);
    }

    public void register(SqlOperator op, Function f) {
        byOperator.put(op, f);
    }
}
