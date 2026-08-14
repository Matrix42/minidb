package com.minidb.server.calcite;

import com.minidb.server.exec.InformationSchema;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/** 只读系统 schema:暴露 information_schema.schemata/tables/columns 给 Calcite 规划。 */
public class MiniDbInformationSchemaCalciteSchema extends AbstractSchema {

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        tables.put("schemata", new MiniDbCalciteTable(InformationSchema.schemataSchema()));
        tables.put("tables", new MiniDbCalciteTable(InformationSchema.tablesSchema()));
        tables.put("columns", new MiniDbCalciteTable(InformationSchema.columnsSchema()));
        return tables;
    }
}
