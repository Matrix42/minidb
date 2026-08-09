package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

public class MiniDbCalciteSchema extends AbstractSchema {

    private final MiniDbCatalog catalog;

    public MiniDbCalciteSchema(MiniDbCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        for (String name : catalog.tableNames()) {
            tables.put(name, new MiniDbCalciteTable(catalog.getTable(name)));
        }
        return tables;
    }
}
