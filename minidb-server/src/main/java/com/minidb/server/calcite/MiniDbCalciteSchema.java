package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

public class MiniDbCalciteSchema extends AbstractSchema {

    private final MiniDbCatalog catalog;
    private final String schemaName;

    public MiniDbCalciteSchema(MiniDbCatalog catalog, String schemaName) {
        this.catalog = catalog;
        this.schemaName = schemaName.toLowerCase(Locale.ROOT);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        for (String name : catalog.tableNames(schemaName)) {
            TableSchema ts = catalog.getTable(schemaName, name);
            tables.put(name, new MiniDbCalciteTable(ts, catalog));
        }
        return tables;
    }
}
