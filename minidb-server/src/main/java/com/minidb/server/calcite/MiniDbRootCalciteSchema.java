package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Container schema mounted under the root as "minidb". Exposes every catalog schema as a sub-schema
 * (so {@code other.t} resolves via {@code minidb.other.t}) and also exposes the {@code
 * currentSchema}'s tables directly (so unqualified {@code t} resolves via {@code minidb.t}). The
 * catalog reader path is {@code ["minidb"]}; unqualified resolution relies on the table map below.
 */
public class MiniDbRootCalciteSchema extends AbstractSchema {

    private final MiniDbCatalog catalog;
    private final String currentSchema;

    public MiniDbRootCalciteSchema(MiniDbCatalog catalog, String currentSchema) {
        this.catalog = catalog;
        this.currentSchema = currentSchema.toLowerCase(Locale.ROOT);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return MiniDbCalciteSchema.tableMap(catalog, currentSchema);
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        Map<String, Schema> subs = new HashMap<>();
        for (String name : catalog.schemaNames()) {
            subs.put(name, new MiniDbCalciteSchema(catalog, name));
        }
        return subs;
    }
}
