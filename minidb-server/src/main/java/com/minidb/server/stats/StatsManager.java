package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.storage.ArrowTable;
import com.minidb.server.storage.StorageManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class StatsManager implements AutoCloseable {

    private final StorageManager storage;

    public StatsManager(StorageManager storage) {
        this.storage = storage;
    }

    public void analyze(String table) {
        analyze(table, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    /** 支持裸名(按 currentSchema 解析)或限定名 `schema.table`。 */
    public void analyze(String table, String currentSchema) {
        String[] st = split(table, currentSchema);
        ArrowTable arrowTable = storage.getTable(st[0], st[1]);
        TableSchema schema = arrowTable.schema();
        Map<String, Histogram> columnHistograms = new HashMap<>();
        int numCols = schema.columns().size();
        List<List<ValueVector>> allColumnVectors = new ArrayList<>();
        for (int col = 0; col < numCols; col++) {
            allColumnVectors.add(new ArrayList<>());
        }
        try (BatchIterator it = arrowTable.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                for (int col = 0; col < numCols; col++) {
                    allColumnVectors.get(col).add(batch.getVector(col));
                }
            }
            for (int col = 0; col < numCols; col++) {
                String colName = schema.columns().get(col).name();
                ColumnType colType = schema.columns().get(col).type();
                columnHistograms.put(colName.toLowerCase(Locale.ROOT),
                        HistogramBuilder.build(allColumnVectors.get(col), colType));
            }
        }
        storage.catalog().setStats(st[0], st[1],
                new TableStats(columnHistograms, arrowTable.rowCount(), false));
    }

    public void analyzeAll() {
        for (String schema : storage.catalog().schemaNames()) {
            if (InformationSchemaCatalog.isSystemSchema(schema)) {
                continue;
            }
            for (String table : storage.catalog().tableNames(schema)) {
                analyze(schema + "." + table);
            }
        }
    }

    /** 支持裸名(默认 public)或限定名 `schema.table`。 */
    public TableStats tableStats(String table) {
        String[] st = split(table, MiniDbCatalog.DEFAULT_SCHEMA);
        return storage.catalog().getStats(st[0], st[1]);
    }

    @Override public void close() {}

    private static String[] split(String table, String defaultSchema) {
        int dot = table.indexOf('.');
        if (dot >= 0) {
            return new String[]{table.substring(0, dot), table.substring(dot + 1)};
        }
        return new String[]{defaultSchema, table};
    }
}
