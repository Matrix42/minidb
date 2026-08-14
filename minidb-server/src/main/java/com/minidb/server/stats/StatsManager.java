package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
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
        ArrowTable arrowTable = storage.getTable(MiniDbCatalog.DEFAULT_SCHEMA, table);
        TableSchema schema = arrowTable.schema();
        Map<String, Histogram> columnHistograms = new HashMap<>();
        for (int col = 0; col < schema.columns().size(); col++) {
            String colName = schema.columns().get(col).name();
            ColumnType colType = schema.columns().get(col).type();
            List<ValueVector> columnVectors = new ArrayList<>();
            for (VectorSchemaRoot batch : arrowTable.batches()) {
                columnVectors.add(batch.getVector(col));
            }
            columnHistograms.put(colName.toLowerCase(Locale.ROOT),
                    HistogramBuilder.build(columnVectors, colType));
        }
        storage.catalog().setStats(MiniDbCatalog.DEFAULT_SCHEMA, table,
                new TableStats(columnHistograms, arrowTable.rowCount(), false));
    }

    public void analyzeAll() {
        for (String name : storage.catalog().tableNames()) {
            analyze(name);
        }
    }

    public TableStats tableStats(String table) {
        return storage.catalog().getStats(MiniDbCatalog.DEFAULT_SCHEMA, table);
    }

    @Override public void close() {}
}
