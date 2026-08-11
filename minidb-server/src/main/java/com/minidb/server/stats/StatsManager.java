package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.storage.ArrowTable;
import com.minidb.server.storage.StorageManager;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class StatsManager implements AutoCloseable {

    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final Path dataDir;
    private final Map<String, TableStats> tables = new ConcurrentHashMap<>();

    public StatsManager(StorageManager storage, BufferAllocator allocator, Path dataDir) {
        this.storage = storage;
        this.allocator = allocator;
        this.dataDir = dataDir;
    }

    public void analyze(String table) {
        ArrowTable arrowTable = storage.getTable(table); // throws if missing
        TableSchema schema = arrowTable.schema();
        List<VectorSchemaRoot> batches = arrowTable.batches();
        Map<String, Histogram> columnHistograms = new HashMap<>();
        for (int col = 0; col < schema.columns().size(); col++) {
            String colName = schema.columns().get(col).name();
            ColumnType colType = schema.columns().get(col).type();
            List<ValueVector> columnVectors = new ArrayList<>();
            for (VectorSchemaRoot batch : batches) {
                FieldVector fv = batch.getVector(col);
                columnVectors.add(fv);
            }
            columnHistograms.put(colName.toLowerCase(Locale.ROOT),
                    HistogramBuilder.build(columnVectors, colType));
        }
        TableStats ts = new TableStats(columnHistograms, false);
        tables.put(key(table), ts);
        persist(table, ts);
    }

    public void analyzeAll() {
        for (String name : storage.catalog().tableNames()) {
            analyze(name);
        }
    }

    public TableStats tableStats(String table) {
        return tables.get(key(table));
    }

    public void markStale(String table) {
        TableStats ts = tables.get(key(table));
        if (ts != null) {
            tables.put(key(table), new TableStats(ts.columnHistograms(), true));
        }
    }

    public void dropStats(String table) {
        tables.remove(key(table));
        try {
            Files.deleteIfExists(statsFile(table));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void loadAll() {
        if (!Files.exists(dataDir)) {
            return;
        }
        try (var stream = Files.newDirectoryStream(dataDir, "*.stats")) {
            for (Path file : stream) {
                String tableName = stripExtension(file.getFileName().toString());
                if (storage.catalog().hasTable(tableName)) {
                    TableStats ts = read(file);
                    if (ts != null) {
                        tables.put(key(tableName), ts);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void close() {
        // no-op — state is persisted on each analyze
    }

    private void persist(String table, TableStats ts) {
        try {
            Files.createDirectories(dataDir);
            Path file = statsFile(table);
            try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(file))) {
                out.writeObject(ts);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private TableStats read(Path file) {
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(file))) {
            return (TableStats) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }

    private Path statsFile(String table) {
        return dataDir.resolve(key(table) + ".stats");
    }

    private static String stripExtension(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(0, dot);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
