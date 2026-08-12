package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StatsManager.class);

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
        ArrowTable arrowTable = storage.getTable(
                MiniDbCatalog.DEFAULT_SCHEMA, table); // throws if missing
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
        tables.put(resolveKey(table), ts);
        persist(table, ts);
    }

    public void analyzeAll() {
        for (String name : storage.catalog().tableNames()) {
            analyze(name);
        }
    }

    public TableStats tableStats(String table) {
        return tables.get(resolveKey(table));
    }

    public void markStale(String table) {
        String k = resolveKey(table);
        TableStats ts = tables.get(k);
        if (ts != null) {
            tables.put(k, new TableStats(ts.columnHistograms(), true));
        }
    }

    public void dropStats(String table) {
        String k = resolveKey(table);
        tables.remove(k);
        try {
            Files.deleteIfExists(statsFile(k));
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
                String fileName = stripExtension(file.getFileName().toString());
                String[] parts = fileName.split("\\.", 2);
                String schema = parts.length == 2
                        ? parts[0] : MiniDbCatalog.DEFAULT_SCHEMA;
                String table = parts.length == 2 ? parts[1] : fileName;
                if (storage.catalog().hasTable(schema, table)) {
                    TableStats ts = read(file);
                    if (ts != null) {
                        tables.put(key(fileName), ts);
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
            Path file = statsFile(resolveKey(table));
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
            LOG.warn("failed to read stats file {}", file, e);
            return null;
        }
    }

    private Path statsFile(String resolvedKey) {
        return dataDir.resolve(resolvedKey + ".stats");
    }

    private static String stripExtension(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(0, dot);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static String resolveKey(String table) {
        if (table.contains(".")) {
            return key(table);
        }
        return key(MiniDbCatalog.DEFAULT_SCHEMA + "." + table);
    }
}
