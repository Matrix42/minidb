package com.minidb.server.storage;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.stats.StatsManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;
    private final Path dataDir;
    private final Map<String, ArrowTable> tables = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();
    private volatile StatsManager statsManager;

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
    }

    public void setStatsManager(StatsManager statsManager) {
        this.statsManager = statsManager;
    }

    public MiniDbCatalog catalog() {
        return catalog;
    }

    public void loadAll() {
        if (!Files.exists(dataDir)) {
            LOG.info("loaded 0 table(s) (data dir absent)");
            return;
        }
        int count = 0;
        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(dataDir, "*.arrow")) {
            for (Path file : stream) {
                loadFile(file);
                count++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.info("loaded {} table(s)", count);
    }

    private void loadFile(Path file) throws IOException {
        try (SeekableByteChannel channel =
                     Files.newByteChannel(file, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            TableSchema schema = toTableSchema(root.getSchema(),
                    stripExtension(file.getFileName().toString()));
            ArrowTable table = new ArrowTable(schema, allocator);
            while (reader.loadNextBatch()) {
                VectorSchemaRoot copy = table.newBatchRoot();
                ArrowRecordBatch recordBatch =
                        new VectorUnloader(root).getRecordBatch();
                new VectorLoader(copy).load(recordBatch);
                recordBatch.close();
                table.appendBatch(copy);
            }
            tables.put(key(schema.name()), table);
            catalog.createTable(schema);
        }
    }

    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot < 0 ? fileName : fileName.substring(0, dot);
    }

    public ArrowTable getTable(String name) {
        ArrowTable table = tables.get(key(name));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + name);
        }
        return table;
    }

    public ArrowTable createTable(TableSchema schema) {
        ArrowTable table = new ArrowTable(schema, allocator);
        if (tables.putIfAbsent(key(schema.name()), table) != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        catalog.createTable(schema);
        return table;
    }

    public void dropTable(String name) {
        ArrowTable table = tables.remove(key(name));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + name);
        }
        if (statsManager != null) {
            statsManager.dropStats(name);
        }
        catalog.dropTable(name);
        table.close();
        dirty.remove(key(name));
        try {
            Files.deleteIfExists(dataDir.resolve(fileName(name)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void truncateTable(String name) {
        ArrowTable table = tables.get(key(name));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + name);
        }
        table.clear();
        markDirty(name);
    }

    public void markDirty(String tableName) {
        dirty.add(key(tableName));
        if (statsManager != null) {
            statsManager.markStale(tableName);
        }
    }

    public void flushDirty() {
        for (String tableName : List.copyOf(dirty)) {
            flushTable(tableName);
            dirty.remove(tableName);
        }
    }

    private void flushTable(String tableName) {
        ArrowTable table = tables.get(tableName);
        if (table == null) {
            return;
        }
        try {
            Files.createDirectories(dataDir);
            Path file = dataDir.resolve(fileName(tableName));
            try (SeekableByteChannel channel = Files.newByteChannel(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                VectorSchemaRoot sink = table.newBatchRoot();
                try (ArrowFileWriter writer = new ArrowFileWriter(sink, null, channel)) {
                    writer.start();
                    for (VectorSchemaRoot batch : table.batches()) {
                        ArrowRecordBatch recordBatch =
                                new VectorUnloader(batch).getRecordBatch();
                        new VectorLoader(sink).load(recordBatch);
                        recordBatch.close();
                        writer.writeBatch();
                    }
                    writer.end();
                } finally {
                    sink.close();
                }
            }
            LOG.info("flushed table {}", tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        flushDirty();
        for (ArrowTable table : tables.values()) {
            table.close();
        }
        tables.clear();
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static String fileName(String tableName) {
        return tableName.toLowerCase(Locale.ROOT) + ".arrow";
    }

    private static TableSchema toTableSchema(
            org.apache.arrow.vector.types.pojo.Schema arrowSchema, String tableName) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            columns.add(new ColumnMeta(field.getName(), toColumnType(field.getType())));
        }
        return new TableSchema(tableName, columns);
    }

    private static ColumnType toColumnType(ArrowType type) {
        switch (type.getTypeID()) {
            case Int: {
                ArrowType.Int intType = (ArrowType.Int) type;
                return intType.getBitWidth() == 32 ? ColumnType.INTEGER : ColumnType.BIGINT;
            }
            case FloatingPoint:
                return ColumnType.DOUBLE;
            case Utf8:
                return ColumnType.VARCHAR;
            case Bool:
                return ColumnType.BOOLEAN;
            case Date:
                return ColumnType.DATE;
            case Timestamp:
                return ColumnType.TIMESTAMP;
            default:
                throw new IllegalArgumentException(
                        "unsupported arrow type in file: " + type);
        }
    }
}
