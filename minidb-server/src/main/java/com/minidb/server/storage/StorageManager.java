package com.minidb.server.storage;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;
    private final Path dataDir;
    private final CatalogStore catalogStore;
    private final Map<String, ArrowTable> tables = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
        this.catalogStore = new JsonCatalogStore(dataDir.resolve("catalog.json"));
        catalog.addListener(this::persistCatalog);
    }

    private void persistCatalog() {
        try {
            catalogStore.save(catalog.snapshot());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist catalog", e);
        }
    }

    public MiniDbCatalog catalog() {
        return catalog;
    }

    public void loadAll() {
        boolean restored = restoreCatalog();
        if (!Files.exists(dataDir)) {
            LOG.info("loaded 0 table(s) (data dir absent)");
            return;
        }
        int count = 0;
        try (DirectoryStream<Path> schemaDirs = Files.newDirectoryStream(dataDir)) {
            for (Path schemaDir : schemaDirs) {
                if (!Files.isDirectory(schemaDir)) {
                    continue;
                }
                String schemaName = schemaDir.getFileName().toString();
                try (DirectoryStream<Path> files =
                             Files.newDirectoryStream(schemaDir, "*.arrow")) {
                    for (Path file : files) {
                        loadFile(schemaName, file, restored);
                        count++;
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.info("loaded {} table(s)", count);
    }

    /** 恢复元数据。有 catalog.json → 恢复并返回 true;否则回退 .arrow 推断(返回 false)。 */
    private boolean restoreCatalog() {
        try {
            if (Files.exists(dataDir.resolve("catalog.json"))) {
                catalog.restore(catalogStore.load());
                return true;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return false;
    }

    private void loadFile(String schemaName, Path file, boolean restored) throws IOException {
        try (SeekableByteChannel channel =
                     Files.newByteChannel(file, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            String tableName = stripExtension(file.getFileName().toString());
            TableSchema schema;
            if (restored) {
                schema = catalog.getTable(schemaName, tableName);
            } else {
                schema = toTableSchema(root.getSchema(), schemaName, tableName);
            }
            ArrowTable table = new ArrowTable(schema, allocator);
            while (reader.loadNextBatch()) {
                VectorSchemaRoot copy = table.newBatchRoot();
                ArrowRecordBatch recordBatch =
                        new VectorUnloader(root).getRecordBatch();
                new VectorLoader(copy).load(recordBatch);
                recordBatch.close();
                table.appendBatch(copy);
            }
            tables.put(storageKey(schema.schemaName(), schema.name()), table);
            if (!restored) {
                catalog.createTable(schema);
            }
        }
    }

    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot < 0 ? fileName : fileName.substring(0, dot);
    }

    public ArrowTable getTable(String schemaName, String tableName) {
        ArrowTable table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        return table;
    }

    public ArrowTable createTable(TableSchema schema) {
        ArrowTable table = new ArrowTable(schema, allocator);
        String sk = storageKey(schema.schemaName(), schema.name());
        if (tables.putIfAbsent(sk, table) != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        catalog.createTable(schema);
        return table;
    }

    public void dropTable(String schemaName, String tableName) {
        String sk = storageKey(schemaName, tableName);
        ArrowTable table = tables.remove(sk);
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        catalog.dropTable(schemaName, tableName);
        table.close();
        dirty.remove(sk);
        try {
            Files.deleteIfExists(dataDir.resolve(fileName(schemaName, tableName)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void dropSchema(String schemaName) {
        String skPrefix = key(schemaName) + ".";
        List<String> toDrop = new ArrayList<>();
        for (String k : tables.keySet()) {
            if (k.startsWith(skPrefix)) {
                toDrop.add(k);
            }
        }
        catalog.dropSchema(schemaName); // throws for public / missing — do first
        for (String k : toDrop) {
            ArrowTable table = tables.remove(k);
            if (table != null) {
                table.close();
            }
            dirty.remove(k);
        }
        try {
            Path schemaDir = dataDir.resolve(key(schemaName));
            if (Files.exists(schemaDir)) {
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(schemaDir)) {
                    for (Path p : ds) {
                        Files.deleteIfExists(p);
                    }
                }
                Files.deleteIfExists(schemaDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void truncateTable(String schemaName, String tableName) {
        ArrowTable table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        table.clear();
        markDirty(schemaName, tableName);
    }

    public void markDirty(String schemaName, String tableName) {
        String sk = storageKey(schemaName, tableName);
        dirty.add(sk);
        catalog.markStatsStale(schemaName, tableName);
    }

    public void flushDirty() {
        for (String sk : List.copyOf(dirty)) {
            flushTable(sk);
            dirty.remove(sk);
        }
    }

    private void flushTable(String sk) {
        ArrowTable table = tables.get(sk);
        if (table == null) {
            return;
        }
        String[] parts = sk.split("\\.");
        String schemaName = parts[0];
        String tableName = parts[1];
        try {
            Path file = dataDir.resolve(fileName(schemaName, tableName));
            Files.createDirectories(file.getParent());
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
            LOG.info("flushed table {}", sk);
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

    private static String storageKey(String schemaName, String tableName) {
        return key(schemaName) + "." + key(tableName);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static String fileName(String schemaName, String tableName) {
        return key(schemaName) + "/" + key(tableName) + ".arrow";
    }

    private static TableSchema toTableSchema(
            org.apache.arrow.vector.types.pojo.Schema arrowSchema,
            String schemaName, String tableName) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            columns.add(toColumnMeta(field));
        }
        String resolvedSchema = schemaName;
        Map<String, String> meta = arrowSchema.getCustomMetadata();
        if (meta != null && meta.containsKey("schema")) {
            resolvedSchema = meta.get("schema");
        }
        return new TableSchema(resolvedSchema, tableName, columns);
    }

    private static ColumnMeta toColumnMeta(Field field) {
        Map<String, String> meta = field.getMetadata();
        String typeName = meta != null ? meta.get(ArrowTypes.TYPE_NAME_METADATA) : null;
        if (typeName != null) {
            ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
            if (type == ColumnType.DECIMAL || type == ColumnType.NUMERIC) {
                ArrowType.Decimal d = (ArrowType.Decimal) field.getType();
                return new ColumnMeta(field.getName(), type, d.getPrecision(), d.getScale());
            }
            return new ColumnMeta(field.getName(), type);
        }
        // 旧文件无元数据:回退到 Arrow 类型推断。
        return new ColumnMeta(field.getName(), inferFromArrowType(field.getType()));
    }

    private static ColumnType inferFromArrowType(ArrowType type) {
        switch (type.getTypeID()) {
            case Int: {
                ArrowType.Int intType = (ArrowType.Int) type;
                int w = intType.getBitWidth();
                if (w == 16) {
                    return ColumnType.SMALLINT;
                }
                return w == 32 ? ColumnType.INTEGER : ColumnType.BIGINT;
            }
            case FloatingPoint:
                return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.SINGLE
                        ? ColumnType.REAL : ColumnType.DOUBLE;
            case Decimal:
                // 旧格式(无元数据)的 Decimal 无法区分 DECIMAL/NUMERIC,统一归 DECIMAL;
                // precision/scale 由 ColumnMeta 默认 UNSET,toCalciteType 补默认值。
                return ColumnType.DECIMAL;
            case Utf8:
                return ColumnType.VARCHAR;
            case Bool:
                return ColumnType.BOOLEAN;
            case Date:
                return ColumnType.DATE;
            case Time:
                return ColumnType.TIME;
            case Timestamp:
                return ColumnType.TIMESTAMP;
            case Binary:
                return ColumnType.VARBINARY;
            default:
                throw new IllegalArgumentException(
                        "unsupported arrow type in file: " + type);
        }
    }
}
