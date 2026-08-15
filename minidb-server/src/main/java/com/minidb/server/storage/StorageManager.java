package com.minidb.server.storage;

import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.arrow.IpcFileTableStorage;
import com.minidb.storage.common.PartFormat;
import com.minidb.storage.common.SimpleTable;
import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableSchema;
import com.minidb.storage.common.TableStorage;
import com.minidb.storage.parquet.ParquetPartFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 表目录 + catalog 持久化。数据不驻留内存:每表一个目录,数据是目录里的 part 文件,
 * 由 {@link SimpleTable} 写入直接落盘、读取递归读 part。本类只持「目录句柄」。
 */
public class StorageManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;
    private final Path dataDir;
    private final CatalogStore catalogStore;
    private final TableStorage tableStorage;
    private final Map<StorageFormat, PartFormat> formats = new EnumMap<>(StorageFormat.class);
    private final Map<String, SimpleTable> tables = new ConcurrentHashMap<>();

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
        this.catalogStore = new JsonCatalogStore(dataDir.resolve("catalog.json"));
        this.tableStorage = new IpcFileTableStorage(dataDir);
        formats.put(StorageFormat.ARROW, new ArrowPartFormat());
        formats.put(StorageFormat.PARQUET, new ParquetPartFormat());
        catalog.addListener(this::persistCatalog);
    }

    private PartFormat formatFor(TableSchema schema) {
        PartFormat format = formats.get(schema.storageFormat());
        if (format == null) {
            throw new IllegalArgumentException("unknown storage format: " + schema.storageFormat());
        }
        return format;
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

    /** 启动:恢复元数据,并为每张表挂一个「目录句柄」(不加载数据)。 */
    public void loadAll() {
        restoreCatalog();
        for (String schema : catalog.schemaNames()) {
            if (InformationSchemaCatalog.isSystemSchema(schema)) {
                continue;
            }
            for (String tableName : catalog.tableNames(schema)) {
                TableSchema ts = catalog.getTable(schema, tableName);
                tables.put(storageKey(schema, tableName),
                        new SimpleTable(ts, allocator, tableStorage.tableDir(schema, tableName), formatFor(ts)));
            }
        }
        LOG.info("loaded {} table(s)", tables.size());
    }

    /** 恢复元数据(catalog.json)。磁盘型下无旧数据回退:无 catalog.json 即空库。 */
    private void restoreCatalog() {
        try {
            if (Files.exists(dataDir.resolve("catalog.json"))) {
                catalog.restore(catalogStore.load());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public SimpleTable getTable(String schemaName, String tableName) {
        SimpleTable table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        return table;
    }

    public SimpleTable createTable(TableSchema schema) {
        SimpleTable table = new SimpleTable(schema, allocator,
                tableStorage.tableDir(schema.schemaName(), schema.name()), formatFor(schema));
        String sk = storageKey(schema.schemaName(), schema.name());
        if (tables.putIfAbsent(sk, table) != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        catalog.createTable(schema);
        return table;
    }

    public void dropTable(String schemaName, String tableName) {
        String sk = storageKey(schemaName, tableName);
        if (tables.remove(sk) == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        catalog.dropTable(schemaName, tableName);
        tableStorage.delete(schemaName, tableName);
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
            tables.remove(k);
        }
        tableStorage.deleteSchema(schemaName);
    }

    public void truncateTable(String schemaName, String tableName) {
        SimpleTable table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        table.clearParts();
    }

    @Override
    public void close() {
        // 数据不驻留内存,无需 flush/close。
    }

    private static String storageKey(String schemaName, String tableName) {
        return key(schemaName) + "." + key(tableName);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
