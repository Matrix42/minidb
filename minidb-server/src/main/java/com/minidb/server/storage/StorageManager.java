package com.minidb.server.storage;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 表目录 + dirty 跟踪 + catalog 持久化。数据怎么落盘/加载委托给 {@link TableStorage}
 * (默认 {@link IpcFileTableStorage}),换存储引擎只换这一个字段。
 */
public class StorageManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;
    private final Path dataDir;
    private final CatalogStore catalogStore;
    private final TableStorage tableStorage;
    private final Map<String, ArrowTable> tables = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
        this.catalogStore = new JsonCatalogStore(dataDir.resolve("catalog.json"));
        this.tableStorage = new IpcFileTableStorage(dataDir);
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
        int count = 0;
        for (TableStorage.TableRef ref : tableStorage.listTables()) {
            // 有 catalog.json → schema 从 catalog 取;否则回退到存储引擎推断(旧目录兼容)。
            TableSchema schema = restored
                    ? catalog.getTable(ref.schemaName(), ref.tableName())
                    : null;
            TableStorage.LoadedTable loaded = tableStorage.load(
                    ref.schemaName(), ref.tableName(), schema, allocator);
            tables.put(storageKey(ref.schemaName(), ref.tableName()), loaded.table());
            if (!restored) {
                catalog.createTable(loaded.schema());
            }
            count++;
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
            ArrowTable table = tables.remove(k);
            if (table != null) {
                table.close();
            }
            dirty.remove(k);
        }
        tableStorage.deleteSchema(schemaName);
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
            ArrowTable table = tables.get(sk);
            if (table == null) {
                dirty.remove(sk);
                continue;
            }
            tableStorage.save(table.schema().schemaName(), table.schema().name(), table);
            dirty.remove(sk);
            LOG.info("flushed table {}", sk);
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
}
