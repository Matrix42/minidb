package com.minidb.server.storage;

import com.minidb.server.catalog.InformationSchemaCatalog;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.config.MiniDbConfig;
import com.minidb.storage.arrow.ArrowPartFormat;
import com.minidb.storage.arrow.IpcFileTableStorage;
import com.minidb.storage.common.PartFormat;
import com.minidb.storage.common.SimpleTable;
import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.common.TableSchema;
import com.minidb.storage.common.TableStorage;
import com.minidb.storage.lsm.LSMBackgroundExecutor;
import com.minidb.storage.lsm.LSMTable;
import com.minidb.storage.parquet.ParquetPartFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 表目录 + catalog 持久化。数据不驻留内存:每表一个目录,数据是目录里的 part 文件。
 * 无主键表用 {@link SimpleTable}(直接落 part)、有主键表用 {@link LSMTable}(LSM-Tree)。
 */
public class StorageManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;
    private final Path dataDir;
    private final MiniDbConfig config;
    private final CatalogStore catalogStore;
    private final TableStorage tableStorage;
    private final Map<StorageFormat, PartFormat> formats = new EnumMap<>(StorageFormat.class);
    private final Map<String, TableHandle> tables = new ConcurrentHashMap<>();
    private final LSMBackgroundExecutor lsmExecutor;

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
        this.config = MiniDbConfig.load(dataDir);
        this.catalogStore = new JsonCatalogStore(dataDir.resolve("catalog.json"));
        this.tableStorage = new IpcFileTableStorage(dataDir);
        formats.put(StorageFormat.ARROW, new ArrowPartFormat());
        formats.put(StorageFormat.PARQUET, new ParquetPartFormat());
        catalog.addListener(this::persistCatalog);
        this.lsmExecutor = new LSMBackgroundExecutor(
                config.lsmL0FileLimit(), config.compactionTargetSizeBytes(),
                config.lsmBackgroundIntervalMs());
        lsmExecutor.start();
    }

    public MiniDbConfig config() {
        return config;
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

    public LSMBackgroundExecutor lsmExecutor() {
        return lsmExecutor;
    }

    /** 启动:先恢复中断的 compaction,再恢复元数据,并为每张表挂「目录句柄」(按主键有无分发 LSMTable/SimpleTable)。 */
    public void loadAll() {
        recoverCompaction();
        restoreCatalog();
        for (String schema : catalog.schemaNames()) {
            if (InformationSchemaCatalog.isSystemSchema(schema)) {
                continue;
            }
            for (String tableName : catalog.tableNames(schema)) {
                TableSchema ts = catalog.getTable(schema, tableName);
                String sk = storageKey(schema, tableName);
                TableHandle table = createTableHandle(ts);
                tables.put(sk, table);
                if (table instanceof LSMTable) {
                    lsmExecutor.register(sk, (LSMTable) table);
                }
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

    public TableHandle getTable(String schemaName, String tableName) {
        TableHandle table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        return table;
    }

    public TableHandle createTable(TableSchema schema) {
        TableHandle table = createTableHandle(schema);
        String sk = storageKey(schema.schemaName(), schema.name());
        if (tables.putIfAbsent(sk, table) != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        catalog.createTable(schema);
        if (table instanceof LSMTable) {
            lsmExecutor.register(sk, (LSMTable) table);
        }
        return table;
    }

    public void dropTable(String schemaName, String tableName) {
        String sk = storageKey(schemaName, tableName);
        TableHandle old = tables.remove(sk);
        if (old == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        closeAndUnregister(old, sk);
        catalog.dropTable(schemaName, tableName);
        tableStorage.delete(schemaName, tableName);
    }

    /** 替换一张表的 TableSchema 并重建目录句柄(数据 part 不动,由调用方负责重写)。
     *  LSMTable 先 close(flush MemTable+关闭 WAL)再重建。 */
    public void alterTable(String schemaName, String tableName, TableSchema newSchema) {
        String sk = storageKey(schemaName, tableName);
        TableHandle old = tables.remove(sk);
        if (old == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        closeAndUnregister(old, sk);
        catalog.alterTable(schemaName, tableName, newSchema);
        TableHandle table = createTableHandle(newSchema);
        tables.put(sk, table);
        if (table instanceof LSMTable) {
            lsmExecutor.register(sk, (LSMTable) table);
        }
    }

    /** 改表名:迁移表目录 + 替换 catalog + 重建目录句柄。LSMTable 先 close 再重建。 */
    public void renameTable(String schemaName, String oldName, String newName) {
        String oldKey = storageKey(schemaName, oldName);
        TableHandle old = tables.remove(oldKey);
        if (old == null) {
            throw new IllegalArgumentException("table not found: " + oldName);
        }
        closeAndUnregister(old, oldKey);
        catalog.renameTable(schemaName, oldName, newName);
        Path oldDir = tableStorage.tableDir(schemaName, oldName);
        Path newDir = tableStorage.tableDir(schemaName, newName);
        try {
            if (Files.exists(oldDir)) {
                Files.move(oldDir, newDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to rename table directory", e);
        }
        TableSchema newSchema = catalog.getTable(schemaName, newName);
        String newKey = storageKey(schemaName, newName);
        TableHandle table = createTableHandle(newSchema);
        tables.put(newKey, table);
        if (table instanceof LSMTable) {
            lsmExecutor.register(newKey, (LSMTable) table);
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
            TableHandle old = tables.remove(k);
            if (old != null) {
                closeAndUnregister(old, k);
            }
        }
        tableStorage.deleteSchema(schemaName);
    }

    public void truncateTable(String schemaName, String tableName) {
        TableHandle table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        table.clearParts();
    }

    /** 合并一张表的所有 part(按配置的目标大小切分)。 */
    public void compactTable(String schemaName, String tableName) {
        TableHandle table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        table.compact(config.compactionTargetSizeBytes());
    }

    /**
     * 恢复上次 compaction 中断留下的交换目录:表目录缺失时回滚 .bak,已存在时删 .bak,
     * 残留 .tmp 直接删。保证表目录要么是旧数据要么是新数据。
     */
    private void recoverCompaction() {
        if (!Files.exists(dataDir)) {
            return;
        }
        try (DirectoryStream<Path> schemaDirs = Files.newDirectoryStream(dataDir)) {
            for (Path schemaDir : schemaDirs) {
                if (!Files.isDirectory(schemaDir)) {
                    continue;
                }
                recoverSchemaCompaction(schemaDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void recoverSchemaCompaction(Path schemaDir) throws IOException {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(schemaDir)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (name.endsWith(SimpleTable.COMPACT_BACKUP_SUFFIX)) {
                    String table = name.substring(0, name.length() - SimpleTable.COMPACT_BACKUP_SUFFIX.length());
                    Path tableDir = schemaDir.resolve(table);
                    if (Files.exists(tableDir)) {
                        deleteRecursively(entry);
                    } else {
                        Files.move(entry, tableDir);
                    }
                } else if (name.endsWith(SimpleTable.COMPACT_TMP_SUFFIX)) {
                    deleteRecursively(entry);
                }
            }
        }
    }

    private static void deleteRecursively(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (Files.isDirectory(p)) {
                    deleteRecursively(p);
                } else {
                    Files.deleteIfExists(p);
                }
            }
            Files.deleteIfExists(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        // 先关闭所有 LSMTable(flush MemTable + 关闭 WAL),再关闭后台线程。
        for (TableHandle table : tables.values()) {
            if (table instanceof LSMTable) {
                try {
                    table.close();
                } catch (Exception e) {
                    LOG.error("Failed to close LSM table", e);
                }
            }
        }
        lsmExecutor.close();
    }

    // ---- 内部辅助 ----

    /** 按 TableSchema 创建对应类型的 TableHandle:有主键→LSMTable,无→SimpleTable。 */
    private TableHandle createTableHandle(TableSchema schema) {
        PartFormat fmt = formatFor(schema);
        Path tDir = tableStorage.tableDir(schema.schemaName(), schema.name());
        if (!schema.primaryKey().isEmpty()) {
            return new LSMTable(schema, fmt, allocator, tDir, config.lsmMemtableSizeBytes());
        }
        return new SimpleTable(schema, allocator, tDir, fmt);
    }

    /** 关闭 LSMTable 并从后台 executor 注销。 */
    private void closeAndUnregister(TableHandle table, String key) {
        if (table instanceof LSMTable) {
            lsmExecutor.unregister(key);
            try {
                table.close();
            } catch (Exception e) {
                LOG.error("Failed to close LSM table: {}", key, e);
            }
        }
    }

    private static String storageKey(String schemaName, String tableName) {
        return key(schemaName) + "." + key(tableName);
    }

    private static String key(String name) {
        return name.toLowerCase(java.util.Locale.ROOT);
    }
}