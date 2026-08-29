package com.minidb.server.storage;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.config.MiniDbConfig;
import com.minidb.server.plan.physical.RowVectors;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.BatchIterator;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.IndexDef;
import com.minidb.storage.common.PartFormat;
import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableHandle;
import com.minidb.storage.common.TableSchema;
import com.minidb.storage.common.TableStorage;
import com.minidb.storage.common.TableType;
import com.minidb.storage.lsm.LSMBackgroundExecutor;
import com.minidb.storage.lsm.LSMTable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 二级索引的存储管理。
 *
 * <p>每张数据表对应一组索引表,目录位于: {@code data/<schema>/<table>/.indexes/<indexName>/}。这些索引表不进入 catalog, 也不对
 * Calcite 暴露,仅用于查询加速与 UNIQUE 约束校验。
 *
 * <p>索引表 schema = (索引列..., 数据主键列...),主键 = 全部列。合成 schema 的 PK 列会被 {@link TableSchema} 强制标为 NOT
 * NULL,但这只是元数据;LSM 对 null 主键按空串编码,UNIQUE 校验与查询路径通过跳过 null 键来避免语义问题。
 *
 * <p>索引表的 LSMTable 必须注册到 {@link LSMBackgroundExecutor},否则 MemTable 写满后不会自动落盘(测试路径无后台时退化同步
 * flush,但仍需注册以获得 executor)。
 */
public class IndexManager {

    private static final Logger LOG = LoggerFactory.getLogger(IndexManager.class);

    /** 一个索引批次的最大行数。 */
    private static final int MAX_BATCH_ROWS = 4096;

    private final MiniDbCatalog catalog;
    private final MiniDbConfig config;
    private final BufferAllocator allocator;
    private final Map<StorageFormat, PartFormat> formats;
    private final TableStorage tableStorage;
    private final LSMBackgroundExecutor lsmExecutor;

    // outer key = schema.table(小写);inner key = indexName(小写)
    private final Map<String, Map<String, TableHandle>> indexes = new ConcurrentHashMap<>();

    IndexManager(
            MiniDbCatalog catalog,
            MiniDbConfig config,
            BufferAllocator allocator,
            Map<StorageFormat, PartFormat> formats,
            TableStorage tableStorage,
            LSMBackgroundExecutor lsmExecutor) {
        this.catalog = catalog;
        this.config = config;
        this.allocator = allocator;
        this.formats = formats;
        this.tableStorage = tableStorage;
        this.lsmExecutor = lsmExecutor;
    }

    /**
     * 合成索引表 schema。列序 = 索引列 + 数据表主键列,PK = 全部列。 列类型、precision/scale、nullable 均保留数据表原样,storageFormat
     * 与数据表同款。 这是一个公共静态 helper,查询路径(MiniDbScan)需要复用同一合成逻辑。
     */
    public static TableSchema indexSchema(String schemaName, IndexDef def, TableSchema data) {
        List<ColumnMeta> cols = new ArrayList<>(def.columns().size() + data.primaryKey().size());
        for (String name : def.columns()) {
            cols.add(data.column(name));
        }
        for (String name : data.primaryKey()) {
            cols.add(data.column(name));
        }
        List<String> pk = new ArrayList<>(cols.size());
        for (ColumnMeta c : cols) {
            pk.add(c.name());
        }
        return new TableSchema(
                schemaName,
                def.name(),
                cols,
                pk,
                List.of(),
                List.of(),
                data.storageFormat(),
                TableType.LSM,
                List.of(),
                null);
    }

    /** 创建索引表句柄,目录位于数据表目录下的 {@code .indexes/<name>}。 目录必须不存在;调用方负责后续 populate。 */
    public TableHandle createIndex(
            String schemaName, String tableName, IndexDef def, TableSchema data) {
        Path idxDir = indexDir(schemaName, tableName, def.name());
        if (Files.exists(idxDir)) {
            throw new IllegalArgumentException("index directory already exists: " + idxDir);
        }
        try {
            Files.createDirectories(idxDir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create index directory: " + idxDir, e);
        }
        LSMTable table = openIndex(schemaName, def, data, idxDir);
        String outer = storageKey(schemaName, tableName);
        String inner = key(def.name());
        lsmExecutor.register(outer + "." + inner, table);
        indexes.computeIfAbsent(outer, k -> new ConcurrentHashMap<>()).put(inner, table);
        return table;
    }

    /**
     * 扫描数据表,把每行的(索引列..., 主键列...)提取出来批量写入索引表。 LSMTable 的 scan() 返回超集语义(对索引表 key 无裁剪),但这里写入的是精确键,
     * 因此无超集问题。
     *
     * <p>若索引为 UNIQUE,在写入前逐批用 seen 集校验无重复(含存量与批内);冲突抛 {@link
     * IllegalArgumentException}。调用方(QueryExecutor.handleCreateIndex) 捕获异常后清理索引半成品。
     */
    public void populateFromTable(IndexDef def, TableHandle dataTable, TableHandle indexTable) {
        TableSchema data = dataTable.schema();
        List<Integer> idxPositions = new ArrayList<>(def.columns().size());
        for (String col : def.columns()) {
            idxPositions.add(data.columnIndex(col));
        }
        List<Integer> pkPositions = new ArrayList<>(data.primaryKey().size());
        for (String col : data.primaryKey()) {
            pkPositions.add(data.columnIndex(col));
        }

        // UNIQUE 索引:seen 集追踪所有已写键,发现重复即抛错(调用方清理半成品)
        Set<List<Object>> seen = def.unique() ? new HashSet<>() : null;

        List<Object[]> buffer = new ArrayList<>(MAX_BATCH_ROWS);
        try (BatchIterator it = dataTable.scan()) {
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                int n = batch.getRowCount();
                for (int r = 0; r < n; r++) {
                    Object[] key = new Object[idxPositions.size() + pkPositions.size()];
                    int pos = 0;
                    for (int col : idxPositions) {
                        key[pos++] = RowVectors.readObject(batch.getVector(col), r);
                    }
                    for (int col : pkPositions) {
                        key[pos++] = RowVectors.readObject(batch.getVector(col), r);
                    }
                    if (seen != null) {
                        // 仅索引列值(不含主键)检查重复——同索引值跨主键仍算冲突
                        List<Object> idxKey = new ArrayList<>(idxPositions.size());
                        idxKey.addAll(Arrays.asList(key).subList(0, idxPositions.size()));
                        // null 键不参与唯一性
                        if (idxKey.stream().noneMatch(Objects::isNull) && !seen.add(idxKey)) {
                            throw new IllegalArgumentException(
                                    "unique index constraint violation: "
                                            + def.name()
                                            + " — duplicate key "
                                            + idxKey);
                        }
                    }
                    buffer.add(key);
                    if (buffer.size() >= MAX_BATCH_ROWS) {
                        flushIndexBatch(indexTable, buffer);
                    }
                }
            }
        }
        if (!buffer.isEmpty()) {
            flushIndexBatch(indexTable, buffer);
        }
    }

    public void dropIndex(String schemaName, String tableName, String indexName) {
        String outer = storageKey(schemaName, tableName);
        String inner = key(indexName);
        Map<String, TableHandle> map = indexes.get(outer);
        TableHandle table = map == null ? null : map.remove(inner);
        if (table != null) {
            closeAndUnregister(outer + "." + inner, table);
        }
        deleteRecursively(indexDir(schemaName, tableName, indexName));
    }

    public TableHandle getIndex(String schemaName, String tableName, String indexName) {
        Map<String, TableHandle> map = indexes.get(storageKey(schemaName, tableName));
        return map == null ? null : map.get(key(indexName));
    }

    public void onInsert(TableSchema data, VectorSchemaRoot dataBatch) {
        applyToIndexes(
                data,
                (indexTable, idxPositions, pkPositions) -> {
                    List<Object[]> buffer = extractIndexRows(dataBatch, idxPositions, pkPositions);
                    writeBatch(indexTable, buffer, TableHandle.Operation.INSERT);
                });
    }

    public void onDelete(TableSchema data, VectorSchemaRoot dataBatch) {
        applyToIndexes(
                data,
                (indexTable, idxPositions, pkPositions) -> {
                    List<Object[]> buffer = extractIndexRows(dataBatch, idxPositions, pkPositions);
                    writeBatch(indexTable, buffer, TableHandle.Operation.DELETE);
                });
    }

    public void onUpdate(TableSchema data, VectorSchemaRoot oldBatch, VectorSchemaRoot newBatch) {
        applyToIndexes(
                data,
                (indexTable, idxPositions, pkPositions) -> {
                    int rows = oldBatch.getRowCount();
                    List<Object[]> toDelete = new ArrayList<>(rows);
                    List<Object[]> toInsert = new ArrayList<>(rows);
                    for (int r = 0; r < rows; r++) {
                        Object[] oldKey = extractIndexRow(oldBatch, r, idxPositions, pkPositions);
                        Object[] newKey = extractIndexRow(newBatch, r, idxPositions, pkPositions);
                        if (!keysEqual(oldKey, newKey)) {
                            toDelete.add(oldKey);
                            toInsert.add(newKey);
                        }
                    }
                    if (!toDelete.isEmpty()) {
                        writeBatch(indexTable, toDelete, TableHandle.Operation.DELETE);
                    }
                    if (!toInsert.isEmpty()) {
                        writeBatch(indexTable, toInsert, TableHandle.Operation.INSERT);
                    }
                });
    }

    private void applyToIndexes(TableSchema data, IndexAction action) {
        if (data.indexes().isEmpty()) {
            return;
        }
        Map<String, TableHandle> map = indexes.get(storageKey(data.schemaName(), data.name()));
        if (map == null) {
            return;
        }
        List<Integer> pkPositions = new ArrayList<>(data.primaryKey().size());
        for (String col : data.primaryKey()) {
            pkPositions.add(data.columnIndex(col));
        }
        for (IndexDef def : data.indexes()) {
            TableHandle indexTable = map.get(key(def.name()));
            if (indexTable == null) {
                LOG.warn(
                        "index handle missing: {}.{}.{}",
                        data.schemaName(),
                        data.name(),
                        def.name());
                continue;
            }
            List<Integer> idxPositions = new ArrayList<>(def.columns().size());
            for (String col : def.columns()) {
                idxPositions.add(data.columnIndex(col));
            }
            action.apply(indexTable, idxPositions, pkPositions);
        }
    }

    @FunctionalInterface
    private interface IndexAction {
        void apply(TableHandle indexTable, List<Integer> idxPositions, List<Integer> pkPositions);
    }

    private List<Object[]> extractIndexRows(
            VectorSchemaRoot batch, List<Integer> idxPositions, List<Integer> pkPositions) {
        List<Object[]> rows = new ArrayList<>(batch.getRowCount());
        for (int r = 0; r < batch.getRowCount(); r++) {
            rows.add(extractIndexRow(batch, r, idxPositions, pkPositions));
        }
        return rows;
    }

    private Object[] extractIndexRow(
            VectorSchemaRoot batch,
            int row,
            List<Integer> idxPositions,
            List<Integer> pkPositions) {
        Object[] key = new Object[idxPositions.size() + pkPositions.size()];
        int pos = 0;
        for (int col : idxPositions) {
            key[pos++] = RowVectors.readObject(batch.getVector(col), row);
        }
        for (int col : pkPositions) {
            key[pos++] = RowVectors.readObject(batch.getVector(col), row);
        }
        return key;
    }

    private boolean keysEqual(Object[] a, Object[] b) {
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (!Objects.equals(a[i], b[i])) {
                return false;
            }
        }
        return true;
    }

    private void flushIndexBatch(TableHandle indexTable, List<Object[]> rows) {
        writeBatch(indexTable, rows, TableHandle.Operation.INSERT);
    }

    private void writeBatch(TableHandle indexTable, List<Object[]> rows, TableHandle.Operation op) {
        if (rows.isEmpty()) {
            return;
        }
        try (VectorSchemaRoot root =
                VectorSchemaRoot.create(ArrowTypes.arrowSchema(indexTable.schema()), allocator)) {
            for (FieldVector v : root.getFieldVectors()) {
                v.setInitialCapacity(rows.size());
                v.allocateNew();
            }
            for (int r = 0; r < rows.size(); r++) {
                Object[] row = rows.get(r);
                for (int c = 0; c < row.length; c++) {
                    RowVectors.writeObject(root.getVector(c), r, row[c]);
                }
            }
            root.setRowCount(rows.size());
            indexTable.writePart(root, op);
        }
        rows.clear();
    }

    void clearIndexes(String schemaName, String tableName) {
        Map<String, TableHandle> map = indexes.get(storageKey(schemaName, tableName));
        if (map == null) {
            return;
        }
        for (TableHandle t : map.values()) {
            t.clearParts();
        }
    }

    void dropIndexesForTable(String schemaName, String tableName) {
        String outer = storageKey(schemaName, tableName);
        Map<String, TableHandle> map = indexes.remove(outer);
        if (map == null) {
            return;
        }
        for (Map.Entry<String, TableHandle> e : map.entrySet()) {
            closeAndUnregister(outer + "." + e.getKey(), e.getValue());
        }
    }

    void renameTable(String oldSchema, String oldName, String newSchema, String newName) {
        String oldOuter = storageKey(oldSchema, oldName);
        Map<String, TableHandle> map = indexes.remove(oldOuter);
        if (map == null || map.isEmpty()) {
            return;
        }
        for (Map.Entry<String, TableHandle> e : map.entrySet()) {
            closeAndUnregister(oldOuter + "." + e.getKey(), e.getValue());
        }
        // 目录已随数据表目录移动,按新 schema 重建句柄
        rebuildFromDisk(newSchema, newName, catalog.getTable(newSchema, newName));
    }

    void rebuildFromDisk(String schemaName, String tableName, TableSchema data) {
        if (data.indexes().isEmpty()) {
            return;
        }
        Path idxParent = tableStorage.tableDir(schemaName, tableName).resolve(".indexes");
        if (!Files.exists(idxParent)) {
            return;
        }
        String outer = storageKey(schemaName, tableName);
        Map<String, TableHandle> map =
                indexes.computeIfAbsent(outer, k -> new ConcurrentHashMap<>());
        for (IndexDef def : data.indexes()) {
            Path idxDir = idxParent.resolve(def.name());
            if (!Files.exists(idxDir)) {
                LOG.warn("index directory missing during recovery: {}", idxDir);
                continue;
            }
            LSMTable table = openIndex(schemaName, def, data, idxDir);
            String inner = key(def.name());
            lsmExecutor.register(outer + "." + inner, table);
            map.put(inner, table);
        }
    }

    private LSMTable openIndex(String schemaName, IndexDef def, TableSchema data, Path idxDir) {
        TableSchema schema = indexSchema(schemaName, def, data);
        PartFormat format = formats.get(data.storageFormat());
        if (format == null) {
            throw new IllegalArgumentException(
                    "unknown storage format for index: " + data.storageFormat());
        }
        return new LSMTable(
                schema,
                format,
                allocator,
                idxDir,
                config.lsmMemtableSizeBytes(),
                config.lsmBloomBitsPerKey(),
                config.lsmL0FileLimit(),
                config.lsmLevelSizeMultiplier());
    }

    private void closeAndUnregister(String key, TableHandle table) {
        if (table instanceof LSMTable) {
            lsmExecutor.unregister(key);
            try {
                table.close();
            } catch (Exception e) {
                LOG.error("failed to close index table: {}", key, e);
            }
        }
    }

    private Path indexDir(String schemaName, String tableName, String indexName) {
        return tableStorage.tableDir(schemaName, tableName).resolve(".indexes").resolve(indexName);
    }

    private static String storageKey(String schemaName, String tableName) {
        return key(schemaName) + "." + key(tableName);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
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
            throw new UncheckedIOException("failed to delete index directory: " + dir, e);
        }
    }
}
