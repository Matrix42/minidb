package com.minidb.storage.lsm;

import com.minidb.storage.common.*;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class LSMTable implements TableHandle {

    private final TableSchema schema;
    private final PartFormat format;
    private final BufferAllocator allocator;
    private final Path tableDir;
    private final long flushThresholdBytes;
    private final int bloomBitsPerKey;
    private final int l0FileLimit;
    private final int levelSizeMultiplier;
    private final SSTableManager sstManager;
    private final Compaction compaction;
    private final WAL wal;
    private volatile MemTable memTable;
    private volatile boolean closed;

    public LSMTable(TableSchema schema, PartFormat format, BufferAllocator allocator,
                    Path tableDir, long flushThresholdBytes) {
        this(schema, format, allocator, tableDir, flushThresholdBytes, 10, 4, 10);
    }

    public LSMTable(TableSchema schema, PartFormat format, BufferAllocator allocator,
                    Path tableDir, long flushThresholdBytes, int bloomBitsPerKey) {
        this(schema, format, allocator, tableDir, flushThresholdBytes, bloomBitsPerKey, 4, 10);
    }

    public LSMTable(TableSchema schema, PartFormat format, BufferAllocator allocator,
                    Path tableDir, long flushThresholdBytes, int bloomBitsPerKey,
                    int l0FileLimit, int levelSizeMultiplier) {
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
        this.tableDir = tableDir;
        this.flushThresholdBytes = flushThresholdBytes;
        this.bloomBitsPerKey = bloomBitsPerKey;
        this.l0FileLimit = l0FileLimit;
        this.levelSizeMultiplier = levelSizeMultiplier;
        this.sstManager = new SSTableManager();
        this.compaction = new Compaction();

        try {
            Files.createDirectories(tableDir);
            Path walFile = tableDir.resolve("wal.log");
            this.wal = new WAL(walFile, schema);
            this.memTable = new MemTable(schema, flushThresholdBytes);

            // 恢复：先 WAL → 再 SSTable
            recover();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void recover() {
        // 恢复 SSTable 元数据
        sstManager.loadExisting(tableDir, schema, format, allocator);

        // 恢复 WAL
        List<WAL.Entry> walEntries = wal.recover();
        if (!walEntries.isEmpty()) {
            // WAL 有数据 = 上次 crash 前 MemTable 没 flush
            for (WAL.Entry entry : walEntries) {
                // WAL key 统一为 String，需解析为 Integer/Long/String 以匹配 MemTable 的 raw Comparable 比较器
                List<Object> key = new ArrayList<>();
                for (Object k : entry.key()) {
                    key.add(SSTableReader.decodeKeyValue(k.toString()));
                }
                // WAL values 也是 String，需按列类型转换
                RowValue rv = entry.value();
                if (rv.values() != null) {
                    Object[] converted = new Object[rv.values().length];
                    for (int c = 0; c < converted.length; c++) {
                        converted[c] = convertValue(rv.values()[c], schema.columns().get(c).type());
                    }
                    rv = new RowValue(rv.kind(), converted);
                }
                memTable.put(key, rv);
            }
            // 如果接近阈值，直接 flush
            if (memTable.needsFlush()) {
                flushMemTable();
            }
        }
    }

    @Override
    public TableSchema schema() {
        return schema;
    }

    @Override
    public BatchIterator scan() {
        return new MergeIterator(memTable, sstManager, schema, format, allocator).scan();
    }

    @Override
    public void writePart(VectorSchemaRoot batch, Operation op) {
        byte kind = switch (op) {
            case INSERT -> RowValue.INSERT;
            case UPDATE -> RowValue.UPDATE;
            case DELETE -> RowValue.DELETE;
        };
        List<String> pkCols = schema.primaryKey();
        List<Integer> pkIdx = new ArrayList<>();
        for (String pkCol : pkCols) {
            pkIdx.add(schema.columnIndex(pkCol));
        }

        for (int r = 0; r < batch.getRowCount(); r++) {
            List<Object> key = new ArrayList<>();
            for (int idx : pkIdx) {
                Object val = batch.getVector(idx).getObject(r);
                key.add(val == null ? "" : val);
            }
            Object[] values = op == Operation.DELETE ? null : new Object[schema.columns().size()];
            if (values != null) {
                for (int c = 0; c < values.length; c++) {
                    values[c] = batch.getVector(c).getObject(r);
                }
            }
            RowValue rv = new RowValue(kind, values);
            wal.append(key, rv);
            memTable.put(key, rv);
        }

        if (memTable.needsFlush()) {
            flushMemTable();
        }
    }

    /** 查找主键对应的行（用于约束校验）。返回 null 表示不存在或被删除。 */
    public RowValue getByKey(List<Object> key) {
        RowValue rv = memTable.get(key);
        if (rv != null) {
            return rv.kind() == RowValue.DELETE ? null : rv;
        }
        // 查 SSTable：先 Bloom filter，再 key range，再读
        // SSTableWriter.encodeKey 对整数零填充以保证字典序与数值序一致，
        // 与 flushMemTable 中归一化后的 key 编码一致
        byte[] encodedKey = SSTableWriter.encodeKey(key);
        for (int level : sstManager.allLevels()) {
            for (SSTable sst : sstManager.levelFiles(level)) {
                if (!sst.bloom().mightContain(encodedKey)) continue;
                if (!sst.overlaps(key, key)) continue;
                SSTableReader reader = new SSTableReader(sst.file(), schema, format, allocator);
                try {
                    BatchIterator it = reader.scan();
                    while (it.hasNext()) {
                        VectorSchemaRoot batch = it.next();
                        for (int i = 0; i < batch.getRowCount(); i++) {
                            List<Object> rowKey = new ArrayList<>();
                            for (String pkCol : schema.primaryKey()) {
                                int idx = schema.columnIndex(pkCol);
                                rowKey.add(batch.getVector(idx).getObject(i));
                            }
                            // SSTable key 是 Integer/Long，用 SSTable.KEY_COMPARATOR
                            // （非 MemTable.KEY_COMPARATOR，后者要求 String 强转）
                            if (SSTable.KEY_COMPARATOR.compare(rowKey, key) == 0) {
                                Object[] values = new Object[schema.columns().size()];
                                for (int c = 0; c < values.length; c++) {
                                    values[c] = batch.getVector(c).getObject(i);
                                }
                                it.close();
                                return new RowValue(RowValue.INSERT, values);
                            }
                        }
                    }
                    it.close();
                } finally {
                    reader.close();
                }
            }
        }
        return null;
    }

    @Override
    public long rowCount() {
        long count = 0;
        // MemTable 中的有效行（非 DELETE）
        for (Map.Entry<List<Object>, RowValue> e : memTable.rows()) {
            if (e.getValue().kind() != RowValue.DELETE) {
                count++;
            }
        }
        // SSTable 中的行数（近似，不扣减 MemTable 覆盖的）
        for (SSTable sst : sstManager.allSSTables()) {
            count += sst.rowCount();
        }
        return count;
    }

    @Override
    public int partCount() {
        return sstManager.allSSTables().size();
    }

    @Override
    public int compact(long targetSizeBytes) {
        // L0 → L1: 文件数触发
        if (sstManager.levelFiles(0).size() >= l0FileLimit) {
            compaction.compactLevel0To1(sstManager, schema, format, allocator,
                    tableDir, targetSizeBytes, bloomBitsPerKey);
        }
        // L1+ → L(n+1): 层大小触发（每层上限 = targetSizeBytes * multiplier^level）
        long levelSizeLimit = targetSizeBytes;
        for (int level = 1; ; level++) {
            List<SSTable> files = sstManager.levelFiles(level);
            if (files.isEmpty()) break;
            long levelSize = 0;
            for (SSTable sst : files) {
                levelSize += sst.rowCount() * 100; // 每行约 100 字节估算
            }
            if (levelSize >= levelSizeLimit) {
                compaction.compactLevel(level, sstManager, schema, format, allocator,
                        tableDir, targetSizeBytes, bloomBitsPerKey);
            }
            levelSizeLimit *= levelSizeMultiplier;
            // 溢出保护
            if (levelSizeLimit < 0) break;
        }
        return 1;
    }

    @Override
    public void clearParts() {
        // truncate: 清空 MemTable + 删所有 SSTable
        memTable = new MemTable(schema, flushThresholdBytes);
        wal.truncate();
        List<SSTable> all = new ArrayList<>(sstManager.allSSTables());
        sstManager.remove(all);
    }

    @Override
    public VectorSchemaRoot newBatchRoot() {
        return VectorSchemaRoot.create(ArrowTypes.arrowSchema(schema), allocator);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        // 关闭前 flush MemTable
        if (!memTable.isEmpty()) {
            flushMemTable();
        }
        wal.close();
    }

    public void flushMemTable() {
        if (closed) return;
        MemTable oldMt = this.memTable;
        this.memTable = new MemTable(schema, flushThresholdBytes);
        if (oldMt.isEmpty()) {
            wal.truncate();
            return;
        }
        long seq = sstManager.nextSeq();
        Path file = tableDir.resolve("sst-L0-" + String.format("%06d", seq) + ".sst");

        // 过滤 DELETE tombstone，SSTable 只存有效行
        List<Map.Entry<List<Object>, RowValue>> normalized = new ArrayList<>();
        for (Map.Entry<List<Object>, RowValue> entry : oldMt.rows()) {
            if (entry.getValue().kind() == RowValue.DELETE) continue;
            normalized.add(Map.entry(entry.getKey(), entry.getValue()));
        }

        if (normalized.isEmpty()) {
            wal.truncate();
            return;
        }

        SSTableWriter writer = new SSTableWriter(file, 0, schema, format, allocator, bloomBitsPerKey);
        writer.writeFromIterator(normalized.iterator(), normalized.size());
        SSTableReader reader = new SSTableReader(file, schema, format, allocator);
        SSTable sst = reader.metadata();
        reader.close();
        sstManager.addLevel0(new SSTable(file, 0, seq, sst.minKey(), sst.maxKey(),
                sst.rowCount(), sst.bloom()));
        wal.truncate();
    }

    // 暴露给后台任务
    public boolean needsCompaction(int l0Limit) {
        if (closed) return false;
        // L0: 文件数触发
        if (sstManager.levelFiles(0).size() >= l0Limit) return true;
        // L1+: 层大小触发
        long levelSizeLimit = 64L * 1024 * 1024; // L1 基准: 64MB
        for (int level = 1; ; level++) {
            List<SSTable> files = sstManager.levelFiles(level);
            if (files.isEmpty()) break;
            long levelSize = 0;
            for (SSTable sst : files) {
                levelSize += sst.rowCount() * 100;
            }
            if (levelSize >= levelSizeLimit) return true;
            levelSizeLimit *= levelSizeMultiplier;
            if (levelSizeLimit < 0) break;
        }
        return false;
    }

    /** WAL 恢复时将 String 值转换为列类型对应的 Java 类型。 */
    private static Object convertValue(Object val, ColumnType type) {
        if (val == null) return null;
        String s = val.toString();
        return switch (type) {
            case SMALLINT -> Short.parseShort(s);
            case INTEGER -> Integer.parseInt(s);
            case BIGINT -> Long.parseLong(s);
            case REAL, FLOAT -> Float.parseFloat(s);
            case DOUBLE -> Double.parseDouble(s);
            case DECIMAL, NUMERIC -> new java.math.BigDecimal(s);
            case BOOLEAN -> Boolean.parseBoolean(s);
            case DATE -> Integer.parseInt(s);
            case TIME -> Integer.parseInt(s);
            case TIMESTAMP -> Long.parseLong(s);
            default -> s; // VARCHAR, CHAR, BINARY 等保持字符串
        };
    }
}