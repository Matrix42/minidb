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
    private final SSTableManager sstManager;
    private final Compaction compaction;
    private final WAL wal;
    private volatile MemTable memTable;

    public LSMTable(TableSchema schema, PartFormat format, BufferAllocator allocator,
                    Path tableDir, long flushThresholdBytes) {
        this(schema, format, allocator, tableDir, flushThresholdBytes, 10);
    }

    public LSMTable(TableSchema schema, PartFormat format, BufferAllocator allocator,
                    Path tableDir, long flushThresholdBytes, int bloomBitsPerKey) {
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
        this.tableDir = tableDir;
        this.flushThresholdBytes = flushThresholdBytes;
        this.bloomBitsPerKey = bloomBitsPerKey;
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
                memTable.put(entry.key(), entry.value());
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
            // MemTable 的 KEY_COMPARATOR 要求 key 元素为 String 类型；
            // Arrow 列值（Integer 等）需转为 String 再存入 MemTable。
            List<Object> key = new ArrayList<>();
            for (int idx : pkIdx) {
                Object val = batch.getVector(idx).getObject(r);
                key.add(val == null ? "" : val.toString());
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
        // MemTable 存储 String key，输入 key 是 Arrow 原生类型（Integer 等），先转换
        List<Object> stringKey = new ArrayList<>();
        for (Object k : key) {
            stringKey.add(k == null ? "" : k.toString());
        }
        RowValue rv = memTable.get(stringKey);
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
        // 调用方（LSMBackgroundExecutor）已经通过 needsCompaction(l0FileLimit) 检查，
        // 这里不再重复检查，直接用配置的阈值执行 compaction
        compaction.compactLevel0To1(sstManager, schema, format, allocator, tableDir, targetSizeBytes, bloomBitsPerKey);
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
        // 关闭前 flush MemTable
        if (!memTable.isEmpty()) {
            flushMemTable();
        }
        wal.close();
    }

    public void flushMemTable() {
        MemTable oldMt = this.memTable;
        this.memTable = new MemTable(schema, flushThresholdBytes);
        if (oldMt.isEmpty()) {
            wal.truncate();
            return;
        }
        long seq = sstManager.nextSeq();
        Path file = tableDir.resolve("sst-L0-" + String.format("%06d", seq) + ".sst");

        // 归一化 key：MemTable 的 String key → Integer/Long/String，
        // 与 SSTableWriter.encodeKey 的零填充编码一致，保证 bloom filter 和索引可查。
        // 同时过滤 DELETE tombstone，SSTable 只存有效行。
        List<Map.Entry<List<Object>, RowValue>> normalized = new ArrayList<>();
        for (Map.Entry<List<Object>, RowValue> entry : oldMt.rows()) {
            if (entry.getValue().kind() == RowValue.DELETE) continue;
            List<Object> normalizedKey = MergeIterator.normalizeKey(entry.getKey());
            normalized.add(Map.entry(normalizedKey, entry.getValue()));
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
        return sstManager.levelFiles(0).size() >= l0Limit;
    }
}