package com.minidb.storage.lsm;

import com.minidb.storage.common.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.file.Path;
import java.util.*;

/** Leveled compaction: 合并 L(n) 的 SSTable 到 L(n+1)，按 key 排序并去重 （同 key 保留最新版本）。 */
public class Compaction {

    /** L0 → L1 compaction: 合并 L0 所有 SSTable + L1 中重叠的 SSTable */
    public void compactLevel0To1(
            SSTableManager mgr,
            TableSchema schema,
            PartFormat format,
            BufferAllocator allocator,
            Path tableDir,
            long targetSizeBytes,
            int bloomBitsPerKey) {
        List<SSTable> l0Files = mgr.levelFiles(0);
        if (l0Files.isEmpty()) return;

        // 找到 L0 的 key range 并集
        List<Object> overallMin = null;
        List<Object> overallMax = null;
        for (SSTable sst : l0Files) {
            if (overallMin == null
                    || SSTable.KEY_COMPARATOR.compare(sst.minKey(), overallMin) < 0) {
                overallMin = sst.minKey();
            }
            if (overallMax == null
                    || SSTable.KEY_COMPARATOR.compare(sst.maxKey(), overallMax) > 0) {
                overallMax = sst.maxKey();
            }
        }

        // 选 L1 中重叠的 SSTable
        List<SSTable> l1Overlap = new ArrayList<>();
        for (SSTable sst : mgr.levelFiles(1)) {
            if (sst.overlaps(overallMin, overallMax)) {
                l1Overlap.add(sst);
            }
        }

        // 合并输入：L0 在前（新版本），L1 在后（旧版本）
        List<SSTable> inputs = new ArrayList<>();
        inputs.addAll(l0Files);
        inputs.addAll(l1Overlap);

        // 归并排序输出
        List<SSTable> outputs =
                mergeAndWrite(
                        inputs,
                        1,
                        mgr,
                        schema,
                        format,
                        allocator,
                        tableDir,
                        targetSizeBytes,
                        bloomBitsPerKey);

        // 删除旧文件，添加新文件
        mgr.remove(inputs);
        mgr.addLevelN(1, outputs);
    }

    /** 通用 compaction: 合并 L(n) → L(n+1) */
    public void compactLevel(
            int level,
            SSTableManager mgr,
            TableSchema schema,
            PartFormat format,
            BufferAllocator allocator,
            Path tableDir,
            long targetSizeBytes,
            int bloomBitsPerKey) {
        List<SSTable> levelFiles = mgr.levelFiles(level);
        if (levelFiles.isEmpty()) return;

        List<Object> minKey = levelFiles.get(0).minKey();
        List<Object> maxKey = levelFiles.get(levelFiles.size() - 1).maxKey();

        // 选下一层重叠的
        List<SSTable> nextOverlap = new ArrayList<>();
        for (SSTable sst : mgr.levelFiles(level + 1)) {
            if (sst.overlaps(minKey, maxKey)) {
                nextOverlap.add(sst);
            }
        }

        // 当前层在前（新版本），下一层在后（旧版本）
        List<SSTable> inputs = new ArrayList<>();
        inputs.addAll(levelFiles);
        inputs.addAll(nextOverlap);

        List<SSTable> outputs =
                mergeAndWrite(
                        inputs,
                        level + 1,
                        mgr,
                        schema,
                        format,
                        allocator,
                        tableDir,
                        targetSizeBytes,
                        bloomBitsPerKey);

        mgr.remove(inputs);
        mgr.addLevelN(level + 1, outputs);
    }

    /** 归并排序多个 SSTable → 新的 SSTable 列表。 inputs 的顺序决定同 key 去重时的优先级：前面的文件更新，保留第一个遇到的值。 */
    private List<SSTable> mergeAndWrite(
            List<SSTable> inputs,
            int targetLevel,
            SSTableManager mgr,
            TableSchema schema,
            PartFormat format,
            BufferAllocator allocator,
            Path tableDir,
            long targetSizeBytes,
            int bloomBitsPerKey) {
        // 读所有 input 行到内存
        List<Map.Entry<List<Object>, RowValue>> allRows = new ArrayList<>();

        for (SSTable sst : inputs) {
            try (SSTableReader reader = new SSTableReader(sst.file(), schema, format, allocator)) {
                BatchIterator it = reader.scan();
                while (it.hasNext()) {
                    VectorSchemaRoot batch = it.next();
                    for (int i = 0; i < batch.getRowCount(); i++) {
                        List<Object> key = new ArrayList<>();
                        List<String> pkCols = schema.primaryKey();
                        for (String pkCol : pkCols) {
                            int colIdx = schema.columnIndex(pkCol);
                            key.add(batch.getVector(colIdx).getObject(i));
                        }
                        Object[] values = new Object[schema.columns().size()];
                        for (int c = 0; c < values.length; c++) {
                            values[c] = batch.getVector(c).getObject(i);
                        }
                        allRows.add(Map.entry(key, new RowValue(RowValue.INSERT, values)));
                    }
                }
                it.close();
            }
        }

        // 按 key 排序，同 key 保留第一个（inputs 中 L0/Ln 在前，版本最新）
        allRows.sort(Map.Entry.comparingByKey(SSTable.KEY_COMPARATOR));
        List<Map.Entry<List<Object>, RowValue>> deduped = new ArrayList<>();
        List<Object> lastKey = null;
        for (var entry : allRows) {
            if (lastKey == null || SSTable.KEY_COMPARATOR.compare(entry.getKey(), lastKey) != 0) {
                // 新 key：添加（第一个遇到的是最新版本，因为 inputs 中较新的文件在前）
                deduped.add(entry);
                lastKey = entry.getKey();
            }
            // 同 key 的后续条目（旧版本）跳过
        }

        // 按目标大小切分输出
        List<SSTable> outputs = new ArrayList<>();
        List<Map.Entry<List<Object>, RowValue>> chunk = new ArrayList<>();
        long chunkBytes = 0;

        for (var entry : deduped) {
            chunk.add(entry);
            chunkBytes += estimateRowBytes(entry.getValue().values());
            if (chunkBytes >= targetSizeBytes) {
                outputs.add(
                        writeChunk(
                                chunk,
                                targetLevel,
                                mgr,
                                schema,
                                format,
                                allocator,
                                tableDir,
                                bloomBitsPerKey));
                chunk.clear();
                chunkBytes = 0;
            }
        }
        if (!chunk.isEmpty()) {
            outputs.add(
                    writeChunk(
                            chunk,
                            targetLevel,
                            mgr,
                            schema,
                            format,
                            allocator,
                            tableDir,
                            bloomBitsPerKey));
        }

        return outputs;
    }

    private SSTable writeChunk(
            List<Map.Entry<List<Object>, RowValue>> chunk,
            int level,
            SSTableManager mgr,
            TableSchema schema,
            PartFormat format,
            BufferAllocator allocator,
            Path tableDir,
            int bloomBitsPerKey) {
        long seq = mgr.nextSeq();
        Path file = tableDir.resolve("sst-L" + level + "-" + String.format("%06d", seq) + ".sst");
        SSTableWriter writer =
                new SSTableWriter(file, level, schema, format, allocator, bloomBitsPerKey);
        Iterator<Map.Entry<List<Object>, RowValue>> iter = chunk.iterator();
        writer.writeFromIterator(iter, chunk.size());
        SSTableReader reader = new SSTableReader(file, schema, format, allocator);
        SSTable sst = reader.metadata();
        reader.close();
        return new SSTable(
                file, level, seq, sst.minKey(), sst.maxKey(), sst.rowCount(), sst.bloom());
    }

    private long estimateRowBytes(Object[] values) {
        long size = 0;
        for (Object v : values) {
            size += v == null ? 8 : (v instanceof String s ? 24 + s.length() * 2L : 16);
        }
        return size;
    }
}
