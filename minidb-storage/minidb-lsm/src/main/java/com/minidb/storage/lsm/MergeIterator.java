package com.minidb.storage.lsm;

import com.minidb.storage.common.*;
import java.nio.file.Path;
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 合并 MemTable + 所有 level 的 SSTable，返回去重后的 BatchIterator。
 * 按 key 排序，同 key 取最新版本（MemTable > L0 > L1 > ...），DELETE tombstone 跳过。
 *
 * <p>Key 比较使用 {@link SSTable#KEY_COMPARATOR}（而非 MemTable.KEY_COMPARATOR），
 * 因为 MemTable key 是 String，SSTable key 经 decodeKeyValue 后可能是 Integer/Long。
 * MemTable 的 String key 在进入堆之前先归一化为 Integer/Long/String（与 SSTable 一致），
 * 避免跨类型比较 ClassCastException。
 */
public class MergeIterator {
    private final MemTable memTable;
    private final SSTableManager sstManager;
    private final TableSchema schema;
    private final PartFormat format;
    private final BufferAllocator allocator;
    private final Path tableDir;

    public MergeIterator(MemTable memTable, SSTableManager sstManager,
                         TableSchema schema, PartFormat format,
                         BufferAllocator allocator, Path tableDir) {
        this.memTable = memTable;
        this.sstManager = sstManager;
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
        this.tableDir = tableDir;
    }

    public BatchIterator scan() {
        return new MergeScanIterator();
    }

    /**
     * 将 MemTable 的 String key 归一化为与 SSTable 相同的类型（Integer/Long/String），
     * 使 {@link SSTable#KEY_COMPARATOR} 能安全比较跨来源的 key。
     */
    static List<Object> normalizeKey(List<Object> key) {
        List<Object> normalized = new ArrayList<>(key.size());
        for (Object k : key) {
            normalized.add(SSTableReader.decodeKeyValue((String) k));
        }
        return normalized;
    }

    private class MergeScanIterator implements BatchIterator {
        // 每个 source 的当前行 (key, RowValue, sourcePriority)
        // 使用 SSTable.KEY_COMPARATOR 而非 MemTable.KEY_COMPARATOR：
        // MemTable key 是 String，SSTable key 可能是 Integer/Long，前者 String 强转会 ClassCastException
        private final PriorityQueue<SourceEntry> heap;
        private final List<SSTableReader> readers = new ArrayList<>();
        private final List<Iterator<Map.Entry<List<Object>, RowValue>>> sstIters = new ArrayList<>();
        private VectorSchemaRoot currentBatch = null;
        private int batchPos = 0;
        private final List<Object[]> batchRows = new ArrayList<>();
        private boolean exhausted = false;

        MergeScanIterator() {
            // 优先级：MemTable=0, L0=1, L1=2, L2=3...
            // 数字越小优先级越高；key 相同时优先级高的先出堆
            this.heap = new PriorityQueue<>(
                    Comparator.<SourceEntry, List<Object>>comparing(e -> e.key, SSTable.KEY_COMPARATOR)
                            .thenComparingInt(e -> e.priority));

            // MemTable source (priority 0)
            if (!memTable.isEmpty()) {
                Iterator<Map.Entry<List<Object>, RowValue>> mtIter = memTable.iterator();
                advanceSource(mtIter, 0);
            }

            // SSTable sources
            int priority = 1;
            for (int level : sstManager.allLevels()) {
                for (SSTable sst : sstManager.levelFiles(level)) {
                    SSTableReader reader = new SSTableReader(sst.file(), schema, format, allocator);
                    readers.add(reader);
                    // 读该 SSTable 的所有行到一个 list（简化实现，后续可优化为流式）
                    List<Map.Entry<List<Object>, RowValue>> sstRows = materializeSST(reader);
                    Iterator<Map.Entry<List<Object>, RowValue>> sstIter = sstRows.iterator();
                    advanceSource(sstIter, priority);
                    sstIters.add(sstIter);
                }
                priority++;
            }
        }

        @Override
        public boolean hasNext() {
            if (batchPos < batchRows.size()) return true;
            if (exhausted) return false;
            // 构建下一批
            batchRows.clear();
            buildBatch();
            batchPos = 0;
            return !batchRows.isEmpty();
        }

        @Override
        public VectorSchemaRoot next() {
            if (!hasNext()) throw new NoSuchElementException();
            if (currentBatch != null) {
                currentBatch.close();
            }
            currentBatch = rowsToRoot(batchRows);
            batchPos = batchRows.size(); // 标记已消费
            return currentBatch;
        }

        @Override
        public void close() {
            if (currentBatch != null) {
                currentBatch.close();
            }
            for (SSTableReader reader : readers) {
                reader.close();
            }
        }

        private void buildBatch() {
            List<Object> lastKey = null;
            while (!heap.isEmpty() && batchRows.size() < 4096) {
                SourceEntry entry = heap.poll();
                // 同一个 key 只取第一个（优先级最高的），使用 SSTable.KEY_COMPARATOR
                if (lastKey != null && SSTable.KEY_COMPARATOR.compare(entry.key, lastKey) == 0) {
                    advanceSource(entry.sourceIter, entry.priority);
                    continue;
                }
                lastKey = entry.key;
                // DELETE tombstone：跳过
                if (entry.value.kind() == RowValue.DELETE) {
                    advanceSource(entry.sourceIter, entry.priority);
                    continue;
                }
                batchRows.add(entry.value.values());
                advanceSource(entry.sourceIter, entry.priority);
            }
            if (heap.isEmpty()) {
                exhausted = true;
            }
        }

        private void advanceSource(Iterator<Map.Entry<List<Object>, RowValue>> iter, int priority) {
            if (iter.hasNext()) {
                Map.Entry<List<Object>, RowValue> e = iter.next();
                // MemTable key 是 String，归一化为 Integer/Long/String 以匹配 SSTable key 类型
                List<Object> key = priority == 0 ? normalizeKey(e.getKey()) : e.getKey();
                heap.offer(new SourceEntry(key, e.getValue(), iter, priority));
            }
        }

        private List<Map.Entry<List<Object>, RowValue>> materializeSST(SSTableReader reader) {
            List<Map.Entry<List<Object>, RowValue>> rows = new ArrayList<>();
            BatchIterator it = reader.scan();
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++) {
                    List<Object> key = new ArrayList<>();
                    for (int c = 0; c < schema.primaryKey().size(); c++) {
                        key.add(batch.getVector(c).getObject(i));
                    }
                    Object[] values = new Object[schema.columns().size()];
                    for (int c = 0; c < values.length; c++) {
                        values[c] = batch.getVector(c).getObject(i);
                    }
                    rows.add(Map.entry(key, new RowValue(RowValue.INSERT, values)));
                }
            }
            it.close();
            return rows;
        }
    }

    private VectorSchemaRoot rowsToRoot(List<Object[]> rows) {
        VectorSchemaRoot root = VectorSchemaRoot.create(
                ArrowTypes.arrowSchema(schema), allocator);
        root.allocateNew();
        for (int i = 0; i < rows.size(); i++) {
            SSTableWriter.writeRow(root, i, rows.get(i), schema);
        }
        root.setRowCount(rows.size());
        return root;
    }

    private static class SourceEntry {
        final List<Object> key;
        final RowValue value;
        final Iterator<Map.Entry<List<Object>, RowValue>> sourceIter;
        final int priority;

        SourceEntry(List<Object> key, RowValue value,
                    Iterator<Map.Entry<List<Object>, RowValue>> sourceIter, int priority) {
            this.key = key;
            this.value = value;
            this.sourceIter = sourceIter;
            this.priority = priority;
        }
    }
}