package com.minidb.storage.lsm;

import com.minidb.storage.common.*;
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 合并 MemTable + 所有 level 的 SSTable，返回去重后的 BatchIterator。
 * 按 key 排序，同 key 取最新版本（MemTable > L0 > L1 > ...），DELETE tombstone 跳过。
 *
 * <p>MemTable 和 SSTable 使用统一的 {@link SSTable#KEY_COMPARATOR}（raw Comparable），
 * key 类型一致（Integer/Long/String），无需边界转换。
 */
public class MergeIterator {
    private final MemTable memTable;
    private final SSTableManager sstManager;
    private final TableSchema schema;
    private final PartFormat format;
    private final BufferAllocator allocator;

    public MergeIterator(MemTable memTable, SSTableManager sstManager,
                         TableSchema schema, PartFormat format,
                         BufferAllocator allocator) {
        this.memTable = memTable;
        this.sstManager = sstManager;
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
    }

    public BatchIterator scan() {
        return new MergeScanIterator();
    }

    private class MergeScanIterator implements BatchIterator {
        private final PriorityQueue<SourceEntry> heap;
        private final List<SSTableReader> readers = new ArrayList<>();
        private VectorSchemaRoot currentBatch = null;
        private int batchPos = 0;
        private final List<Object[]> batchRows = new ArrayList<>();
        private boolean exhausted = false;

        MergeScanIterator() {
            // 优先级：MemTable=0, L0=1, L1=2, L2=3...
            // 数字越小优先级越高；key 相同时优先级高的先出堆
            this.heap = new PriorityQueue<>(
                    Comparator.<SourceEntry, List<Object>>comparing(e -> e.key, SSTable.KEY_COMPARATOR)
                            .thenComparingInt(e -> e.priority)
                            .thenComparingLong(e -> -e.seq));

            // MemTable source (priority 0)
            if (!memTable.isEmpty()) {
                Iterator<Map.Entry<List<Object>, RowValue>> mtIter = memTable.iterator();
                advanceSource(mtIter, 0, Long.MAX_VALUE);
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
                    advanceSource(sstIter, priority, sst.seq());
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
                    advanceSource(entry.sourceIter, entry.priority, entry.seq);
                    continue;
                }
                lastKey = entry.key;
                // DELETE tombstone：跳过
                if (entry.value.kind() == RowValue.DELETE) {
                    advanceSource(entry.sourceIter, entry.priority, entry.seq);
                    continue;
                }
                batchRows.add(entry.value.values());
                advanceSource(entry.sourceIter, entry.priority, entry.seq);
            }
            if (heap.isEmpty()) {
                exhausted = true;
            }
        }

        private void advanceSource(Iterator<Map.Entry<List<Object>, RowValue>> iter, int priority, long seq) {
            if (iter.hasNext()) {
                Map.Entry<List<Object>, RowValue> e = iter.next();
                heap.offer(new SourceEntry(e.getKey(), e.getValue(), iter, priority, seq));
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
        if (!rows.isEmpty()) {
            // allocateNew 后 vector valueCount=0，setRowCount 确保有效性缓冲区已分配
            root.setRowCount(rows.size());
        }
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
        final long seq;

        SourceEntry(List<Object> key, RowValue value,
                    Iterator<Map.Entry<List<Object>, RowValue>> sourceIter, int priority, long seq) {
            this.key = key;
            this.value = value;
            this.sourceIter = sourceIter;
            this.priority = priority;
            this.seq = seq;
        }
    }
}