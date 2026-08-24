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
        // 源在构造时固定（1 个 MemTable + 每 SSTable 文件 1 个），之后不变。
        // 堆用裸 int[] 存「源下标」，比较时直接对源当前行的 key 判序——
        // 相比 PriorityQueue<SourceEntry>：不装箱、每消费一行只做一次根替换下滤
        // （poll 下滤 + offer 上滤各一次 → 一次下滤）、初始化一次性 heapify 而非
        // 逐次 siftUp、SourceSlot 跨行复用无每行对象分配。
        private final SourceSlot[] sources;
        private final int[] heap;
        private int heapSize = 0;

        private final List<SSTableReader> readers = new ArrayList<>();
        private VectorSchemaRoot currentBatch = null;
        private int batchPos = 0;
        private final List<Object[]> batchRows = new ArrayList<>();
        private boolean exhausted = false;
        // 已通过 next() 返回给调用方的 batch。调用方不负责 close(所有权模型与
        // SimpleTable 一致:靠迭代器 close 统一释放),故在此累积,close() 时全关。
        // 原 next() 关上一批 currentBatch 会 close 掉调用方仍在使用的 batch
        // (materializeColumns 把它收进 batches list),导致 use-after-close 和
        // merged.copyRow 读已释放 buffer —— 内存泄漏 + 潜在数据错乱。
        private final List<VectorSchemaRoot> emitted = new ArrayList<>();

        MergeScanIterator() {
            // 优先级：MemTable=0, L0=1, L1=2, L2=3...
            // 数字越小优先级越高；key 相同时优先级高的先出堆。
            List<SSTable> sstFiles = sstManager.allSSTables();
            sources = new SourceSlot[sstFiles.size() + 1];
            heap = new int[sources.length];

            // MemTable source (priority 0)
            sources[0] = newSource(memTable.iterator(), 0, Long.MAX_VALUE);

            // SSTable sources。priority 直接用 sst.level()+1（比按 allLevels() 的
            // keySet 迭代序号更确定——CHM 的迭代序不保证升序，而 level 即优先级语义）。
            int idx = 1;
            for (SSTable sst : sstFiles) {
                SSTableReader reader = new SSTableReader(sst.file(), schema, format, allocator);
                readers.add(reader);
                // 读该 SSTable 的所有行到一个 list（简化实现，后续可优化为流式）
                List<Map.Entry<List<Object>, RowValue>> sstRows = materializeSST(reader);
                sources[idx++] = newSource(sstRows.iterator(), sst.level() + 1, sst.seq());
            }

            // 每个源读首行入堆，再一次性自底向上 heapify（O(N)）——替代逐次 offer
            // 的 siftUp。之后每消费一行只对根做一次下滤（O(log N)），无上滤。
            for (int i = 0; i < idx; i++) {
                SourceSlot s = sources[i];
                if (s.iter.hasNext()) {
                    Map.Entry<List<Object>, RowValue> e = s.iter.next();
                    s.key = e.getKey();
                    s.value = e.getValue();
                    heap[heapSize++] = i;
                }
            }
            heapify();
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
            // 不关闭上一批 currentBatch:它已通过前一次 next() 返回给调用方,
            // 调用方可能仍在使用(materializeColumns 收进 batches list 后 copyRow)。
            // 改为累积到 emitted,由 close() 统一释放。
            currentBatch = rowsToRoot(batchRows);
            emitted.add(currentBatch);
            batchPos = batchRows.size(); // 标记已消费
            return currentBatch;
        }

        @Override
        public void close() {
            // 关闭所有已发出的 batch + 未发出的当前批 + SSTable reader。
            for (VectorSchemaRoot batch : emitted) {
                batch.close();
            }
            emitted.clear();
            // currentBatch 已在 emitted 中(若发过),避免 double close。
            for (SSTableReader reader : readers) {
                reader.close();
            }
        }

        private void buildBatch() {
            List<Object> lastKey = null;
            while (heapSize > 0 && batchRows.size() < 4096) {
                int src = heap[0]; // 根 = 全局最小（主键升序 → 优先级升序 → seq 降序）
                SourceSlot s = sources[src];
                // 同一个 key 只取第一个（优先级最高的），使用 SSTable.compareKeys
                if (lastKey != null && SSTable.compareKeys(s.key, lastKey) == 0) {
                    advanceSource(src);
                    continue;
                }
                lastKey = s.key;
                // DELETE tombstone：跳过
                if (s.value.kind() == RowValue.DELETE) {
                    advanceSource(src);
                    continue;
                }
                batchRows.add(s.value.values());
                advanceSource(src);
            }
            if (heapSize == 0) {
                exhausted = true;
            }
        }

        /**
         * 消费源 src 的当前行后推进到下一行：
         * <ul>
         *   <li>还有下一行：读入同一 slot（复用对象），从堆根下滤恢复堆序——一次
         *       O(log N) 下滤，替代 PriorityQueue 的 poll 下滤 + offer 上滤各一次；</li>
         *   <li>源耗尽：根与末元素交换（堆缩小），再下滤。</li>
         * </ul>
         */
        private void advanceSource(int src) {
            SourceSlot s = sources[src];
            if (s.iter.hasNext()) {
                Map.Entry<List<Object>, RowValue> e = s.iter.next();
                s.key = e.getKey();
                s.value = e.getValue();
                siftDown(0);
            } else {
                heapSize--;
                if (heapSize > 0) {
                    heap[0] = heap[heapSize];
                    siftDown(0);
                }
            }
        }

        private static SourceSlot newSource(
                Iterator<Map.Entry<List<Object>, RowValue>> iter, int priority, long seq) {
            SourceSlot s = new SourceSlot(priority, seq);
            s.iter = iter;
            return s;
        }

        /** 自底向上建堆：对最后一个非叶节点起逐个下滤。 */
        private void heapify() {
            for (int i = heapSize / 2 - 1; i >= 0; i--) {
                siftDown(i);
            }
        }

        /** 小顶堆下滤：把 heap[i] 下沉到合适位置。 */
        private void siftDown(int i) {
            int key = heap[i];
            int half = heapSize >>> 1;
            while (i < half) {
                int child = (i << 1) + 1;
                int right = child + 1;
                if (right < heapSize && less(heap[right], heap[child])) {
                    child = right;
                }
                if (!less(heap[child], key)) {
                    break;
                }
                heap[i] = heap[child];
                i = child;
            }
            heap[i] = key;
        }

        /** 源 a 是否应排在源 b 之前：主键升序，同键优先级升序，同优先级 seq 降序。 */
        private boolean less(int a, int b) {
            SourceSlot sa = sources[a];
            SourceSlot sb = sources[b];
            int cmp = SSTable.compareKeys(sa.key, sb.key);
            if (cmp != 0) return cmp < 0;
            if (sa.priority != sb.priority) return sa.priority < sb.priority;
            return sa.seq > sb.seq;
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

    /**
     * 单个合并源（1 个 MemTable + 每 SSTable 文件 1 个）的当前行。
     * 对象在构造时一次性分配、跨行复用——消费一行后 {@code advanceSource} 原地换新行，
     * 避免 PriorityQueue 方案每行新建一个 entry 对象。key 在入堆期间非 null。
     */
    private static final class SourceSlot {
        List<Object> key; // 当前行主键
        RowValue value;   // 当前行值
        Iterator<Map.Entry<List<Object>, RowValue>> iter; // 该源剩余行
        final int priority; // MemTable=0, L0=1, L1=2...
        final long seq;     // SSTable 全局序号，同优先级内越大越新

        SourceSlot(int priority, long seq) {
            this.priority = priority;
            this.seq = seq;
        }
    }
}
