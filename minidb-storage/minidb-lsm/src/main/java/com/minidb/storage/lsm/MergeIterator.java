package com.minidb.storage.lsm;

import com.minidb.storage.common.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    // 按优先级排序的 MemTable 列表:[0]=当前表(最新),越靠后越旧(swap 早的表)。
    // 双缓冲 flush 下待落盘的表在此列表中,scan 必须覆盖,否则丢数据。
    private final List<MemTable> memTables;
    private final SSTableManager sstManager;
    private final TableSchema schema;
    private final PartFormat format;
    private final BufferAllocator allocator;
    // 主键范围裁剪:null = 无界。只读与 [rangeLo, rangeHi] 相交的文件/块
    // (超集语义,行级过滤由调用方按原条件做)。
    private final List<Object> rangeLo;
    private final List<Object> rangeHi;

    public MergeIterator(List<MemTable> memTables, SSTableManager sstManager,
                         TableSchema schema, PartFormat format,
                         BufferAllocator allocator) {
        this(memTables, sstManager, schema, format, allocator, null, null);
    }

    public MergeIterator(List<MemTable> memTables, SSTableManager sstManager,
                         TableSchema schema, PartFormat format,
                         BufferAllocator allocator,
                         List<Object> rangeLo, List<Object> rangeHi) {
        this.memTables = memTables;
        this.sstManager = sstManager;
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
        this.rangeLo = rangeLo;
        this.rangeHi = rangeHi;
    }

    /**
     * 事务快照读构造器:合并 shared MemTable 和已提交的 tx-private MemTable。
     * 已提交的 tx-private 数据已通过 {@link LSMTable#commitTx} 合并到 shared MemTable,
     * 故此处直接使用 shared MemTable;未提交的 tx-private 不在 snapshot 可见范围内。
     * txMemTables 和 snapshotTxId 参数保留供后续扩展。
     */
    public MergeIterator(List<MemTable> memTables,
                         ConcurrentHashMap<Long, MemTable> txMemTables,
                         SSTableManager sstManager, TableSchema schema,
                         PartFormat format, BufferAllocator allocator,
                         List<Object> rangeLo, List<Object> rangeHi,
                         long snapshotTxId) {
        this.memTables = memTables;
        this.sstManager = sstManager;
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
        this.rangeLo = rangeLo;
        this.rangeHi = rangeHi;
        // 简化实现:已提交的 tx-private 数据已通过 commitTx() 合并到 shared MemTable,
        // 未提交的 tx-private MemTable 不在 snapshot 可见范围内。
    }

    public BatchIterator scan() {
        return new MergeScanIterator();
    }

    private class MergeScanIterator implements BatchIterator {
        // 源在构造时固定（1 个 MemTable + 每 SSTable 文件 1 个），之后不变。
        // 堆用裸 int[] 存「源下标」，比较时直接对源当前行的 key 判序——
        // 相比 PriorityQueue<Source>：不装箱、每消费一行只做一次根替换下滤
        // （poll 下滤 + offer 上滤各一次 → 一次下滤）、初始化一次性 heapify 而非
        // 逐次 siftUp、源槽跨行复用无每行对象分配。
        private final Source[] sources;
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
            // 优先级：当前 MemTable=0, 待 flush 表按 swap 新→旧 = 1..k, L0=k+1, L1=k+2...
            // 数字越小优先级越高；key 相同时优先级高的先出堆。
            List<SSTable> sstFiles = sstManager.allSSTables();
            sources = new Source[memTables.size() + sstFiles.size()];
            heap = new int[sources.length];

            // MemTable sources:memTables 已按优先级排好([0]=当前表最新)
            int idx = 0;
            for (MemTable mt : memTables) {
                sources[idx] = new MemTableSource(mt.iterator(), idx);
                idx++;
            }

            // SSTable sources。priority 直接用 idx + sst.level()+1(比按 allLevels() 的
            // keySet 迭代序号更确定——CHM 的迭代序不保证升序,而 level 即优先级语义)。
            for (SSTable sst : sstFiles) {
                // 文件级裁剪:与查询范围不相交的文件直接跳过(块级裁剪在 reader 内)
                if (!overlapsRange(sst)) {
                    continue;
                }
                SSTableReader reader = new SSTableReader(sst.file(), schema, format, allocator);
                readers.add(reader);
                // 流式源:不物化整个文件,按需从 reader 拉批——每源常驻当前一批,
                // 内存 O(批大小 × 文件数) 而非 O(文件总行数)。
                BatchIterator it = rangeLo == null && rangeHi == null
                        ? reader.scan() : reader.scan(rangeLo, rangeHi);
                sources[idx++] = new SstSource(it, schema, idx + sst.level(), sst.seq());
            }

            // 每个源读首行入堆，再一次性自底向上 heapify（O(N)）——替代逐次 offer
            // 的 siftUp。之后每消费一行只对根做一次下滤（O(log N)），无上滤。
            for (int i = 0; i < idx; i++) {
                if (sources[i].nextRow()) {
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
            // 关闭所有已发出的 batch + 未发出的当前批 + 各源的流式迭代器。
            for (VectorSchemaRoot batch : emitted) {
                batch.close();
            }
            emitted.clear();
            // currentBatch 已在 emitted 中(若发过),避免 double close。
            for (Source s : sources) {
                if (s != null) {
                    s.close();
                }
            }
            for (SSTableReader reader : readers) {
                reader.close();
            }
        }

        private void buildBatch() {
            List<Object> lastKey = null;
            while (heapSize > 0 && batchRows.size() < 4096) {
                int src = heap[0]; // 根 = 全局最小（主键升序 → 优先级升序 → seq 降序）
                Source s = sources[src];
                // 同一个 key 只取第一个（优先级最高的），使用 SSTable.compareKeys
                if (lastKey != null && SSTable.compareKeys(s.key(), lastKey) == 0) {
                    advanceSource(src);
                    continue;
                }
                // 拷贝 lastKey:源 key 槽跨行复用(nextRow 会 clear 同一对象),
                // 堆外比较必须持有独立副本。
                lastKey = new ArrayList<>(s.key());
                // DELETE tombstone：跳过
                if (s.kind() == RowValue.DELETE) {
                    advanceSource(src);
                    continue;
                }
                batchRows.add(s.values());
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
            Source s = sources[src];
            if (s.nextRow()) {
                siftDown(0);
            } else {
                heapSize--;
                if (heapSize > 0) {
                    heap[0] = heap[heapSize];
                    siftDown(0);
                }
            }
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
            Source sa = sources[a];
            Source sb = sources[b];
            int cmp = SSTable.compareKeys(sa.key(), sb.key());
            if (cmp != 0) return cmp < 0;
            if (sa.priority != sb.priority) return sa.priority < sb.priority;
            return sa.seq > sb.seq;
        }

        /** 文件与查询范围 [rangeLo, rangeHi] 相交 ⟺ minKey <= hi && maxKey >= lo。 */
        private boolean overlapsRange(SSTable sst) {
            return SSTableReader.keyLte(sst.minKey(), rangeHi)
                    && SSTableReader.keyGte(sst.maxKey(), rangeLo);
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
     * 合并源(1 个 MemTable + 每 SSTable 文件 1 个)的「当前行槽」抽象。
     * nextRow 推进到下一行,之后 key()/values()/kind() 返回新当前行(借用,
     * 下轮 nextRow 后失效)。对象在构造时一次性分配、跨行复用。
     */
    private abstract static class Source {
        final int priority; // MemTable=0, L0=1, L1=2...
        final long seq;     // SSTable 全局序号，同优先级内越大越新

        Source(int priority, long seq) {
            this.priority = priority;
            this.seq = seq;
        }

        /** 推进到下一行;false=耗尽。之后 key()/values() 返回新当前行。 */
        abstract boolean nextRow();

        /** 当前行主键(借用,勿改;下轮 nextRow 后失效)。 */
        abstract List<Object> key();

        /** 当前行值数组(借用,勿改;下轮 nextRow 后失效)。 */
        abstract Object[] values();

        /** DELETE tombstone 标记(MemTable 行可能有;磁盘行恒 INSERT)。 */
        abstract byte kind();

        /** 释放持有的批/通道。 */
        abstract void close();
    }

    /** MemTable 源:内存 entry 迭代器,零拷贝引用 map 里的行对象。 */
    private static final class MemTableSource extends Source {
        private final Iterator<Map.Entry<List<Object>, RowValue>> iter;
        private Map.Entry<List<Object>, RowValue> current;

        MemTableSource(Iterator<Map.Entry<List<Object>, RowValue>> iter, int priority) {
            super(priority, Long.MAX_VALUE);
            this.iter = iter;
        }

        @Override
        boolean nextRow() {
            if (!iter.hasNext()) {
                return false;
            }
            current = iter.next();
            return true;
        }

        @Override
        List<Object> key() {
            return current.getKey();
        }

        @Override
        Object[] values() {
            return current.getValue().values();
        }

        @Override
        byte kind() {
            return current.getValue().kind();
        }

        @Override
        void close() {
            // MemTable 无持有资源
        }
    }

    /**
     * SSTable 源:流式按批从 reader 拉取,每源常驻当前一批(内存 O(批大小) 而非
     * O(文件行数))。换批时 reader 释放旧批,故当前行必须拷进独立 key 槽与
     * 独立 values 数组(batchRows 消费的是 values 引用,独立数组保证跨行稳定)。
     */
    private static final class SstSource extends Source {
        private final BatchIterator it;
        private final int pkCols;
        private final int totalCols;
        private VectorSchemaRoot batch;
        private int pos;
        private final List<Object> key = new ArrayList<>();
        private Object[] rowValues;

        SstSource(BatchIterator it, TableSchema schema, int priority, long seq) {
            super(priority, seq);
            this.it = it;
            this.pkCols = schema.primaryKey().size();
            this.totalCols = schema.columns().size();
        }

        @Override
        boolean nextRow() {
            if (batch == null || pos >= batch.getRowCount()) {
                if (!it.hasNext()) {
                    return false;
                }
                batch = it.next();
                pos = 0;
            }
            key.clear();
            for (int c = 0; c < pkCols; c++) {
                key.add(batch.getVector(c).getObject(pos));
            }
            Object[] values = new Object[totalCols];
            for (int c = 0; c < totalCols; c++) {
                values[c] = batch.getVector(c).getObject(pos);
            }
            rowValues = values;
            pos++;
            return true;
        }

        @Override
        List<Object> key() {
            return key;
        }

        @Override
        Object[] values() {
            return rowValues;
        }

        @Override
        byte kind() {
            return RowValue.INSERT; // 磁盘行恒 INSERT(MemTable 的 DELETE 不进 flush)
        }

        @Override
        void close() {
            it.close();
        }
    }
}
