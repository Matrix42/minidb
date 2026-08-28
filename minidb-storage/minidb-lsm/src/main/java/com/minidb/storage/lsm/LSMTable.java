package com.minidb.storage.lsm;
import com.minidb.storage.common.*;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
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

    // 双缓冲 flush:memTable 满时 swap 出并加入此列表,由后台执行器异步落盘。
    // 列表头 = 最先 swap(最旧),列表尾 = 最新 swap;读路径(scan/getByKey/rowCount)
    // 必须覆盖待落盘的表,否则丢数据。
    private final Object tableLock = new Object();
    private final List<PendingFlush> pendingFlush = new ArrayList<>();
    // TRUNCATE 递增:正在落盘的 flush 任务在 addLevel0 前检查,变了则丢弃(表已清空)。
    private volatile long flushEpoch = 0;
    // 后台 flush 执行器(由 LSMBackgroundExecutor.register 注入);null = 同步退化。
    private volatile LSMBackgroundExecutor flushExecutor;

    // tx-private MemTable: key = txId, value = 该事务在此表的私有 MemTable
    private final ConcurrentHashMap<Long, MemTable> txMemTables = new ConcurrentHashMap<>();

    /** swap 出的待落盘表 + 其 WAL 段代号(落盘完成后 dropSegment 删段)。 */
    private static final class PendingFlush {
        final MemTable memTable;
        final int walGen;
        volatile boolean flushing; // 正在落盘:读路径仍须看到,落盘完成才移除

        PendingFlush(MemTable memTable, int walGen) {
            this.memTable = memTable;
            this.walGen = walGen;
        }
    }

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

        // 恢复 WAL(旧段按代升序 + 当前段——双缓冲下 swap 出的表在 flush 完成前
        // 其 WAL 段必须保留,故恢复重放全部段)
        List<WAL.Entry> walEntries = wal.recover();
        if (!walEntries.isEmpty()) {
            // WAL 有数据 = 上次 crash 前 MemTable 没 flush
            for (WAL.Entry entry : walEntries) {
                // WAL key 二进制类型自描述,恢复时已是 Integer/Long/String 原类型
                // (无需 toString + 重新 parse 三趟)
                List<Object> key = new ArrayList<>(entry.key());
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
            // 不在此 flush:无参 recover() 重放的是全部 WAL(含未提交事务条目),若在此
            // flush 会把未提交数据永久固化进 SSTable,破坏原子性(bug #4)。事务感知恢复
            // 由 recover(committedTxIds) 完成——届时只重放已提交条目,再按需 flush。
            // 无事务环境(测试直连):数据保留在 memTable,查询仍可见,不依赖此处 flush。
        }
    }

    @Override
    public TableSchema schema() {
        return schema;
    }

    @Override
    public BatchIterator scan() {
        return new MergeIterator(memTablesSnapshot(), sstManager, schema, format, allocator).scan();
    }

    @Override
    public BatchIterator scan(List<Object> rangeLo, List<Object> rangeHi) {
        return new MergeIterator(memTablesSnapshot(), sstManager, schema, format, allocator,
                rangeLo, rangeHi).scan();
    }

    @Override
    public BatchIterator scan(int[] projectedColumns) {
        // LSM 是内存 merge,没有文件读时投影——按列拷贝包装(与 SimpleTable 的
        // 存储层投影行为一致,MiniDbScan 据此把 filter 重映射到投影位置)。
        return projectColumns(scan(), projectedColumns);
    }

    /** 按列投影包装迭代器:每批把所选列的 buffer 零拷贝转移(transfer)到新 root。
     *  源批被转移的列 buffer 变空,close 由 source 迭代器统一处理。 */
    private BatchIterator projectColumns(BatchIterator source, int[] cols) {
        if (cols == null) {
            return source;
        }
        return new BatchIterator() {
            private VectorSchemaRoot pending;
            private final List<VectorSchemaRoot> emitted = new ArrayList<>();

            @Override
            public boolean hasNext() {
                while (pending == null && source.hasNext()) {
                    VectorSchemaRoot src = source.next();
                    List<FieldVector> vectors = new ArrayList<>();
                    for (int c : cols) {
                        FieldVector srcV = src.getVector(c);
                        FieldVector dstV = srcV.getField().createVector(allocator);
                        dstV.allocateNew();
                        // 零拷贝转移 buffer(所有权归 dstV;srcV 变空,src root close 无害)
                        srcV.makeTransferPair(dstV).transfer();
                        vectors.add(dstV);
                    }
                    VectorSchemaRoot out = VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
                    // 空投影(COUNT(*) 等,cols 为空数组)产出 0 向量的 root,其 rowCount 推导为 0——必须显式设回,否则聚合算子读到 0 行。
                    // 非空投影下 of() 已按首个向量 valueCount 设值,此处设回同值(幂等)。
                    out.setRowCount(src.getRowCount());
                    emitted.add(out);
                    pending = out;
                }
                return pending != null;
            }

            @Override
            public VectorSchemaRoot next() {
                VectorSchemaRoot out = pending;
                pending = null;
                return out;
            }

            @Override
            public void close() {
                source.close();
                for (VectorSchemaRoot r : emitted) {
                    r.close();
                }
                emitted.clear();
            }
        };
    }

    @Override
    public void writePart(VectorSchemaRoot batch, Operation op) {
        byte kind = switch (op) {
            case INSERT -> RowValue.INSERT;
            case UPDATE -> RowValue.UPDATE;
            case DELETE -> RowValue.DELETE;
        };
        List<Integer> pkIdx = pkIndexes();

        for (int r = 0; r < batch.getRowCount(); r++) {
            List<Object> key = extractKey(batch, r, pkIdx);
            Object[] values = op == Operation.DELETE ? null : extractValues(batch, r);
            RowValue rv = new RowValue(kind, values);
            wal.append(key, rv);
            memTable.put(key, rv);
        }

        if (memTable.needsFlush()) {
            swapAndFlushAsync();
        }
    }

    @Override
    public void writePart(VectorSchemaRoot batch, Operation op, long txId) {
        if (txId == 0) {
            writePart(batch, op); // 非事务路径
            return;
        }
        MemTable txMem = txMemTables.computeIfAbsent(txId,
                k -> new MemTable(schema, flushThresholdBytes));
        byte kind = switch (op) {
            case INSERT -> RowValue.INSERT;
            case UPDATE -> RowValue.UPDATE;
            case DELETE -> RowValue.DELETE;
        };
        List<Integer> pkIdx = pkIndexes();
        for (int r = 0; r < batch.getRowCount(); r++) {
            List<Object> key = extractKey(batch, r, pkIdx);
            Object[] values = op == Operation.DELETE ? null : extractValues(batch, r);
            RowValue rv = new RowValue(kind, values);
            wal.append(txId, key, rv);
            txMem.put(key, rv);
        }
    }

    @Override
    public void commitTx(long txId) {
        MemTable txMem = txMemTables.remove(txId);
        if (txMem == null) return;
        synchronized (tableLock) {
            for (Map.Entry<List<Object>, RowValue> e : txMem.rows()) {
                memTable.put(e.getKey(), e.getValue());
            }
        }
    }

    @Override
    public void rollbackTx(long txId) {
        txMemTables.remove(txId);
    }

    @Override
    public BatchIterator scan(long snapshotTxId) {
        return scan(snapshotTxId, 0L);
    }

    @Override
    public BatchIterator scan(long snapshotTxId, long txId) {
        if (snapshotTxId < 0) {
            return scan(); // READ_UNCOMMITTED：不过滤
        }
        // 快照读:合并 shared MemTable + 当前事务自己的 tx-private MemTable(优先级最高)。
        // 已提交事务的数据已经由 commitTx() 合并进 shared MemTable;txMemTables 此刻
        // 只含未提交(ACTIVE)事务的私有表——自己的可见(合并),他人的不可见(不合并)。
        return new MergeIterator(snapshotWithOwnWrites(txId), sstManager,
                schema, format, allocator, null, null).scan();
    }

    /**
     * 事务快照读的 memTable 列表:[自己事务的私有表(最新,优先)] + [shared + 待落盘]。
     * 自己的写必须覆盖共享基础数据(读自己的写,ACID-C);txId==0 表非事务路径,无自有写。
     */
    private List<MemTable> snapshotWithOwnWrites(long txId) {
        List<MemTable> shared = memTablesSnapshot();
        MemTable own = txId == 0 ? null : txMemTables.get(txId);
        if (own == null) {
            return shared;
        }
        List<MemTable> merged = new ArrayList<>(shared.size() + 1);
        merged.add(own);
        merged.addAll(shared);
        return merged;
    }

    /**
     * 事务恢复:从 WAL 恢复已提交事务的数据,跳过未提交事务的条目。
     * 保留旧 {@link #recover()} 向后兼容(无事务时使用)。
     * 事务感知恢复：构造函数已调用无参 recover() 加载 SSTable 元数据 + 重放全部 WAL，
     * 这里只补做 WAL 过滤——丢弃未提交事务的条目，仅保留已提交的。
     * 不重复加载 SSTable 元数据（避免 levels map 出现重复条目）。
     */
    public void recover(Set<Long> committedTxIds) {
        // 重建 memTable：清空构造函数 recover() 放置的条目（含未提交事务数据），
        // 只重放已提交事务的 WAL 条目。
        this.memTable = new MemTable(schema, flushThresholdBytes);
        List<WAL.Entry> entries = wal.recover(committedTxIds);
        for (WAL.Entry entry : entries) {
            List<Object> key = new ArrayList<>(entry.key());
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
        if (memTable.needsFlush()) {
            flushMemTable();
        }
    }

    /** 从 batch 中提取主键。 */
    private List<Object> extractKey(VectorSchemaRoot batch, int row, List<Integer> pkIdx) {
        List<Object> key = new ArrayList<>();
        for (int idx : pkIdx) {
            Object val = batch.getVector(idx).getObject(row);
            key.add(val == null ? "" : val);
        }
        return key;
    }

    /** 从 batch 中提取所有列值。 */
    private Object[] extractValues(VectorSchemaRoot batch, int row) {
        Object[] values = new Object[schema.columns().size()];
        for (int c = 0; c < values.length; c++) {
            values[c] = batch.getVector(c).getObject(row);
        }
        return values;
    }

    /** 主键列索引列表。 */
    private List<Integer> pkIndexes() {
        List<String> pkCols = schema.primaryKey();
        List<Integer> pkIdx = new ArrayList<>();
        for (String pkCol : pkCols) {
            pkIdx.add(schema.columnIndex(pkCol));
        }
        return pkIdx;
    }

    /**
     * 双缓冲:满表 swap 出交给后台 flush,写路径只做指针交换 + WAL 段切换
     * (O(1),不落盘)。落盘由 {@link LSMBackgroundExecutor#flushAsync} 后台执行。
     */
    private void swapAndFlushAsync() {
        synchronized (tableLock) {
            if (closed) return;
            MemTable full = memTable;
            memTable = new MemTable(schema, flushThresholdBytes);
            // 当前 WAL 段(数据 = 满表)切为旧段,新段给新表——flush 完成后 drop 旧段,
            // 避免 truncate 误删新表数据
            int gen = wal.rotate();
            pendingFlush.add(new PendingFlush(full, gen));
        }
        triggerFlush();
    }

    private void triggerFlush() {
        LSMBackgroundExecutor ex = flushExecutor;
        if (ex == null) {
            flushNextPending(); // 无后台执行器(测试直连):同步 flush,语义一致
        } else {
            ex.flushAsync(this);
        }
    }

    /** 由后台执行器调用:把最早 swap 的表落盘,循环处理完所有待落盘表。 */
    public void flushNextPending() {
        while (true) {
            PendingFlush pf;
            synchronized (tableLock) {
                pf = firstQueued();
                if (pf == null) {
                    return;
                }
                pf.flushing = true;
            }
            flushOne(pf);
        }
    }

    /** 单个待落盘表:过滤 tombstone → 写 SSTable → 移出列表 → drop WAL 段。 */
    private void flushOne(PendingFlush pf) {
        long epoch = flushEpoch;
        // 过滤 DELETE tombstone，SSTable 只存有效行
        List<Map.Entry<List<Object>, RowValue>> normalized = new ArrayList<>();
        for (Map.Entry<List<Object>, RowValue> entry : pf.memTable.rows()) {
            if (entry.getValue().kind() == RowValue.DELETE) continue;
            normalized.add(Map.entry(entry.getKey(), entry.getValue()));
        }

        if (!normalized.isEmpty()) {
            long seq = sstManager.nextSeq();
            Path file = tableDir.resolve("sst-L0-" + String.format("%06d", seq) + ".sst");
            SSTableWriter writer = new SSTableWriter(file, 0, schema, format, allocator, bloomBitsPerKey);
            writer.writeFromIterator(normalized.iterator(), normalized.size());
            SSTableReader reader = new SSTableReader(file, schema, format, allocator);
            SSTable sst = reader.metadata();
            reader.close();
            if (flushEpoch == epoch) {
                sstManager.addLevel0(new SSTable(file, 0, seq, sst.minKey(), sst.maxKey(),
                        sst.rowCount(), sst.bloom()));
            } else {
                // TRUNCATE 在 flush 期间发生:表已清空,丢弃刚写的文件
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        synchronized (tableLock) {
            pendingFlush.remove(pf);
        }
        wal.dropSegment(pf.walGen);
    }

    private PendingFlush firstQueued() {
        for (PendingFlush pf : pendingFlush) {
            if (!pf.flushing) {
                return pf;
            }
        }
        return null;
    }

    /** 查找主键对应的行（用于约束校验与扫描点查）。返回 null 表示不存在或被删除。 */
    @Override
    public RowValue getByKey(List<Object> key) {
        // 内存表(当前 + 待落盘):最新者优先
        for (MemTable mt : memTablesSnapshot()) {
            RowValue rv = mt.get(key);
            if (rv != null) {
                return rv.kind() == RowValue.DELETE ? null : rv;
            }
        }
        // 查 SSTable：先 Bloom filter，再 key range，再读
        // 与 flush 时 encodeKey 的二进制保序编码一致(整数定长 + 符号位翻转)
        byte[] encodedKey = SSTableWriter.encodeKey(key);
        for (int level : sstManager.allLevels()) {
            for (SSTable sst : sstManager.levelFiles(level)) {
                if (!sst.bloom().mightContain(encodedKey)) continue;
                if (!sst.overlaps(key, key)) continue;
                try (SSTableReader reader = new SSTableReader(sst.file(), schema, format, allocator)) {
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
                }
            }
        }
        return null;
    }

    @Override
    public long rowCount() {
        long count = 0;
        // 内存表(当前 + 待落盘)中的有效行（非 DELETE）
        for (MemTable mt : memTablesSnapshot()) {
            for (Map.Entry<List<Object>, RowValue> e : mt.rows()) {
                if (e.getValue().kind() != RowValue.DELETE) {
                    count++;
                }
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
        synchronized (tableLock) {
            memTable = new MemTable(schema, flushThresholdBytes);
            // 挂起的 swap 作废(数据即删);正在落盘的 flush 任务靠 epoch 检查丢弃
            pendingFlush.clear();
            flushEpoch++;
            wal.truncateAll();
        }
        List<SSTable> all = new ArrayList<>(sstManager.allSSTables());
        sstManager.remove(all);
    }

    @Override
    public VectorSchemaRoot newBatchRoot() {
        return VectorSchemaRoot.create(ArrowTypes.arrowSchema(schema), allocator);
    }

    @Override
    public void close() throws Exception {
        // 先停异步提交,再同步清空 pending 与当前表(数据必须落盘)。
        flushExecutor = null;
        flushNextPending();
        if (!memTable.isEmpty()) {
            flushMemTable();
        }
        closed = true;
        wal.close();
    }

    /** 同步落盘当前 MemTable(恢复/close/测试直连用;无后台时写满即走此路径)。 */
    public void flushMemTable() {
        synchronized (tableLock) {
            if (closed) return;
            MemTable oldMt = memTable;
            memTable = new MemTable(schema, flushThresholdBytes);
            if (oldMt.isEmpty()) {
                wal.truncateAll();
                return;
            }

            // 过滤 DELETE tombstone，SSTable 只存有效行
            List<Map.Entry<List<Object>, RowValue>> normalized = new ArrayList<>();
            for (Map.Entry<List<Object>, RowValue> entry : oldMt.rows()) {
                if (entry.getValue().kind() == RowValue.DELETE) continue;
                normalized.add(Map.entry(entry.getKey(), entry.getValue()));
            }

            if (normalized.isEmpty()) {
                wal.truncateAll();
                return;
            }

            long seq = sstManager.nextSeq();
            Path file = tableDir.resolve("sst-L0-" + String.format("%06d", seq) + ".sst");
            SSTableWriter writer = new SSTableWriter(file, 0, schema, format, allocator, bloomBitsPerKey);
            writer.writeFromIterator(normalized.iterator(), normalized.size());
            SSTableReader reader = new SSTableReader(file, schema, format, allocator);
            SSTable sst = reader.metadata();
            reader.close();
            sstManager.addLevel0(new SSTable(file, 0, seq, sst.minKey(), sst.maxKey(),
                    sst.rowCount(), sst.bloom()));
            wal.truncateAll();
        }
    }

    /** 注入后台 flush 执行器(LSMBackgroundExecutor.register 时调用);null = 同步退化。 */
    public void setFlushExecutor(LSMBackgroundExecutor executor) {
        this.flushExecutor = executor;
    }

    /** 待落盘表数量(诊断/测试用:确认 swap 后未被 flush 前读路径仍覆盖)。 */
    int pendingFlushCount() {
        synchronized (tableLock) {
            return pendingFlush.size();
        }
    }

    /** 当前表 + 待落盘表的快照,已按版本优先级排好([0]=当前表最新,往后越旧)。 */
    private List<MemTable> memTablesSnapshot() {
        synchronized (tableLock) {
            List<MemTable> out = new ArrayList<>(pendingFlush.size() + 1);
            out.add(memTable);
            // pending 从最新 swap(列表尾)到最旧(列表头)——版本新者优先
            for (int i = pendingFlush.size() - 1; i >= 0; i--) {
                out.add(pendingFlush.get(i).memTable);
            }
            return out;
        }
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
            case DECIMAL, NUMERIC -> new BigDecimal(s);
            case BOOLEAN -> Boolean.parseBoolean(s);
            case DATE -> SSTableWriter.parseDate(s);
            case TIME -> SSTableWriter.parseTime(s);
            case TIMESTAMP -> SSTableWriter.parseTimestamp(s);
            default -> s; // VARCHAR, CHAR, BINARY 等保持字符串
        };
    }
}
