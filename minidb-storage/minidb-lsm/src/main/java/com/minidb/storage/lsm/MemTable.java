package com.minidb.storage.lsm;

import com.minidb.storage.common.RowValue;
import com.minidb.storage.common.TableSchema;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {
    private final TableSchema schema;
    private final long flushThresholdBytes;
    private final ConcurrentSkipListMap<List<Object>, RowValue> rows;
    private final AtomicLong estimatedBytes;

    // 统一 key 比较器：Number 类型用 long 值比较，其他用 raw Comparable。
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final Comparator<List<Object>> KEY_COMPARATOR = (a, b) -> {
        int minLen = Math.min(a.size(), b.size());
        for (int i = 0; i < minLen; i++) {
            Object ai = a.get(i), bi = b.get(i);
            int cmp;
            if (ai instanceof Number an && bi instanceof Number bn) {
                cmp = Long.compare(an.longValue(), bn.longValue());
            } else {
                cmp = ((Comparable) ai).compareTo(bi);
            }
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.size(), b.size());
    };

    public MemTable(TableSchema schema, long flushThresholdBytes) {
        this.schema = schema;
        this.flushThresholdBytes = flushThresholdBytes;
        this.rows = new ConcurrentSkipListMap<>(KEY_COMPARATOR);
        this.estimatedBytes = new AtomicLong(0);
    }

    public TableSchema schema() {
        return schema;
    }

    public void put(List<Object> key, RowValue value) {
        RowValue old = rows.put(key, value);
        long delta = estimateSize(key, value);
        if (old != null) {
            delta -= estimateSize(key, old);
        }
        estimatedBytes.addAndGet(delta);
    }

    public RowValue get(List<Object> key) {
        return rows.get(key);
    }

    public long estimatedBytes() {
        return estimatedBytes.get();
    }

    public boolean needsFlush() {
        return estimatedBytes.get() >= flushThresholdBytes;
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    public Iterator<Map.Entry<List<Object>, RowValue>> iterator() {
        return rows.entrySet().iterator();
    }

    public int size() {
        return rows.size();
    }

    /** 暴露所有 entry 供 rowCount 和 flush 使用（快照视图） */
    public java.util.Set<Map.Entry<List<Object>, RowValue>> rows() {
        return rows.entrySet();
    }

    /** 每个 entry 的近似内存开销：key 对象引用 + value 对象引用 + 数据 */
    private long estimateSize(List<Object> key, RowValue value) {
        long size = 64; // map entry overhead (approx)
        for (Object k : key) {
            size += k == null ? 8 : 16;
        }
        if (value.values() != null) {
            for (Object v : value.values()) {
                size += v == null ? 8 : (v instanceof String s ? 24 + s.length() * 2L : 16);
            }
        }
        return size;
    }
}