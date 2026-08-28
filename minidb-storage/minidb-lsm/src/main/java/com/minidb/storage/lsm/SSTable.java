package com.minidb.storage.lsm;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

public record SSTable(Path file, int level, long seq,
                      List<Object> minKey, List<Object> maxKey,
                      long rowCount, BloomFilter bloom) {

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final Comparator<Object> CMP = (a, b) -> {
        // 同类型用 Comparable；跨 Number 类型（Integer vs Long）用 long 值比较
        if (a instanceof Number an && b instanceof Number bn) {
            return Long.compare(an.longValue(), bn.longValue());
        }
        return ((Comparable) a).compareTo(b);
    };

    /** 按原始 Comparable 比较 key 列表（不假定 String 类型）。
     *  与 MemTable.KEY_COMPARATOR 不同，后者要求 key 元素全为 String。
     *  SSTable 的 key 可能含 Integer/Long（SSTableReader.decodeKeyValue 解析整数时产生）。 */
    public static final Comparator<List<Object>> KEY_COMPARATOR = SSTable::compareKeys;

    public boolean overlaps(List<Object> rangeMin, List<Object> rangeMax) {
        // 两个 key range 不重叠的条件: maxKey < rangeMin 或 minKey > rangeMax
        // 重叠 = !(maxKey < rangeMin || minKey > rangeMax)
        boolean maxLessThanRangeMin = compareKeys(maxKey, rangeMin) < 0;
        boolean minGreaterThanRangeMax = compareKeys(minKey, rangeMax) > 0;
        return !(maxLessThanRangeMin || minGreaterThanRangeMax);
    }

    static int compareKeys(List<Object> a, List<Object> b) {
        int minLen = Math.min(a.size(), b.size());
        for (int i = 0; i < minLen; i++) {
            int cmp = CMP.compare(a.get(i), b.get(i));
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.size(), b.size());
    }
}