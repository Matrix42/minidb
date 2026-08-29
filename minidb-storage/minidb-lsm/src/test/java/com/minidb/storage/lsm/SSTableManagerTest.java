package com.minidb.storage.lsm;

import com.minidb.storage.common.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SSTableManagerTest {
    @Test
    void addAndRetrieveByLevel(@TempDir Path dir) {
        SSTableManager mgr = new SSTableManager();
        SSTable sst =
                new SSTable(
                        dir.resolve("sst-L0-000001.sst"),
                        0,
                        1,
                        List.of(1),
                        List.of(10),
                        5,
                        new BloomFilter(10, 100));
        mgr.addLevel0(sst);

        assertEquals(1, mgr.levelFiles(0).size());
        assertTrue(mgr.levelFiles(1).isEmpty());
        assertEquals(1, mgr.allSSTables().size());
    }

    @Test
    void level0SortedBySeqDesc() {
        SSTableManager mgr = new SSTableManager();
        SSTable sst1 =
                new SSTable(
                        Path.of("sst-L0-000001.sst"),
                        0,
                        1,
                        List.of(1),
                        List.of(10),
                        5,
                        new BloomFilter(10, 100));
        SSTable sst2 =
                new SSTable(
                        Path.of("sst-L0-000002.sst"),
                        0,
                        2,
                        List.of(1),
                        List.of(10),
                        5,
                        new BloomFilter(10, 100));
        mgr.addLevel0(sst1);
        mgr.addLevel0(sst2);

        List<SSTable> l0 = mgr.levelFiles(0);
        // 新的在前（seq 大的在前）
        assertEquals(2, l0.get(0).seq());
        assertEquals(1, l0.get(1).seq());
    }

    @Test
    void removeRemovesFromLevel() {
        SSTableManager mgr = new SSTableManager();
        SSTable sst =
                new SSTable(
                        Path.of("sst-L0-000001.sst"),
                        0,
                        1,
                        List.of(1),
                        List.of(10),
                        5,
                        new BloomFilter(10, 100));
        mgr.addLevel0(sst);
        mgr.remove(List.of(sst));
        assertTrue(mgr.levelFiles(0).isEmpty());
    }

    @Test
    void nextSeqIncrements() {
        SSTableManager mgr = new SSTableManager();
        long s1 = mgr.nextSeq();
        long s2 = mgr.nextSeq();
        assertEquals(s1 + 1, s2);
    }

    @Test
    void addLevelNSortedByMinKey() {
        SSTableManager mgr = new SSTableManager();
        // minKey [5] < [10] < [20], 乱序插入验证排序
        SSTable sst1 =
                new SSTable(
                        Path.of("sst-L1-000001.sst"),
                        1,
                        1,
                        List.of(20),
                        List.of(30),
                        5,
                        new BloomFilter(10, 100));
        SSTable sst2 =
                new SSTable(
                        Path.of("sst-L1-000002.sst"),
                        1,
                        2,
                        List.of(5),
                        List.of(10),
                        5,
                        new BloomFilter(10, 100));
        SSTable sst3 =
                new SSTable(
                        Path.of("sst-L1-000003.sst"),
                        1,
                        3,
                        List.of(10),
                        List.of(15),
                        5,
                        new BloomFilter(10, 100));
        mgr.addLevelN(1, List.of(sst1, sst2, sst3));

        List<SSTable> l1 = mgr.levelFiles(1);
        assertEquals(3, l1.size());
        // 按 minKey 升序: [5], [10], [20]
        assertEquals(5, l1.get(0).minKey().get(0));
        assertEquals(10, l1.get(1).minKey().get(0));
        assertEquals(20, l1.get(2).minKey().get(0));
    }

    @Test
    void addLevelNSortedByMinKeyString() {
        SSTableManager mgr = new SSTableManager();
        SSTable sst1 =
                new SSTable(
                        Path.of("sst-L1-000001.sst"),
                        1,
                        1,
                        List.of("c"),
                        List.of("d"),
                        5,
                        new BloomFilter(10, 100));
        SSTable sst2 =
                new SSTable(
                        Path.of("sst-L1-000002.sst"),
                        1,
                        2,
                        List.of("a"),
                        List.of("b"),
                        5,
                        new BloomFilter(10, 100));
        SSTable sst3 =
                new SSTable(
                        Path.of("sst-L1-000003.sst"),
                        1,
                        3,
                        List.of("b"),
                        List.of("c"),
                        5,
                        new BloomFilter(10, 100));
        mgr.addLevelN(1, List.of(sst1, sst2, sst3));

        List<SSTable> l1 = mgr.levelFiles(1);
        assertEquals(3, l1.size());
        assertEquals("a", l1.get(0).minKey().get(0));
        assertEquals("b", l1.get(1).minKey().get(0));
        assertEquals("c", l1.get(2).minKey().get(0));
    }

    @Test
    void addLevelNCompoundKey() {
        SSTableManager mgr = new SSTableManager();
        // 复合键: 先比第一列，再比第二列
        SSTable sst1 =
                new SSTable(
                        Path.of("sst-L1-000001.sst"),
                        1,
                        1,
                        List.of(1, 5),
                        List.of(1, 10),
                        5,
                        new BloomFilter(10, 100));
        SSTable sst2 =
                new SSTable(
                        Path.of("sst-L1-000002.sst"),
                        1,
                        2,
                        List.of(1, 1),
                        List.of(1, 3),
                        5,
                        new BloomFilter(10, 100));
        SSTable sst3 =
                new SSTable(
                        Path.of("sst-L1-000003.sst"),
                        1,
                        3,
                        List.of(2, 0),
                        List.of(2, 10),
                        5,
                        new BloomFilter(10, 100));
        mgr.addLevelN(1, List.of(sst1, sst3, sst2));

        List<SSTable> l1 = mgr.levelFiles(1);
        assertEquals(3, l1.size());
        // [1,1] < [1,5] < [2,0]
        assertEquals(List.of(1, 1), l1.get(0).minKey());
        assertEquals(List.of(1, 5), l1.get(1).minKey());
        assertEquals(List.of(2, 0), l1.get(2).minKey());
    }

    @Test
    void addLevelNIntegerKeysNoClassCast() {
        // 验证修复: SSTable.KEY_COMPARATOR 支持 Integer 类型 key，
        // 不会像 MemTable.KEY_COMPARATOR 那样 (String) 强转抛 ClassCastException
        SSTableManager mgr = new SSTableManager();
        SSTable sst =
                new SSTable(
                        Path.of("sst-L1-000001.sst"),
                        1,
                        1,
                        List.of(100),
                        List.of(200),
                        5,
                        new BloomFilter(10, 100));
        mgr.addLevelN(1, List.of(sst));

        List<SSTable> l1 = mgr.levelFiles(1);
        assertEquals(1, l1.size());
        assertEquals(100, l1.get(0).minKey().get(0));
    }
}
