package com.minidb.storage.lsm;

import static org.junit.jupiter.api.Assertions.*;
import com.minidb.storage.common.*;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableManagerTest {
    @Test
    void addAndRetrieveByLevel(@TempDir Path dir) {
        SSTableManager mgr = new SSTableManager();
        SSTable sst = new SSTable(dir.resolve("sst-L0-000001.sst"), 0, 1,
                List.of(1), List.of(10), 5, new BloomFilter(10, 100));
        mgr.addLevel0(sst);

        assertEquals(1, mgr.levelFiles(0).size());
        assertTrue(mgr.levelFiles(1).isEmpty());
        assertEquals(1, mgr.allSSTables().size());
    }

    @Test
    void level0SortedBySeqDesc() {
        SSTableManager mgr = new SSTableManager();
        SSTable sst1 = new SSTable(Path.of("sst-L0-000001.sst"), 0, 1,
                List.of(1), List.of(10), 5, new BloomFilter(10, 100));
        SSTable sst2 = new SSTable(Path.of("sst-L0-000002.sst"), 0, 2,
                List.of(1), List.of(10), 5, new BloomFilter(10, 100));
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
        SSTable sst = new SSTable(Path.of("sst-L0-000001.sst"), 0, 1,
                List.of(1), List.of(10), 5, new BloomFilter(10, 100));
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
}