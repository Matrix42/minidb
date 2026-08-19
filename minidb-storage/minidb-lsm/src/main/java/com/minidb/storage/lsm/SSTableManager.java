package com.minidb.storage.lsm;

import com.minidb.storage.common.PartFormat;
import com.minidb.storage.common.TableSchema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;

public class SSTableManager {
    // level → sorted SSTables (L0: 按 seq 降序; L1+: 按 minKey 升序)
    private final ConcurrentSkipListMap<Integer, List<SSTable>> levels = new ConcurrentSkipListMap<>();
    private final AtomicLong seq = new AtomicLong(0);

    public void addLevel0(SSTable sst) {
        levels.compute(0, (k, list) -> {
            if (list == null) list = new ArrayList<>();
            List<SSTable> newList = new ArrayList<>(list);
            // L0: 按 seq 降序（新的在前）
            int pos = 0;
            while (pos < newList.size() && newList.get(pos).seq() > sst.seq()) {
                pos++;
            }
            newList.add(pos, sst);
            return newList;
        });
    }

    public void addLevelN(int level, List<SSTable> ssts) {
        levels.compute(level, (k, list) -> {
            if (list == null) list = new ArrayList<>();
            List<SSTable> newList = new ArrayList<>(list);
            newList.addAll(ssts);
            // L1+: 按 minKey 升序
            newList.sort(Comparator.comparing(SSTable::minKey, MemTable.KEY_COMPARATOR));
            return newList;
        });
    }

    public List<SSTable> levelFiles(int level) {
        List<SSTable> list = levels.get(level);
        return list == null ? List.of() : Collections.unmodifiableList(list);
    }

    public List<SSTable> allSSTables() {
        List<SSTable> all = new ArrayList<>();
        for (List<SSTable> list : levels.values()) {
            all.addAll(list);
        }
        return all;
    }

    public Set<Integer> allLevels() {
        return levels.keySet();
    }

    public void remove(List<SSTable> toRemove) {
        Set<Path> removeSet = new HashSet<>();
        for (SSTable sst : toRemove) {
            removeSet.add(sst.file());
        }
        for (Map.Entry<Integer, List<SSTable>> entry : levels.entrySet()) {
            List<SSTable> filtered = new ArrayList<>();
            for (SSTable sst : entry.getValue()) {
                if (!removeSet.contains(sst.file())) {
                    filtered.add(sst);
                }
            }
            if (filtered.isEmpty()) {
                levels.remove(entry.getKey());
            } else {
                levels.put(entry.getKey(), filtered);
            }
        }
        // 删除文件
        for (SSTable sst : toRemove) {
            try {
                Files.deleteIfExists(sst.file());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public long nextSeq() {
        return seq.incrementAndGet();
    }

    /** 从已有目录加载 SSTable 文件，恢复元数据（重启用） */
    public void loadExisting(Path tableDir, TableSchema schema,
                              PartFormat format, BufferAllocator allocator) {
        if (!Files.exists(tableDir)) return;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(tableDir, "sst-*.sst")) {
            for (Path file : ds) {
                SSTableReader reader = new SSTableReader(file, schema, format, allocator);
                SSTable sst = reader.metadata();
                // 从文件名解析 level 和 seq
                String name = file.getFileName().toString();
                // sst-L<level>-<seq>.sst
                int lStart = name.indexOf('L') + 1;
                int lEnd = name.indexOf('-', lStart);
                int level = Integer.parseInt(name.substring(lStart, lEnd));
                int sStart = lEnd + 1;
                int sEnd = name.lastIndexOf('.');
                long fileSeq = Long.parseLong(name.substring(sStart, sEnd));
                SSTable loaded = new SSTable(file, level, fileSeq,
                        sst.minKey(), sst.maxKey(), sst.rowCount(), sst.bloom());
                if (level == 0) {
                    addLevel0(loaded);
                } else {
                    addLevelN(level, List.of(loaded));
                }
                if (fileSeq > seq.get()) {
                    seq.set(fileSeq);
                }
                reader.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}