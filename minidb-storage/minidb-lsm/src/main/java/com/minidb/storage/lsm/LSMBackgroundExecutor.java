package com.minidb.storage.lsm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSMBackgroundExecutor implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LSMBackgroundExecutor.class);

    private final ScheduledExecutorService flushExecutor;
    private final ScheduledExecutorService compactionExecutor;
    private final Map<String, LSMTable> tables = new ConcurrentHashMap<>();
    private final int l0FileLimit;
    private final long targetSizeBytes;
    private final long intervalMs;

    public LSMBackgroundExecutor(int l0FileLimit, long targetSizeBytes, long intervalMs) {
        this.l0FileLimit = l0FileLimit;
        this.targetSizeBytes = targetSizeBytes;
        this.intervalMs = intervalMs;
        this.flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lsm-flush");
            t.setDaemon(true);
            return t;
        });
        this.compactionExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lsm-compaction");
            t.setDaemon(true);
            return t;
        });
    }

    public void register(String key, LSMTable table) {
        tables.put(key, table);
    }

    public void unregister(String key) {
        tables.remove(key);
    }

    public void start() {
        flushExecutor.scheduleWithFixedDelay(() -> {
            for (var entry : tables.entrySet()) {
                try {
                    // Flush is triggered synchronously in LSMTable.writePart.
                    // This periodic check is a safety net for idle tables.
                    entry.getValue().flushMemTable();
                } catch (Exception e) {
                    LOG.error("flush check failed for {}", entry.getKey(), e);
                }
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        compactionExecutor.scheduleWithFixedDelay(() -> {
            for (var entry : tables.entrySet()) {
                try {
                    LSMTable table = entry.getValue();
                    if (table.needsCompaction(l0FileLimit)) {
                        table.compact(targetSizeBytes);
                    }
                } catch (Exception e) {
                    LOG.error("compaction failed for {}", entry.getKey(), e);
                }
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        flushExecutor.shutdown();
        compactionExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(5, TimeUnit.SECONDS);
            compactionExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}