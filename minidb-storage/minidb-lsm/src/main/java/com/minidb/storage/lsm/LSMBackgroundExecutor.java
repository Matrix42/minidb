package com.minidb.storage.lsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LSMBackgroundExecutor implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LSMBackgroundExecutor.class);

    private final ScheduledExecutorService compactionExecutor;
    private final ExecutorService flushExecutor;
    private final Map<String, LSMTable> tables = new ConcurrentHashMap<>();
    private final int l0FileLimit;
    private final long targetSizeBytes;
    private final long intervalMs;

    public LSMBackgroundExecutor(int l0FileLimit, long targetSizeBytes, long intervalMs) {
        this.l0FileLimit = l0FileLimit;
        this.targetSizeBytes = targetSizeBytes;
        this.intervalMs = intervalMs;
        this.compactionExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "lsm-compaction");
                            t.setDaemon(true);
                            return t;
                        });
        this.flushExecutor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "lsm-flush");
                            t.setDaemon(true);
                            return t;
                        });
    }

    public void register(String key, LSMTable table) {
        table.setFlushExecutor(this); // 注入后台 flush,写路径 swap 后异步落盘
        tables.put(key, table);
    }

    public void unregister(String key) {
        tables.remove(key);
    }

    /** 写路径 swap 后调用:提交该表的落盘任务(单线程串行,表间排队)。 */
    public void flushAsync(LSMTable table) {
        flushExecutor.execute(
                () -> {
                    try {
                        table.flushNextPending();
                    } catch (Exception e) {
                        LOG.error("async flush failed", e);
                    }
                });
    }

    public void start() {
        // 双缓冲 flush 由 writePart 换表时触发(flushAsync),此处只做周期 compaction。
        compactionExecutor.scheduleWithFixedDelay(
                () -> {
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
                },
                intervalMs,
                intervalMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        // shutdown 后不再调度新任务;等当前正在执行的 compaction/flush 全部结束再返回。
        // 不能用固定短超时(如 5s)——大表 bulk load 触发的 compaction 可能持续数十秒,
        // 超时返回会让仍在运行的 compaction 用到已关闭的 allocator → IllegalStateException
        // + 内存泄漏。compaction 任务是有界的(处理完当前文件集即结束),等待是安全的。
        compactionExecutor.shutdown();
        flushExecutor.shutdown();
        awaitTerminated(compactionExecutor);
        awaitTerminated(flushExecutor);
    }

    private static void awaitTerminated(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                // 仍在运行:继续等
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
