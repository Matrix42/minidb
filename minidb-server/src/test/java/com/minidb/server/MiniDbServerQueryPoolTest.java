package com.minidb.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 查询线程池行为:固定大小(非 cached 无界)、大小取配置 server.query-threads、 超额任务排队而非建线程。 */
class MiniDbServerQueryPoolTest {

    @Test
    void queryPoolIsFixedAtDefaultThreads(@TempDir Path dir) throws Exception {
        try (MiniDbServer server = new MiniDbServer()) {
            server.start(0, dir);
            ThreadPoolExecutor pool = (ThreadPoolExecutor) server.queryPool();
            int expected = Math.max(1, Runtime.getRuntime().availableProcessors());
            assertEquals(expected, pool.getCorePoolSize());
            assertEquals(expected, pool.getMaximumPoolSize(), "fixed pool max=core,线程数封顶");
        }
    }

    @Test
    void queryPoolHonorsConfig(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("config.yaml"), "server:\n  query-threads: 2\n");
        try (MiniDbServer server = new MiniDbServer()) {
            server.start(0, dir);
            ThreadPoolExecutor pool = (ThreadPoolExecutor) server.queryPool();
            assertEquals(2, pool.getCorePoolSize());
        }
    }

    @Test
    void excessTasksQueueInsteadOfSpawningThreads(@TempDir Path dir) throws Exception {
        try (MiniDbServer server = new MiniDbServer()) {
            server.start(0, dir);
            ThreadPoolExecutor pool = (ThreadPoolExecutor) server.queryPool();
            int n = pool.getCorePoolSize();
            CountDownLatch block = new CountDownLatch(1);
            CountDownLatch started = new CountDownLatch(n);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < n * 4; i++) {
                futures.add(
                        pool.submit(
                                () -> {
                                    started.countDown();
                                    try {
                                        block.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }));
            }
            // fixed pool 线程惰性创建,最多 corePoolSize 个;超额任务进入队列。
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread.sleep(200); // 给线程池时间把线程数涨到上限
            assertTrue(pool.getPoolSize() <= n, "固定池线程数必须封顶,当前 " + pool.getPoolSize());
            assertTrue(pool.getQueue().size() > 0, "超额任务应排队");
            block.countDown();
            for (Future<?> f : futures) {
                f.get(5, TimeUnit.SECONDS);
            }
        }
    }
}
