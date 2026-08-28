package com.minidb.tpcds;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 直接用 {@link QueryExecutor}(不走 MiniDbServer/JDBC 网络层)跑 TPC-DS 查询,
 * 验证内核执行路径能跑通。区别于 {@link TpcdsEndToEndTest}(走网络 + JDBC),
 * 本测试更快、更稳定,失败时能直接看到内核抛出的异常。
 */
class TpcdsQueryExecutorTest {

    /**
     * 只跑前 10 条保持单测快(覆盖 join/EXISTS 子查询/窗口/聚合等模式);全量 99 条在
     * 0.01 scale 下约 9 分钟(query72/14 等重查询),需要全量验证时改用
     * {@code TpcdsBenchmark run} 命令。
     */
    private static final int QUERY_LIMIT = 10;

    @Test
    void queriesExecuteDirectly(@TempDir Path dataDir) throws Exception {
        new TpcdsDataGenerator().generate(0.01, dataDir);

        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            storage.loadAll();
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);

            Map<String, String> queries = new TpcdsTemplateParser().parseBundled();
            assertEquals(99, queries.size(), "应内置 99 条查询");

            // 收集所有失败(而非首个即中断),便于一次性看清哪些查询有问题。
            List<String> failures = new ArrayList<>();
            int count = 0;
            for (Map.Entry<String, String> e : queries.entrySet()) {
                if (count++ >= QUERY_LIMIT) {
                    break;
                }
                try {
                    QueryResult result = executor.execute(e.getValue());
                    if (result instanceof QueryResult.Rows rows) {
                        rows.data().close();
                    }
                    System.out.println("执行成功: " + e.getKey());
                } catch (Exception ex) {
                    failures.add(e.getKey() + ": " + ex.getMessage());
                }
            }
            assertTrue(failures.isEmpty(), "失败查询: " + failures);
            storage.close();
        }
    }
}
