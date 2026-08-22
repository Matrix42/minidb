package com.minidb.tpcds;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.server.MiniDbServer;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * TPC-DS 查询执行器:启动 MiniDbServer,用 JDBC 逐条跑 SQL(按 query 号排序),
 * 记录耗时/返回行数/成败,结果写 JSON。失败不中断后续查询。
 */
public class TpcdsBenchmarkRunner {

    public record QueryResult(String name, long elapsedMs, long rowCount,
                              boolean success, String error) {
    }

    public void run(Map<String, String> queries, Path dataDir, Path outputJson, double scale,
                    String name, long perQueryTimeoutMs)
            throws Exception {
        List<QueryResult> results = new ArrayList<>();
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        try (Connection c = DriverManager.getConnection("jdbc:minidb://127.0.0.1:" + server.port() + "?timeout=0");
             Statement s = c.createStatement()) {
            int total = queries.size();
            int idx = 0;
            for (Map.Entry<String, String> e : queries.entrySet()) {
                idx++;
                System.out.println("[" + idx + "/" + total + "] " + e.getKey()
                        + " (timeout=" + (perQueryTimeoutMs > 0 ? perQueryTimeoutMs + "ms" : "none") + ") ...");
                long qstart = System.nanoTime();
                if (perQueryTimeoutMs > 0) {
                    s.setQueryTimeout((int) Math.max(1, perQueryTimeoutMs / 1000));
                }
                QueryResult qr = runOne(s, e.getKey(), e.getValue());
                System.out.println("[" + idx + "/" + total + "] " + e.getKey()
                        + " -> " + (qr.success() ? "ok " + qr.rowCount() + " rows" : "FAIL")
                        + " in " + (System.nanoTime() - qstart) / 1_000_000 + " ms"
                        + (qr.error() != null ? " (" + qr.error() + ")" : ""));
                results.add(qr);
            }
        } finally {
            try {
                server.close();
            } catch (Exception e) {
                // 查询执行可能残留未释放的 Arrow 批(MiniDbServer 既有行为),close 时
                // 触发 allocator 泄漏检测。基准测试不因此中断,记录警告后继续写结果。
                System.err.println("server close warning: " + e.getMessage());
            }
        }

        writeReport(results, outputJson, scale, name);
    }

    /**
     * 同 {@link #run} 但直接构造 {@link QueryExecutor} 执行,不走 MiniDbServer/JDBC
     * 网络层,更快、更稳定(失败时能直接看到内核异常)。
     *
     * @param perQueryTimeoutMs 单条查询超时(ms),0 表示不限。超时的查询记 success=false 跳过。
     */
    public void runDirect(Map<String, String> queries, Path dataDir, Path outputJson, double scale,
                          String name, long perQueryTimeoutMs)
            throws Exception {
        List<QueryResult> results = new ArrayList<>();
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            storage.loadAll();
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            int total = queries.size();
            int idx = 0;
            for (Map.Entry<String, String> e : queries.entrySet()) {
                idx++;
                System.out.println("[" + idx + "/" + total + "] " + e.getKey()
                        + " (timeout=" + (perQueryTimeoutMs > 0 ? perQueryTimeoutMs + "ms" : "none") + ") ...");
                long qstart = System.nanoTime();
                QueryResult qr = runOneDirect(executor, e.getKey(), e.getValue(), perQueryTimeoutMs);
                System.out.println("[" + idx + "/" + total + "] " + e.getKey()
                        + " -> " + (qr.success() ? "ok " + qr.rowCount() + " rows" : "FAIL")
                        + " in " + (System.nanoTime() - qstart) / 1_000_000 + " ms"
                        + " alloc=" + allocator.getAllocatedMemory() + "/" + allocator.getLimit()
                        + (qr.error() != null ? " (" + qr.error() + ")" : ""));
                results.add(qr);
            }
            storage.close();
        }
        writeReport(results, outputJson, scale, name);
    }

    private QueryResult runOneDirect(QueryExecutor executor, String name, String sql,
                                     long timeoutMs) {
        long start = System.nanoTime();
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "tpcds-" + name);
            t.setDaemon(true);
            return t;
        });
        java.util.concurrent.Future<QueryResult> future = pool.submit(() -> doRunOneDirect(executor, name, sql, start));
        try {
            if (timeoutMs > 0) {
                return future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
            return future.get();
        } catch (java.util.concurrent.TimeoutException te) {
            future.cancel(true);
            return new QueryResult(name, (System.nanoTime() - start) / 1_000_000,
                    -1, false, "timeout after " + timeoutMs + " ms");
        } catch (Exception ex) {
            String msg = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
            if (msg == null) {
                msg = (ex.getCause() != null ? ex.getCause() : ex).getClass().getSimpleName();
            }
            if (msg.length() > 200) {
                msg = msg.substring(0, 200);
            }
            return new QueryResult(name, (System.nanoTime() - start) / 1_000_000, -1, false, msg);
        } finally {
            pool.shutdownNow();
        }
    }

    private QueryResult doRunOneDirect(QueryExecutor executor, String name, String sql, long startNanos) {
        try {
            com.minidb.server.exec.QueryResult r = executor.execute(sql);
            long rows;
            if (r instanceof com.minidb.server.exec.QueryResult.Rows rowsR) {
                rows = rowsR.data().getRowCount();
                rowsR.data().close();
            } else if (r instanceof com.minidb.server.exec.QueryResult.Update upd) {
                rows = upd.count();
            } else {
                rows = 0;
            }
            return new QueryResult(name, elapsedMs(startNanos), rows, true, null);
        } catch (Exception ex) {
            String msg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
            if (msg.length() > 200) {
                msg = msg.substring(0, 200);
            }
            return new QueryResult(name, elapsedMs(startNanos), -1, false, msg);
        }
    }

    private void writeReport(List<QueryResult> results, Path outputJson, double scale,
                            String name)
            throws IOException {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", name != null ? name : "unknown");
        out.put("scale", scale);
        out.put("timestamp", Instant.now().toString());
        out.put("queries", results);
        if (outputJson.getParent() != null) {
            Files.createDirectories(outputJson.getParent());
        }
        Files.writeString(outputJson,
                new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(out));
    }

    private QueryResult runOne(Statement s, String name, String sql) {
        long start = System.nanoTime();
        try {
            boolean hasResult = s.execute(sql);
            long rows;
            if (hasResult) {
                try (ResultSet rs = s.getResultSet()) {
                    rows = 0;
                    while (rs.next()) {
                        rows++;
                    }
                }
            } else {
                rows = s.getUpdateCount();
            }
            return new QueryResult(name, elapsedMs(start), rows, true, null);
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (msg.length() > 200) {
                msg = msg.substring(0, 200);
            }
            return new QueryResult(name, elapsedMs(start), -1, false, msg);
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
