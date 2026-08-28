package com.minidb.tpcds;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableType;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TPC-DS 查询正确性验证:用 DuckDB 作为参照数据库,对比 MiniDB 与 DuckDB 的查询结果。
 *
 * <p>数据生成后以 Parquet 格式落盘。DuckDB 通过 {@code read_parquet} 直接读取同一份
 * Parquet 文件,无需额外导入。同一个 SQL 字符串喂给两边,对比行数和排序后的内容。</p>
 *
 * <p>默认 0.01 scale(快测试,~10s)。0.01 scale 下 store 等维度表极稀疏(teradata 库
 * SF0.01 store=2),多数查询过滤条件命中 0 行,两边都 0 行仅作「空洞验证」(0==0,无数据可比)。
 * 不做空洞验证、要实质的行级对比,用 SF1 数据:</p>
 * <pre>{@code
 *   # 前 10 条(默认):
 *   mvn test -pl minidb-tpcds -Dtest=TpcdsCorrectnessTest -Dtpcds.scale=1.0
 *   # 全 99 条(约 18 分钟):
 *   mvn test -pl minidb-tpcds -Dtest=TpcdsCorrectnessTest -Dtpcds.scale=1.0 -Dtpcds.full=true
 *   # 指定查询:
 *   mvn test -pl minidb-tpcds -Dtest=TpcdsCorrectnessTest -Dtpcds.scale=1.0 -Dtpcds.query=8,9,10
 * }</pre>
 * <p>SF1 下 store=12,customer=100000,查询条件命中率大幅提升,前 10 条中 9 条有实质数据
 * 可对比(0.01 下只有 2 条)。</p>
 */
class TpcdsCorrectnessTest {

    private static final int DEFAULT_LIMIT = 10;
    /** 默认 0.01 scale(快测试)。更大 scale(1.0)数据更密集,更多查询返回非空,但生成慢,手动触发:
     *  {@code -Dtpcds.scale=1.0}。 */
    private static final double SCALE =
            Double.parseDouble(System.getProperty("tpcds.scale", "0.01"));

    @Test
    void verifyQueryResults(@TempDir Path dataDir) throws Exception {
        new TpcdsDataGenerator().generate(SCALE, dataDir, StorageFormat.PARQUET, TableType.SIMPLE);

        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            storage.loadAll();
            StatsManager stats = new StatsManager(storage);
            QueryExecutor miniDb = new QueryExecutor(catalog, storage, allocator, stats);

            try (Connection duckDb = DriverManager.getConnection("jdbc:duckdb:");
                 Statement duckStmt = duckDb.createStatement()) {

                registerTables(duckStmt, catalog, dataDir);

                Map<String, String> queries = new TpcdsTemplateParser().parseBundled();
                int limit = "true".equals(System.getProperty("tpcds.full")) ? 99 : DEFAULT_LIMIT;
                // 支持只跑指定查询号:-Dtpcds.query=5,14,22
                String queryFilter = System.getProperty("tpcds.query");
                if (queryFilter != null && !queryFilter.isEmpty()) {
                    limit = 99;
                }

                List<String> failures = new ArrayList<>();
                List<String> skipped = new ArrayList<>();
                // 两边都返回 0 行的查询:行数一致但无数据可比,记为「空洞验证」,
                // 与「实质验证」(非空行级对比)区分。0.01 scale 下事实表虽非空,但 store 等
                // 维度表极稀疏(如 store=2),多数查询的过滤条件命中 0 行 —— 这是数据规模导致,
                // 不是 MiniDB 算错(DuckDB 在同数据上同样 0 行)。空洞验证仍证明 MiniDB 没把
                // 「该非空」算成空(没漏行),但无法验证数据细节,故单独标记。
                List<String> vacuous = new ArrayList<>();
                int count = 0;
                for (Map.Entry<String, String> e : queries.entrySet()) {
                    if (count++ >= limit) {
                        break;
                    }
                    if (queryFilter != null) {
                        int qn = Integer.parseInt(e.getKey().substring("query".length()));
                        boolean match = false;
                        for (String q : queryFilter.split(",")) {
                            if (Integer.parseInt(q.trim()) == qn) {
                                match = true;
                                break;
                            }
                        }
                        if (!match) {
                            continue;
                        }
                    }
                    compareOne(e.getKey(), e.getValue(), miniDb, duckStmt, failures, skipped, vacuous);
                }

                int total = count;
                int substantiated = total - skipped.size() - vacuous.size();
                System.out.println("==== 正确性验证汇总 ====");
                System.out.println("  实质验证(非空行级对比通过): " + substantiated + " 条");
                System.out.println("  空洞验证(两边均 0 行,无数据可比): " + vacuous.size() + " 条");
                System.out.println("  跳过(DuckDB 异常/不支持): " + skipped.size() + " 条");
                if (!vacuous.isEmpty()) {
                    System.out.println("  空洞查询: " + String.join(", ", vacuous));
                }
                if (!skipped.isEmpty()) {
                    System.out.println("  跳过查询: " + String.join(", ", skipped));
                }
                assertTrue(failures.isEmpty(),
                        "结果不匹配(" + failures.size() + "条):\n" + String.join("\n", failures));
            }
            storage.close();
        }
    }

    private void registerTables(Statement duckStmt, MiniDbCatalog catalog, Path dataDir)
            throws Exception {
        for (String tableName : catalog.tableNames("public")) {
            Path tableDir = dataDir.resolve("public").resolve(tableName);
            if (!Files.isDirectory(tableDir)) {
                continue;
            }
            String path = tableDir.toAbsolutePath().toString().replace('\\', '/');
            duckStmt.execute("CREATE VIEW \"" + tableName + "\" AS "
                    + "SELECT * FROM read_parquet('" + path + "/*.parquet')");
        }
    }

    private void compareOne(String name, String sql, QueryExecutor miniDb, Statement duckStmt,
                            List<String> failures, List<String> skipped, List<String> vacuous) {
        List<List<Object>> miniRows;
        try {
            miniRows = executeMiniDb(miniDb, sql);
        } catch (Exception ex) {
            failures.add(name + ": MiniDB异常 " + ex.getClass().getSimpleName() + ": "
                    + ex.getMessage());
            return;
        }

        List<List<Object>> duckRows;
        try {
            duckRows = executeDuckDb(duckStmt, sql);
        } catch (Exception ex) {
            String msg = ex.getMessage();
            if (msg != null && msg.length() > 120) {
                msg = msg.substring(0, 120);
            }
            if (miniRows != null) {
                skipped.add(name + "(" + msg + ")");
            }
            return;
        }

        if (miniRows == null) {
            failures.add(name + ": MiniDB 返回 null");
            return;
        }

        if (miniRows.size() != duckRows.size()) {
            failures.add(name + ": 行数不匹配 MiniDB=" + miniRows.size()
                    + " DuckDB=" + duckRows.size());
            return;
        }

        // 两边均 0 行:行数一致但无数据可比,记为空洞验证(不进 failures)。
        if (miniRows.isEmpty()) {
            vacuous.add(name);
            return;
        }

        Comparator<List<Object>> cmp = TpcdsCorrectnessTest::compareRows;
        miniRows.sort(cmp);
        duckRows.sort(cmp);

        for (int i = 0; i < miniRows.size(); i++) {
            if (!rowsEqual(miniRows.get(i), duckRows.get(i))) {
                List<Object> ma = miniRows.get(i);
                List<Object> da = duckRows.get(i);
                failures.add(name + ": 第" + (i + 1) + "行不匹配 (sizes " + ma.size()
                        + " vs " + da.size() + ") " + ma + " vs " + da);
                return;
            }
        }
        System.out.println("实质验证通过: " + name + " (" + miniRows.size() + "行)");
    }

    // ---- MiniDB 执行 ----

    private List<List<Object>> executeMiniDb(QueryExecutor executor, String sql) {
        QueryResult result = executor.execute(sql);
        if (result instanceof QueryResult.Rows rows) {
            VectorSchemaRoot root = rows.data();
            List<List<Object>> out = new ArrayList<>(root.getRowCount());
            for (int r = 0; r < root.getRowCount(); r++) {
                List<Object> row = new ArrayList<>(root.getFieldVectors().size());
                for (FieldVector v : root.getFieldVectors()) {
                    row.add(readVector(v, r));
                }
                out.add(row);
            }
            root.close();
            return out;
        }
        return null;
    }

    private static Object readVector(FieldVector v, int row) {
        if (v.isNull(row)) {
            return null;
        }
        if (v instanceof IntVector iv) {
            return iv.get(row);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(row);
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(row);
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(row);
        }
        if (v instanceof VarCharVector vv) {
            return new String(vv.get(row));
        }
        if (v instanceof BitVector bv) {
            return bv.get(row) == 1;
        }
        if (v instanceof DateDayVector dv) {
            return dv.get(row);
        }
        if (v instanceof TimeMilliVector tv) {
            return tv.get(row);
        }
        return v.getObject(row);
    }

    // ---- DuckDB 执行 ----

    private static List<List<Object>> executeDuckDb(Statement stmt, String sql) throws Exception {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            List<List<Object>> out = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>(cols);
                for (int c = 1; c <= cols; c++) {
                    row.add(rs.getObject(c));
                }
                out.add(row);
            }
            return out;
        }
    }

    // ---- 行排序与比较 ----

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareRows(List<Object> a, List<Object> b) {
        for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
            Comparable ca = toComparable(a.get(i));
            Comparable cb = toComparable(b.get(i));
            if (ca == null && cb == null) {
                continue;
            }
            if (ca == null) {
                return -1;
            }
            if (cb == null) {
                return 1;
            }
            int cmp = ca.compareTo(cb);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.size(), b.size());
    }

    private static boolean rowsEqual(List<Object> a, List<Object> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!valueEqual(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean valueEqual(Object a, Object b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (isNumeric(a) && isNumeric(b)) {
            // 若任一方是浮点(DuckDB 对 AVG 等返回 Double),用相对容差比较,
            // 因为 MiniDB 的 DECIMAL 除法按 scale 舍入,DuckDB 用全精度 DOUBLE。
            boolean anyFloating = a instanceof Double || a instanceof Float
                    || b instanceof Double || b instanceof Float;
            if (anyFloating) {
                double da = toBigDecimal(a).doubleValue();
                double db = toBigDecimal(b).doubleValue();
                if (Double.isNaN(da) || Double.isNaN(db)) {
                    return false;
                }
                double tol = Math.max(1e-6, Math.abs(db) * 1e-6);
                return Math.abs(da - db) < tol;
            }
            return toBigDecimal(a).compareTo(toBigDecimal(b)) == 0;
        }
        if (a instanceof String sa && b instanceof String sb) {
            return stripTrailing(sa).equals(stripTrailing(sb));
        }
        if (a instanceof Integer && b instanceof java.sql.Date) {
            return ((Integer) a).intValue() == ((java.sql.Date) b).toLocalDate().toEpochDay();
        }
        if (b instanceof Integer && a instanceof java.sql.Date) {
            return ((Integer) b).intValue() == ((java.sql.Date) a).toLocalDate().toEpochDay();
        }
        // DuckDB JDBC 可能返回 java.time.LocalDate 而非 java.sql.Date
        if (a instanceof Integer && b instanceof java.time.LocalDate) {
            return ((Integer) a).intValue() == ((java.time.LocalDate) b).toEpochDay();
        }
        if (b instanceof Integer && a instanceof java.time.LocalDate) {
            return ((Integer) b).intValue() == ((java.time.LocalDate) a).toEpochDay();
        }
        // DuckDB 可能返回日期字符串 "2001-03-06"
        if (a instanceof Integer && b instanceof String) {
            return ((Integer) a).intValue() == java.time.LocalDate.parse((String) b).toEpochDay();
        }
        if (b instanceof Integer && a instanceof String) {
            return ((Integer) b).intValue() == java.time.LocalDate.parse((String) a).toEpochDay();
        }
        if (a instanceof Integer && b instanceof java.sql.Time) {
            return ((Integer) a).intValue() == (int) ((java.sql.Time) b).toLocalTime().toNanoOfDay() / 1_000_000;
        }
        if (b instanceof Integer && a instanceof java.sql.Time) {
            return ((Integer) b).intValue() == (int) ((java.sql.Time) a).toLocalTime().toNanoOfDay() / 1_000_000;
        }
        if (a.getClass().equals(b.getClass())) {
            return Objects.equals(a, b);
        }
        return Objects.equals(a.toString(), b.toString());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable toComparable(Object v) {
        if (v == null) {
            return null;
        }
        if (isNumeric(v)) {
            return toBigDecimal(v);
        }
        if (v instanceof java.sql.Date d) {
            return (int) d.toLocalDate().toEpochDay();
        }
        if (v instanceof java.time.LocalDate ld) {
            return (int) ld.toEpochDay();
        }
        if (v instanceof java.sql.Time t) {
            return (int) (t.toLocalTime().toNanoOfDay() / 1_000_000);
        }
        if (v instanceof String s) {
            return stripTrailing(s);
        }
        if (v instanceof Comparable c) {
            return c;
        }
        return v.toString();
    }

    private static boolean isNumeric(Object v) {
        return v instanceof Integer || v instanceof Long || v instanceof Float
                || v instanceof Double || v instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal bd) {
            return bd;
        }
        if (v instanceof Double d) {
            return BigDecimal.valueOf(d);
        }
        if (v instanceof Float f) {
            return BigDecimal.valueOf(f.doubleValue());
        }
        if (v instanceof Long l) {
            return BigDecimal.valueOf(l);
        }
        if (v instanceof Integer i) {
            return BigDecimal.valueOf(i);
        }
        return BigDecimal.ZERO;
    }

    private static String stripTrailing(String s) {
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == ' ') {
            end--;
        }
        return end == s.length() ? s : s.substring(0, end);
    }
}