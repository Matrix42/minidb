# 统计迁入 catalog.json + 接入 Calcite 成本模型(阶段一)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把表统计从 `.stats` 文件迁到 `data/catalog.json`,并把 rowCount/distinct/selectivity 三路接入 Calcite 成本模型(为阶段二 CBO 打底)。

**Architecture:** `Histogram`/`TableStats` 改成 Jackson 可序列化的 record(直方图值由 `Comparable<?>` 改为 `String` + `ColumnType`);统计归 `MiniDbCatalog` 统一持有并随 `CatalogSnapshot` 持久化;`StatsManager` 退化为薄分析器;`MiniDbCalciteTable` 实现 `getStatistic()` + `BuiltInMetadata.Selectivity.Handler` + `DistinctRowCount.Handler`。

**Tech Stack:** Java 17、Apache Calcite 1.42、Apache Arrow、Jackson(已有)、JUnit 5。

**Spec:** `docs/superpowers/specs/2026-08-14-stats-cbo-design.md`

## Global Constraints

- JDK 17,构建用 `./mvnw.cmd`(bash 下直接跑,不是 `mvnw.cmd`)。
- 单模块测试:`./mvnw.cmd test -pl minidb-server`;单类:`./mvnw.cmd test -pl minidb-server -Dtest=ClassName`。
- 测试用 JUnit 5 + `@TempDir` + `RootAllocator`。
- 每任务结束提交,conventional commit 风格(`feat:`/`fix:`/`refactor:`/`test:`),在 `master` 分支,不 amend。
- 回复用户用中文;代码/标识符/路径保持原文。
- 统计值类型:`HistogramBuilder` 只支持 INTEGER/BIGINT/DOUBLE/VARCHAR/BOOLEAN/DATE/TIMESTAMP(其余类型 `analyze` 抛异常,不在本阶段范围)。

---

## File Structure

- `minidb-server/src/main/java/com/minidb/server/stats/Histogram.java` — 改 record:`Bucket`/`McValue` 值改 `String`,`Histogram` 加 `ColumnType`,比较逻辑按类型解析。
- `minidb-server/src/main/java/com/minidb/server/stats/HistogramBuilder.java` — 产 `String` 值。
- `minidb-server/src/main/java/com/minidb/server/stats/TableStats.java` — 加 `rowCount`,去 `Serializable`。
- `minidb-server/src/main/java/com/minidb/server/stats/StatsEstimator.java` — **新建**:`findFirstInputRef` + `histogramForCondition` 公共方法。
- `minidb-server/src/main/java/com/minidb/server/stats/StatsManager.java` — 薄分析器,委托 catalog。
- `minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java` — 持统计 map + `getStats`/`setStats`/`markStatsStale`,drop 时删统计,snapshot/restore 含统计。
- `minidb-server/src/main/java/com/minidb/server/catalog/CatalogSnapshot.java` — 加 `Map<String, TableStats> stats`。
- `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java` — 去掉 `statsManager` 字段,`markDirty` 改调 `catalog.markStatsStale`。
- `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteTable.java` — 接 `getStatistic()` + 两个 Handler。
- `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbRootCalciteSchema.java`、`MiniDbCalciteSchema.java` — 构造 `MiniDbCalciteTable` 时传 `catalog`。
- `minidb-server/src/main/java/com/minidb/server/exec/ExplainExecutor.java` — 复用 `StatsEstimator`。
- `minidb-server/src/main/java/com/minidb/server/MiniDbServer.java` — `StatsManager` 构造改、去 `setStatsManager`/`loadAll`。

---

### Task 1: Histogram 改 JSON 可序列化

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/stats/Histogram.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/stats/HistogramBuilder.java`
- Test: `minidb-server/src/test/java/com/minidb/server/stats/HistogramJsonTest.java`(新建)
- Test: 适配 `minidb-server/src/test/java/com/minidb/server/stats/HistogramTest.java`、`HistogramBuilderTest.java`

**Interfaces:**
- Produces: `Histogram(ColumnType type, List<Bucket> buckets, List<McValue> mcv, long distinctCount, long nullCount, long totalRows)`;`Bucket(String lower, String upper, long rowCount)`;`McValue(String value, long frequency)`;`Histogram.selectivity(RexNode, long)`;`Histogram.totalRows()`/`distinctCount()`/`nullCount()`;`Histogram.empty(ColumnType type)`。

- [ ] **Step 1: 写失败测试 —— Histogram 可 Jackson 序列化往返**

```java
// minidb-server/src/test/java/com/minidb/server/stats/HistogramJsonTest.java
package com.minidb.server.stats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.server.catalog.ColumnType;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistogramJsonTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void histogramJsonRoundTrip() throws Exception {
        Histogram h = new Histogram(ColumnType.INTEGER,
                List.of(new Histogram.Bucket("1", "5", 3), new Histogram.Bucket("6", "10", 2)),
                List.of(new Histogram.McValue("3", 2)),
                8, 1, 10);

        String json = MAPPER.writeValueAsString(h);
        Histogram back = MAPPER.readValue(json, Histogram.class);

        assertEquals(ColumnType.INTEGER, back.type());
        assertEquals(h.buckets(), back.buckets());
        assertEquals(h.mcv(), back.mcv());
        assertEquals(h.distinctCount(), back.distinctCount());
        assertEquals(h.nullCount(), back.nullCount());
        assertEquals(h.totalRows(), back.totalRows());
    }

    @Test
    void emptyHistogramJsonRoundTrip() throws Exception {
        Histogram h = Histogram.empty(ColumnType.VARCHAR);
        Histogram back = MAPPER.readValue(MAPPER.writeValueAsString(h), Histogram.class);
        assertEquals(ColumnType.VARCHAR, back.type());
        assertEquals(0, back.totalRows());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=HistogramJsonTest`
Expected: 编译失败(Histogram 不是 record、无 `type()`、`Bucket`/`McValue` 构造器签名不同)。

- [ ] **Step 3: 改 `Histogram` 为 record + String 值 + ColumnType**

把 `Histogram` 整个类改为(保留 `DEFAULT_SELECTIVITY` 常量与 `selectivity` 系列方法,比较逻辑见 Step 4):

```java
package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnType;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public record Histogram(
        ColumnType type,
        List<Bucket> buckets,
        List<McValue> mcv,
        long distinctCount,
        long nullCount,
        long totalRows) {

    public static final double DEFAULT_SELECTIVITY = 0.33;

    public record Bucket(String lower, String upper, long rowCount) {}

    public record McValue(String value, long frequency) {}

    public Histogram {
        buckets = List.copyOf(buckets);
        mcv = List.copyOf(mcv);
    }

    public static Histogram empty(ColumnType type) {
        return new Histogram(type, List.of(), List.of(), 0, 0, 0);
    }

    // ---- selectivity 系列方法(原样保留方法名与流程,只改比较用的取值方式)----

    public double selectivity(RexNode condition, long inputRows) { /* 原样 */ }
    private double selectivityCall(RexCall call, long inputRows) { /* 原样 */ }
    private double comparisonSelectivity(RexCall call, SqlKind kind) { /* 原样,literal 侧改用 normalizeLiteral */ }
    private double equalitySelectivity(Comparable<?> value) { /* 改用 histValue */ }
    private double rangeSelectivity(Comparable<?> literal, SqlKind kind) { /* 改用 histValue + normalizeLiteral */ }
    private long spanSize(Bucket b) { /* 改用 histValue */ }
    private static long numericDelta(Comparable<Object> a, Comparable<Object> b) { /* 入参改为已归一化值 */ }
    private static Integer inputRefIndex(RexNode node) { /* 原样 */ }
    private static Comparable<?> rexLiteral(RexNode node) { /* 原 literalValue,改名 */ }
    private Comparable<Object> histValue(String s) { /* 新增:按 type 解析 */ }
    private static Comparable<Object> normalizeLiteral(Comparable<?> c) { /* 原 compareValue,改名 */ }
    private static boolean typesCompatible(Comparable<?> a, Comparable<?> b) { /* 原样 */ }
}
```

- [ ] **Step 4: 实现比较逻辑的取值改造**

具体替换(把原来的 `compareValue` 拆成 `histValue` + `normalizeLiteral`):

```java
    /** 直方图里存的规范字符串 → 可比较值,按列类型解析。 */
    private Comparable<Object> histValue(String s) {
        return switch (type) {
            case INTEGER, BIGINT, SMALLINT, DOUBLE, REAL, FLOAT, DECIMAL, NUMERIC,
                 DATE, TIME, TIMESTAMP -> Double.valueOf(s);
            case VARCHAR, CHAR, NCHAR, NVARCHAR -> s;
            case BOOLEAN -> Boolean.valueOf(s);
            default -> s;
        };
    }

    /** 字面量(RexLiteral 里的 Comparable)→ 可比较值,数值统一归一化到 Double。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<Object> normalizeLiteral(Comparable<?> c) {
        if (c instanceof BigDecimal bd) return Double.valueOf(bd.doubleValue());
        if (c instanceof Integer i) return Double.valueOf(i.doubleValue());
        if (c instanceof Long l) return Double.valueOf(l.doubleValue());
        if (c instanceof Float f) return Double.valueOf(f.doubleValue());
        return (Comparable<Object>) (Comparable) c;
    }
```

调用点替换:
- `equalitySelectivity`: `Objects.equals(compareValue(m.value()), compareValue(value))` → `Objects.equals(histValue(m.value()), normalizeLiteral(value))`。
- `rangeSelectivity`: 所有 `compareValue(b.upper())`/`compareValue(b.lower())` → `histValue(b.upper())`/`histValue(b.lower())`;所有 `compareValue(literal)` → `normalizeLiteral(literal)`。
- `spanSize`: `compareValue(b.lower())`/`compareValue(b.upper())` → `histValue(...)`。
- `numericDelta`: 参数类型改为 `Comparable<Object> a, Comparable<Object> b`,调用处传 `histValue(...)`/`normalizeLiteral(...)` 的结果(不再在内部 `compareValue`)。
- `typesCompatible(sampleBound, normalizedLiteral)`: 入参已是 `histValue`/`normalizeLiteral` 结果。

- [ ] **Step 5: 改 `HistogramBuilder` 产 String 值**

`read(ValueVector v, int i, ColumnType type)` 的返回值改用 `String.valueOf(...)`(各 case 里 `Integer.toString`/`Long.toString`/`Double.toString`/`new String(...)`/`Boolean.toString`)。`values` 声明 `List<Comparable<?>>` 不变(String 是 Comparable)。

把 `normalize` 改成按类型解析(原来按 BigDecimal/Integer/Long 归一化,现在值已是 String):

```java
@SuppressWarnings({"unchecked", "rawtypes"})
private static Comparable<Object> normalize(Comparable<?> c, ColumnType type) {
    if (c instanceof String s) {
        return switch (type) {
            case INTEGER, BIGINT, SMALLINT, DOUBLE, REAL, FLOAT, DECIMAL, NUMERIC,
                 DATE, TIME, TIMESTAMP -> Double.valueOf(Double.parseDouble(s));
            case BOOLEAN -> Boolean.valueOf(s);
            default -> s;
        };
    }
    if (c instanceof java.math.BigDecimal bd) {
        return (Comparable<Object>) (Comparable) Double.valueOf(bd.doubleValue());
    }
    if (c instanceof Integer i) {
        return (Comparable<Object>) (Comparable) Double.valueOf(i.doubleValue());
    }
    if (c instanceof Long l) {
        return (Comparable<Object>) (Comparable) Double.valueOf(l.doubleValue());
    }
    return (Comparable<Object>) c;
}
```

同步改签名与调用:
- `build` 里 `values.sort(Comparator.comparing(v -> normalize(v, type)));`、`distinctCount(values, type)`、`return new Histogram(type, buckets, mcv, distinctCount, nullCount, totalRows);`;`values.isEmpty()` 分支 `return Histogram.empty(type)`。
- `distinctCount(List<Comparable<?>> sorted, ColumnType type)` 内部 `normalize(sorted.get(i), type).compareTo(normalize(sorted.get(i - 1), type))`。
- `topMcv` 用 `Collectors.groupingBy(v -> v)`(String 值按 String 分组),不变。

- [ ] **Step 6: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=HistogramJsonTest,HistogramTest,HistogramBuilderTest`
Expected: 全 PASS(HistogramTest/HistogramBuilderTest 需同步改构造调用为 `new Histogram(type, ...)`、`Histogram.empty(type)`,以及断言里 `bucket.lower()` 等从 `Comparable` 改为 `String`)。

- [ ] **Step 7: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/stats/Histogram.java minidb-server/src/main/java/com/minidb/server/stats/HistogramBuilder.java minidb-server/src/test/java/com/minidb/server/stats/HistogramJsonTest.java minidb-server/src/test/java/com/minidb/server/stats/HistogramTest.java minidb-server/src/test/java/com/minidb/server/stats/HistogramBuilderTest.java
git commit -m "refactor: Histogram 改 record + String 值,支持 JSON 序列化"
```

---

### Task 2: TableStats 加 rowCount、去 Serializable

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/stats/TableStats.java`
- Test: `minidb-server/src/test/java/com/minidb/server/stats/HistogramJsonTest.java`(加一个 TableStats 往返用例)

**Interfaces:**
- Produces: `TableStats(Map<String, Histogram> columnHistograms, long rowCount, boolean stale)`;`rowCount()`/`columnHistograms()`/`stale()`。

- [ ] **Step 1: 写失败测试**

在 `HistogramJsonTest` 里加:

```java
@Test
void tableStatsJsonRoundTrip() throws Exception {
    TableStats ts = new TableStats(
            Map.of("id", new Histogram(ColumnType.INTEGER, List.of(),
                    List.of(), 5, 0, 10)),
            10, false);
    TableStats back = MAPPER.readValue(MAPPER.writeValueAsString(ts), TableStats.class);
    assertEquals(10, back.rowCount());
    assertEquals(false, back.stale());
    assertEquals(ts.columnHistograms().keySet(), back.columnHistograms().keySet());
}
```

- [ ] **Step 2: 跑测试确认失败(TableStats 无 `rowCount` 构造参数、非 record)**

- [ ] **Step 3: 改 `TableStats`**

```java
package com.minidb.server.stats;

import java.util.Map;

public record TableStats(Map<String, Histogram> columnHistograms, long rowCount, boolean stale) {}
```

(去掉 `implements Serializable` 与 `serialVersionUID`。)

- [ ] **Step 4: 跑测试确认通过** `./mvnw.cmd test -pl minidb-server -Dtest=HistogramJsonTest`

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/stats/TableStats.java minidb-server/src/test/java/com/minidb/server/stats/HistogramJsonTest.java
git commit -m "refactor: TableStats 加 rowCount 并改为 JSON record"
```

---

### Task 3: MiniDbCatalog 持有统计 + CatalogSnapshot 持久化

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/CatalogSnapshot.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java`
- Test: `minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogStatsTest.java`(新建)

**Interfaces:**
- Consumes: `TableStats(...)`, `Histogram(...)`(Task 1/2)。
- Produces: `MiniDbCatalog.getStats(schema, table)`(无统计返回 null)、`setStats(schema, table, TableStats)`(触发 notifyChange)、`markStatsStale(schema, table)`(无统计时 no-op,不触发 notifyChange);`CatalogSnapshot(schemas, tables, Map<String,TableStats> stats)`。

- [ ] **Step 1: 写失败测试**

```java
// minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogStatsTest.java
package com.minidb.server.catalog;

import com.minidb.server.stats.Histogram;
import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MiniDbCatalogStatsTest {

    @Test
    void setAndGetStats() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))));
        TableStats ts = new TableStats(Map.of("id",
                new Histogram(ColumnType.INTEGER, List.of(), List.of(), 5, 0, 10)), 10, false);

        catalog.setStats("public", "t", ts);
        assertEquals(10, catalog.getStats("public", "t").rowCount());
        assertNull(catalog.getStats("public", "missing"));
    }

    @Test
    void markStatsStaleIsNoOpWithoutStats() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.markStatsStale("public", "t"); // 无统计:不抛、不产生条目
        assertNull(catalog.getStats("public", "t"));
    }

    @Test
    void dropTableRemovesStats() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t",
                new TableStats(Map.of(), 10, false));
        catalog.dropTable("public", "t");
        assertNull(catalog.getStats("public", "t"));
    }

    @Test
    void snapshotAndRestoreCarryStats() {
        MiniDbCatalog src = new MiniDbCatalog();
        src.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        src.setStats("public", "t", new TableStats(Map.of(), 42, false));

        MiniDbCatalog dst = new MiniDbCatalog();
        dst.restore(src.snapshot());
        assertEquals(42, dst.getStats("public", "t").rowCount());
    }
}
```

- [ ] **Step 2: 跑测试确认失败(编译:`CatalogSnapshot` 无 stats、`MiniDbCatalog` 无 getStats/setStats)**

- [ ] **Step 3: 改 `CatalogSnapshot`**

```java
package com.minidb.server.catalog;

import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Map;

public record CatalogSnapshot(List<String> schemas, List<TableSchema> tables,
                              Map<String, TableStats> stats) {
    public CatalogSnapshot(List<String> schemas, List<TableSchema> tables) {
        this(schemas, tables, Map.of());
    }
}
```

(保留旧的 2 参构造,`JsonCatalogStore` 里 `new CatalogSnapshot(List.of(), List.of())` 仍可用。)

- [ ] **Step 4: 改 `MiniDbCatalog`**

加字段与方法(import `com.minidb.server.stats.TableStats`):

```java
private final Map<String, TableStats> stats = new ConcurrentHashMap<>();

public TableStats getStats(String schemaName, String tableName) {
    return stats.get(statsKey(schemaName, tableName));
}

public void setStats(String schemaName, String tableName, TableStats ts) {
    stats.put(statsKey(schemaName, tableName), ts);
    notifyChange();
}

public void markStatsStale(String schemaName, String tableName) {
    String k = statsKey(schemaName, tableName);
    TableStats ts = stats.get(k);
    if (ts != null) {
        stats.put(k, new TableStats(ts.columnHistograms(), ts.rowCount(), true));
    }
    // 不 notifyChange:避免每次 DML 都写 catalog.json(与旧 .stats 行为一致,stale 随重启丢)
}

private static String statsKey(String schemaName, String tableName) {
    return key(schemaName) + "." + key(tableName);
}
```

`dropTable` 里在 `tables.remove(...)` 后加 `stats.remove(statsKey(schemaName, tableName));`;`dropSchema` 里在 `schemas.remove(k)` 后加 `stats.keySet().removeIf(key -> key.startsWith(k + "."));`。`snapshot()` 返回 `new CatalogSnapshot(names, tables, Map.copyOf(stats));`;`restore()` 里 `for (var e : snapshot.stats().entrySet()) stats.put(e.getKey(), e.getValue());`。

- [ ] **Step 5: 跑测试确认通过** `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbCatalogStatsTest`

- [ ] **Step 6: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/CatalogSnapshot.java minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogStatsTest.java
git commit -m "feat: MiniDbCatalog 持有并持久化表统计"
```

---

### Task 4: StatsManager 退化为分析器 + StorageManager 接线

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/stats/StatsManager.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/MiniDbServer.java`
- Test: `minidb-server/src/test/java/com/minidb/server/stats/StatsManagerTest.java`(改写),并更新所有测试 setUp(见 Step 6)

**Interfaces:**
- Consumes: `MiniDbCatalog.setStats/getStats/markStatsStale`(Task 3)。
- Produces: `StatsManager(StorageManager storage)`;`analyze(String table)`;`analyzeAll()`;`tableStats(String table)`。

- [ ] **Step 1: 写失败测试 —— analyze 委托 catalog**

```java
// minidb-server/src/test/java/com/minidb/server/stats/StatsManagerTest.java(改写)
package com.minidb.server.stats;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StatsManagerTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void analyzeDelegatesToCatalog() {
        storage.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        QueryExecutor q = new QueryExecutor(catalog, storage, allocator, stats);
        q.execute("INSERT INTO t VALUES (1), (2), (2)");
        stats.analyze("t");
        TableStats ts = stats.tableStats("t");
        assertNotNull(ts);
        assertEquals(3, ts.rowCount());
    }
}
```

> 端到端(analyze → catalog.json → 重启恢复)放 Task 6 的集成测试,本任务只验证薄分析器的委托行为。

- [ ] **Step 2: 跑测试确认失败(编译:`StatsManager(storage)` 构造不存在、`tableStats` 读不到)**

- [ ] **Step 3: 改 `StatsManager` 为薄分析器**

删除字段 `dataDir`、`tables`;删除 `loadAll`/`persist`/`read`/`statsFile`/`stripExtension`/`key`/`resolveKey`;构造改为:

```java
public class StatsManager implements AutoCloseable {
    private final StorageManager storage;

    public StatsManager(StorageManager storage) {
        this.storage = storage;
    }

    public void analyze(String table) {
        ArrowTable arrowTable = storage.getTable(MiniDbCatalog.DEFAULT_SCHEMA, table);
        TableSchema schema = arrowTable.schema();
        Map<String, Histogram> columnHistograms = new HashMap<>();
        for (int col = 0; col < schema.columns().size(); col++) {
            String colName = schema.columns().get(col).name();
            ColumnType colType = schema.columns().get(col).type();
            List<ValueVector> columnVectors = new ArrayList<>();
            for (VectorSchemaRoot batch : arrowTable.batches()) {
                columnVectors.add(batch.getVector(col));
            }
            columnHistograms.put(colName.toLowerCase(Locale.ROOT),
                    HistogramBuilder.build(columnVectors, colType));
        }
        storage.catalog().setStats(MiniDbCatalog.DEFAULT_SCHEMA, table,
                new TableStats(columnHistograms, arrowTable.rowCount(), false));
    }

    public void analyzeAll() {
        for (String name : storage.catalog().tableNames()) {
            analyze(name);
        }
    }

    public TableStats tableStats(String table) {
        return storage.catalog().getStats(MiniDbCatalog.DEFAULT_SCHEMA, table);
    }

    @Override public void close() {}
}
```

(import 清理:去掉 `ObjectInputStream/ObjectOutputStream/UncheckedIOException/Files/Path` 等,新增 `HashMap/ArrayList`。)

- [ ] **Step 4: 改 `StorageManager` 接线**

- 删除字段 `private volatile StatsManager statsManager;`、方法 `setStatsManager(...)`、import `StatsManager`。
- `markDirty(String schemaName, String tableName)` 里 `if (statsManager != null) { statsManager.markStale(sk); }` 改为 `catalog.markStatsStale(schemaName, tableName);`。
- `dropTable` 里删 `if (statsManager != null) { statsManager.dropStats(sk); }`(catalog.dropTable 已删统计)。
- `dropSchema` 里删 `if (statsManager != null) { statsManager.dropStats(k); }`(catalog.dropSchema 已删统计)。

- [ ] **Step 5: 改 `MiniDbServer.start()`**

`new StatsManager(storage, allocator, dataDir)` → `new StatsManager(storage)`;删 `storage.setStatsManager(stats)` 与 `stats.loadAll()`(统计现在经 `storage.loadAll()` 里的 `catalog.restore` 一并恢复)。

- [ ] **Step 6: 更新所有测试 setUp**

全局替换模式(所有含 `StatsManager` 构造 + `setStatsManager` 的测试类):`new StatsManager(storage, allocator, dataDir)` → `new StatsManager(storage)`,删 `storage.setStatsManager(stats);` 一行。涉及的测试类至少:`QueryExecutorTest`、`ExistsSubqueryTest`、`SubqueryTest`、`ImplicitCastTest`、`DateCastTest`、`ExplainExecutorTest`、`SchemaDdlTest`、`SessionHandlerSchemaTest`、`DataTypeIntegrationTest` 等(用 `grep -rl "new StatsManager"` 定位全量)。

- [ ] **Step 7: 跑测试确认通过** `./mvnw.cmd test -pl minidb-server -Dtest=StatsManagerTest,QueryExecutorTest`

- [ ] **Step 8: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/stats/StatsManager.java minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java minidb-server/src/main/java/com/minidb/server/MiniDbServer.java minidb-server/src/test/java/
git commit -m "refactor: StatsManager 退化为薄分析器,统计归 MiniDbCatalog"
```

---

### Task 5: MiniDbCalciteTable 接入成本模型

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/stats/StatsEstimator.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/ExplainExecutor.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteTable.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbRootCalciteSchema.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteSchema.java`
- Test: `minidb-server/src/test/java/com/minidb/server/calcite/StatisticWiringTest.java`(新建)

**Interfaces:**
- Consumes: `MiniDbCatalog.getStats`, `Histogram.selectivity/distinctCount`, `TableStats.stale/rowCount`。
- Produces: `StatsEstimator.findFirstInputRef(RexNode) → Integer`;`StatsEstimator.histogramForCondition(RexNode, TableSchema, TableStats) → Histogram`。

- [ ] **Step 1: 写失败测试**

```java
// minidb-server/src/test/java/com/minidb/server/calcite/StatisticWiringTest.java
package com.minidb.server.calcite;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.stats.Histogram;
import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Statistic;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StatisticWiringTest {

    @Test
    void getStatisticReturnsRowCount() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t", new TableStats(Map.of(), 42, false));

        MiniDbCalciteTable table = new MiniDbCalciteTable(
                catalog.getTable("public", "t"), catalog);
        Statistic stat = table.getStatistic();
        assertEquals(42.0, stat.getRowCount());
    }

    @Test
    void getStatisticUnknownWhenStale() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t", new TableStats(Map.of(), 42, true));

        MiniDbCalciteTable table = new MiniDbCalciteTable(
                catalog.getTable("public", "t"), catalog);
        assertNull(table.getStatistic().getRowCount());
    }
}
```

- [ ] **Step 2: 跑测试确认失败(编译:`MiniDbCalciteTable` 构造器仅 1 参)**

- [ ] **Step 3: 新建 `StatsEstimator`**

```java
package com.minidb.server.stats;

import com.minidb.server.catalog.TableSchema;
import java.util.Locale;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public final class StatsEstimator {

    private StatsEstimator() {}

    public static Histogram histogramForCondition(RexNode cond, TableSchema schema, TableStats ts) {
        if (ts.columnHistograms().isEmpty()) {
            return null;
        }
        Integer colIndex = findFirstInputRef(cond);
        if (colIndex == null) {
            return null;
        }
        if (colIndex < 0 || colIndex >= schema.columns().size()) {
            return null;
        }
        String colName = schema.columns().get(colIndex).name().toLowerCase(Locale.ROOT);
        return ts.columnHistograms().get(colName);
    }

    public static Integer findFirstInputRef(RexNode node) {
        if (node instanceof RexInputRef ref) {
            return ref.getIndex();
        }
        if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                Integer idx = findFirstInputRef(operand);
                if (idx != null) {
                    return idx;
                }
            }
        }
        return null;
    }
}
```

- [ ] **Step 4: 改 `ExplainExecutor` 复用 `StatsEstimator`**

删除私有 `findFirstInputRef`、`histogramForCondition` 两个方法;`histogramForCondition` 的调用点(`filterSelectivity`)改为:

```java
Histogram h = histogramForCondition(cond, table, ts);
// 改为
Histogram h = StatsEstimator.histogramForCondition(cond, storage.getTable(table).schema(), ts);
```

- [ ] **Step 5: 改 `MiniDbCalciteTable`**

构造器改 `(TableSchema schema, MiniDbCatalog catalog)`,加 `getStatistic()` 与两个 Handler(import `org.apache.calcite.rel.RelNode`、`org.apache.calcite.rel.metadata.BuiltInMetadata`、`org.apache.calcite.rel.metadata.RelMetadataQuery`、`org.apache.calcite.rex.RexNode`、`org.apache.calcite.util.ImmutableBitSet`、`org.checkerframework.checker.nullness.qual.Nullable`):

```java
public class MiniDbCalciteTable extends AbstractTable
        implements BuiltInMetadata.Selectivity.Handler, BuiltInMetadata.DistinctRowCount.Handler {

    private final TableSchema schema;
    private final MiniDbCatalog catalog;

    public MiniDbCalciteTable(TableSchema schema, MiniDbCatalog catalog) {
        this.schema = schema;
        this.catalog = catalog;
    }

    @Override
    public Statistic getStatistic() {
        TableStats ts = catalog.getStats(schema.schemaName(), schema.name());
        if (ts == null || ts.stale()) {
            return Statistics.UNKNOWN;
        }
        return Statistics.of((double) ts.rowCount(), List.of());
    }

    @Override
    public @Nullable Double getSelectivity(RelNode r, RelMetadataQuery mq,
                                           @Nullable RexNode predicate) {
        if (predicate == null) {
            return null;
        }
        TableStats ts = catalog.getStats(schema.schemaName(), schema.name());
        if (ts == null || ts.stale()) {
            return null;
        }
        Histogram h = StatsEstimator.histogramForCondition(predicate, schema, ts);
        return h == null ? null : h.selectivity(predicate, h.totalRows());
    }

    @Override
    public @Nullable Double getDistinctRowCount(RelNode r, RelMetadataQuery mq,
                                                ImmutableBitSet groupKey, @Nullable RexNode predicate) {
        if (groupKey.cardinality() != 1) {
            return null;
        }
        TableStats ts = catalog.getStats(schema.schemaName(), schema.name());
        if (ts == null || ts.stale()) {
            return null;
        }
        int col = groupKey.nextSetBit(0);
        if (col < 0 || col >= schema.columns().size()) {
            return null;
        }
        String colName = schema.columns().get(col).name().toLowerCase(Locale.ROOT);
        Histogram h = ts.columnHistograms().get(colName);
        return h == null ? null : (double) h.distinctCount();
    }
}
```

(`getRowType` 方法原样保留。)

- [ ] **Step 6: 改两个 schema 类传 catalog**

`MiniDbRootCalciteSchema.getTableMap()` 与 `MiniDbCalciteSchema.getTableMap()` 里 `new MiniDbCalciteTable(ts)` → `new MiniDbCalciteTable(ts, catalog)`。

- [ ] **Step 7: 跑测试确认通过** `./mvnw.cmd test -pl minidb-server -Dtest=StatisticWiringTest,ExplainExecutorTest`

- [ ] **Step 8: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/stats/StatsEstimator.java minidb-server/src/main/java/com/minidb/server/exec/ExplainExecutor.java minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteTable.java minidb-server/src/main/java/com/minidb/server/calcite/MiniDbRootCalciteSchema.java minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteSchema.java minidb-server/src/test/java/com/minidb/server/calcite/StatisticWiringTest.java
git commit -m "feat: MiniDbCalciteTable 接入 getStatistic + 列级统计 Handler"
```

---

### Task 6: 集成回归

**Files:**
- Test: `minidb-server/src/test/java/com/minidb/server/stats/StatsPersistenceTest.java`(新建,端到端)

**Interfaces:** 无新增,验证阶段一整体行为。

- [ ] **Step 1: 写端到端测试 —— analyze 后 catalog.json 含统计、重启恢复、DML 置 stale、drop 删统计**

```java
package com.minidb.server.stats;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.storage.JsonCatalogStore;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatsPersistenceTest {

    @TempDir Path dataDir;

    @Test
    void analyzePersistsAndSurvivesRestart() {
        // 第一次会话:建表、插数据、analyze、关闭(flush)
        {
            BufferAllocator allocator = new RootAllocator();
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor q = new QueryExecutor(catalog, storage, allocator, stats);
            q.execute("CREATE TABLE t (id INTEGER)");
            q.execute("INSERT INTO t VALUES (1), (2), (2)");
            stats.analyze("t");
            assertEquals(3, catalog.getStats("public", "t").rowCount());
            storage.close();
            allocator.close();
        }
        // 第二次会话:只 loadAll,统计应从 catalog.json 恢复
        {
            BufferAllocator allocator = new RootAllocator();
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            storage.loadAll();
            assertNotNull(catalog.getStats("public", "t"));
            assertEquals(3, catalog.getStats("public", "t").rowCount());
            // catalog.json 确实含统计
            JsonCatalogStore store = new JsonCatalogStore(dataDir.resolve("catalog.json"));
            assertNotNull(store.load().stats().get("public.t"));
            storage.close();
            allocator.close();
        }
    }

    @Test
    void dmlMarksStatsStaleAndDropRemovesStats() {
        BufferAllocator allocator = new RootAllocator();
        MiniDbCatalog catalog = new MiniDbCatalog();
        StorageManager storage = new StorageManager(catalog, allocator, dataDir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor q = new QueryExecutor(catalog, storage, allocator, stats);
        q.execute("CREATE TABLE t (id INTEGER)");
        q.execute("INSERT INTO t VALUES (1)");
        stats.analyze("t");
        q.execute("INSERT INTO t VALUES (2)"); // DML → markStatsStale
        assertTrue(catalog.getStats("public", "t").stale());
        q.execute("DROP TABLE t");
        assertNull(catalog.getStats("public", "t"));
        storage.close();
        allocator.close();
    }
}
```

- [ ] **Step 2: 跑测试确认通过** `./mvnw.cmd test -pl minidb-server -Dtest=StatsPersistenceTest`

- [ ] **Step 3: 全量测试** `./mvnw.cmd test -pl minidb-server`(重点看 EXPLAIN 相关与所有 setUp 改过的测试全绿)

- [ ] **Step 4: 提交**

```bash
git add minidb-server/src/test/java/com/minidb/server/stats/StatsPersistenceTest.java
git commit -m "test: 统计持久化端到端(analyze→catalog.json→重启恢复)"
```

---

## Self-Review

- **Spec coverage:** ①(Histogram JSON 化 + TableStats rowCount + 统计归 catalog)→ Task 1-4;②(rowCount/distinct/selectivity 三路接入)→ Task 5;端到端验证 → Task 6。全部覆盖。
- **Placeholder scan:** 无 TBD/TODO;每个代码步骤都有具体代码块。
- **Type consistency:** `Histogram(type, buckets, mcv, distinctCount, nullCount, totalRows)`、`TableStats(columnHistograms, rowCount, stale)`、`catalog.getStats/setStats/markStatsStale`、`StatsEstimator.histogramForCondition/findFirstInputRef` 在 Task 1/2/3/4/5 中签名一致。
- **遗留注意:** 本计划不迁移旧 `.stats` 文件(数据目录里残留的旧文件被忽略);`StatsManager.analyze` 仍只支持 public 表(裸名,坑 18 已记),非 public 表统计降级。
