# TPC-DS 基准测试模块 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `minidb-tpcds` 模块,生成 TPC-DS 数据(直写 part 文件)、跑 99 条查询、存结果、出 2 次对比柱状图。

**Architecture:** 独立 Maven 模块,三个子命令(generate/run/compare)。数据生成用 teradata tpcds 库(内存生成)→ Arrow part 文件;查询用 Java 写的 `.tpl` 模板解析器生成 99 条 SQL;执行走 MiniDbServer + JDBC;结果存 JSON,对比生成 Chart.js HTML。

**Tech Stack:** Java 17、Maven、teradata tpcds 1.2、Arrow、JDBC、Jackson(复用 minidb-server 依赖)、Chart.js(CDN)。

**Spec:** `docs/superpowers/specs/2026-08-17-tpcds-benchmark-design.md`

## Global Constraints

- JDK 17;构建 `./mvnw.cmd`;conventional commit;小步提交;代码自解释 + WHY 注释。
- `minidb-tpcds` 是纯新增模块,不改 minidb-server/protocol/jdbc 稳定核心。
- 数据生成单线程(首版);攒批 4096 行;固定 seed 保证可复现。

---

### Task 1: 搭建 minidb-tpcds 模块

**Files:**
- Create: `minidb-tpcds/pom.xml`
- Modify: `pom.xml`(父 POM `<modules>` 加 `minidb-tpcds`)

**Interfaces:**
- Consumes: 父 POM 的 `calcite.version`、`arrow.version` 等属性。
- Produces: `com.minidb.tpcds` 包的模块,后续任务都放这里。

- [ ] **Step 1: 写 pom.xml**

`minidb-tpcds/pom.xml`(packaging jar),依赖:`com.teradata.tpcds:tpcds:1.2`、`com.minidb:minidb-server`、`com.minidb:minidb-jdbc`、`com.minidb:minidb-storage-common`(或经 minidb-server 传递)。父 POM 里 `<modules>` 追加 `<module>minidb-tpcds</module>`。

- [ ] **Step 2: 编译验证**

Run: `./mvnw.cmd -pl minidb-tpcds -am compile -q`
Expected: BUILD SUCCESS(空模块可编译)。

- [ ] **Step 3: 提交**

```bash
git add minidb-tpcds/pom.xml pom.xml
git commit -m "feat: 新增 minidb-tpcds 模块骨架"
```

---

### Task 2: 数据生成器 `TpcdsDataGenerator`

**Files:**
- Create: `minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsDataGenerator.java`

**Interfaces:**
- Consumes: `com.teradata.tpcds.{Session,Table,Results}`、`com.minidb.storage.common.{ColumnMeta,ColumnType,TableSchema,SimpleTable}`、`com.minidb.server.storage.StorageManager`、`ArrowTypes`、`Kernels.scaleTo`。
- Produces: `void generate(double scale, Path dataDir)` —— 24 张表全生成。

- [ ] **Step 1: 写列类型映射 + 值解析**

`toColumnMeta(com.teradata.tpcds.column.Column c)`:按 `c.getType().getBase()` 映射 INTEGER→INTEGER、IDENTIFIER→BIGINT、DECIMAL→DECIMAL(precision/scale 从 `getPrecision()/getScale()` 取,缺省 10/0)、VARCHAR/CHAR→VARCHAR、DATE→DATE、TIME→TIME。

`setValue(FieldVector v, int row, ColumnType type, String raw, BufferAllocator allocator)`:raw 为 null 或等于 `session.getNullString()` → `setNull`;否则按类型 parse(INTEGER→`Integer.parseInt`、BIGINT→`Long.parseLong`、DECIMAL→`Kernels.scaleTo(v, new BigDecimal(raw))`、VARCHAR→字节 `setSafe(row, bytes)`、DATE→`new DateString(raw).getDaysSinceEpoch()`、TIME→`TimeString`/手写 `HH:MM:SS` 转毫秒)。

- [ ] **Step 2: 写 generate 主流程**

`Session session = Session.getDefaultSession().withScale(scale)`。对 `Table.getBaseTables()` 每张表:构造 `TableSchema`(schemaName="public",表名小写)→ `StorageManager.createTable(schema)` 拿 `SimpleTable` → `Results.constructResults(table, session)` 迭代,每行 `List<String>`(取第一个子行 `row.get(0)`),逐列 `setValue` 入 `table.newBatchRoot()`,满 4096 行 `setRowCount` + `writePart` + close;尾批同理。`StorageManager.close()`。

- [ ] **Step 3: 写测试(小 scale)**

`TpcdsDataGeneratorTest`:`@TempDir` + `generate(0.01, dir)` 后断言 `catalog.hasTable("public","store_sales")`、`storage.getTable(...).rowCount() > 0`、表目录有 `part-*.arrow`。

Run: `./mvnw.cmd test -pl minidb-tpcds -Dtest=TpcdsDataGeneratorTest -am -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS。

- [ ] **Step 4: 提交**

```bash
git add minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsDataGenerator.java minidb-tpcds/src/test/java/com/minidb/tpcds/TpcdsDataGeneratorTest.java
git commit -m "feat: TPC-DS 数据生成器(直写 part 文件)"
```

---

### Task 3: 模板解析器 `TpcdsTemplateParser`

**Files:**
- Create: `minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsTemplateParser.java`

**Interfaces:**
- Consumes: `query_templates/*.tpl`(运行时从 `--query-dir` 读)。
- Produces: `Map<String,String> parseAll(Path templateDir, double scale)`(queryNN → SQL);内部 `String parseTemplate(String tpl, double scale, int queryNumber)`。

- [ ] **Step 1: 实现 define 求值**

手写递归下降解析 define 右侧 `expr`(整数字面量、字符串字面量、`[...]` 变量引用、函数调用、`+ - * /`)。函数实现(固定 seed `Random(42)`):`random(a,b,uniform)`→[a,b] 整数;`rowcount(name)`→查硬编码分布大小表或 `ScalingInfo`;`distmember(dist,[key],idx)`/`dist(dist,…)`→返回硬编码合法候选(按 dist 名);`text({s,w},…)`→加权随机选 s;`ulist/list/range(expr,n)`→n 个不同整数;`date(…)`→固定日期字符串;`scalestep()`→1。变量表 `Map<String,Object>`(整数或字符串或列表)。

- [ ] **Step 2: 实现 substitution 替换**

去掉所有 `define …;` 行后,替换 SQL 文本:`[VAR]`→值;`[VAR n]`→列表第 n 个;`[VAR.n]`→后缀;`[_LIMITA]`/`[_LIMITB]`→当 `_LIMIT` 定义时展开为 `LIMIT n`;`[QUERY]`→queryNumber、`[TEMPLATE]`→`queryNN.tpl`、`[SEED]`/`[STREAM]`→固定数字。

- [ ] **Step 3: 写测试**

`TpcdsTemplateParserTest`:对 DSGen 99 个 `.tpl` 全部 `parseAll` 不抛异常、无残留 `[`/`define`;同 seed 两次输出一致;抽样 query1 含 `customer_total_return` 且不含 `[YEAR]`。

Run: `./mvnw.cmd test -pl minidb-tpcds -Dtest=TpcdsTemplateParserTest`
Expected: PASS。

- [ ] **Step 4: 提交**

```bash
git add minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsTemplateParser.java minidb-tpcds/src/test/java/com/minidb/tpcds/TpcdsTemplateParserTest.java
git commit -m "feat: TPC-DS 查询模板解析器(.tpl → SQL)"
```

---

### Task 4: 执行器 `TpcdsBenchmarkRunner`

**Files:**
- Create: `minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsBenchmarkRunner.java`

**Interfaces:**
- Consumes: `MiniDbServer`、JDBC `DriverManager`、`TpcdsTemplateParser.parseAll`。
- Produces: `void run(Path dataDir, Path queryDir, Path outputJson)`。

- [ ] **Step 1: 实现**

启动 `MiniDbServer`(port 0 + dataDir)。JDBC 连接后,遍历 `queryDir` 下 `query*.sql`(按号排序):`nanoTime` 计时 → `Statement.execute` → 若 `getResultSet()!=null` 则遍历计行数 → 记录 `QueryResult(name, elapsedMs, rowCount, success, error)`;异常捕获(截断 error 到 200 字符)继续。结果用 Jackson `ObjectMapper.writeValue` 写 JSON。

- [ ] **Step 2: 提交**

```bash
git add minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsBenchmarkRunner.java
git commit -m "feat: TPC-DS 查询执行器(计时 + JSON 结果)"
```

---

### Task 5: 结果对比 `TpcdsCompare`

**Files:**
- Create: `minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsCompare.java`

**Interfaces:**
- Consumes: 两个 JSON 结果文件。
- Produces: `void compare(Path run1, Path run2, Path outputHtml)`。

- [ ] **Step 1: 实现**

读 2 个 JSON → 按查询名对齐 → 生成单 HTML:`<script src=chart.js CDN>` + `<canvas>` + 内嵌 `labels`/`dataset`(两次耗时)+ 一个表格(逐条耗时/行数/失败原因)。`<script>` 里用 `new Chart(...)` 分组柱状图。

- [ ] **Step 2: 提交**

```bash
git add minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsCompare.java
git commit -m "feat: TPC-DS 结果对比(HTML 柱状图)"
```

---

### Task 6: CLI 入口 `TpcdsBenchmark`

**Files:**
- Create: `minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsBenchmark.java`

- [ ] **Step 1: 实现**

`main(String[] args)`:手写参数解析(不用 airline),`generate --scale X --data-dir Y`、`run --data-dir Y --query-dir Q --output O`、`compare A B --output O` 三子命令,委托 Task 2/4/5。

- [ ] **Step 2: 提交**

```bash
git add minidb-tpcds/src/main/java/com/minidb/tpcds/TpcdsBenchmark.java
git commit -m "feat: TPC-DS 基准 CLI 入口"
```

---

### Task 7: 端到端测试 + 全量回归

**Files:**
- Create: `minidb-tpcds/src/test/java/com/minidb/tpcds/TpcdsEndToEndTest.java`

- [ ] **Step 1: 写端到端测试**

scale 0.01:`generate` → 断言数据;`parseAll` + `run` → 断言 JSON 含 99 条、有 success 有失败;`compare` → 断言 HTML 含 `Chart`/`canvas`。

- [ ] **Step 2: 全量回归**

Run: `./mvnw.cmd test`
Expected: 全绿(含 minidb-tpcds 新测试,minidb-server 等无回归)。

- [ ] **Step 3: 提交**

```bash
git add minidb-tpcds/src/test/java/com/minidb/tpcds/TpcdsEndToEndTest.java
git commit -m "test: TPC-DS 端到端测试"
```

---

## 完成定义

- `generate --scale 0.1` 产出 24 张表的 part 文件 + catalog。
- 99 个 `.tpl` 全部能解析出 SQL;`run` 产出含 99 条的 JSON;`compare` 产出 Chart.js HTML。
- 全量 `./mvnw.cmd test` 绿,minidb-server 无回归。
