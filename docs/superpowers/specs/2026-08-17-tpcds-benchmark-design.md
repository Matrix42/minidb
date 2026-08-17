# TPC-DS 基准测试模块设计

日期: 2026-08-17
状态: 已确认,待实现
范围: 新增 `minidb-tpcds` 模块,用 TPC-DS 数据对 MiniDB 做基准测试:生成数据(直写 part 文件)、跑 99 条标准查询、存储结果、出 2 次运行对比柱状图

## 目标

给 MiniDB 一个可复现的基准测试工具。核心诉求:

1. **数据生成**:用 teradata tpcds 库(JVM 内生成,`com.teradata.tpcds:tpcds:1.2`)生成 TPC-DS 24 张表的数据,scale factor 可配置(0.01/0.1/1/10 …)。
2. **直写表目录**:不经过 SQL INSERT(1GB 数据逐行 INSERT 太慢),而是把生成的行直接编码成 Arrow IPC part 文件写到 `data/public/<table>/part-*.arrow`,同时注册 catalog。
3. **查询执行**:从 DSGen 的 `query_templates/*.tpl` 生成 99 条 SQL(方案 B:写 Java 模板解析器),逐条跑,记录耗时/行数/成败。
4. **结果与对比**:每次运行存一个 JSON;选 2 次结果生成 HTML 柱状图(Chart.js,浏览器打开)对比每条查询耗时。

## 关键决策(已与用户确认)

1. **scale factor 可配置**:命令行参数,支持 0.01/0.1/1/10 等任意 double。
2. **数据直写表目录**:数据加载器独立于 MiniDbServer 运行,直接操作 `StorageManager`/`SimpleTable` 写 part 文件,绕开 SQL DML。
3. **查询走完整 99 条(方案 B)**:写 Java 的 `.tpl` 模板解析器。MiniDB 不支持的 SQL(如 `ROLLUP`/`GROUPING SETS`/`CUBE`、部分函数)会失败,记录为失败——这是预期,首版不追求官方 answer 校验,只测「能否执行 + 耗时」。
4. **结果 JSON + HTML 柱状图**:无额外重依赖,柱状图用 Chart.js 生成单个自包含 HTML 文件。

## 架构与数据流

新 Maven 模块 `minidb-tpcds`,依赖 `com.teradata.tpcds:tpcds:1.2`(传递引入 guava/airline)+ `minidb-server`(StorageManager/MiniDbServer)+ `minidb-jdbc`(JDBC 驱动)+ `minidb-storage`(SimpleTable/ArrowPartFormat)。

单一 CLI 入口 `TpcdsBenchmark`,三个子命令:

```
generate --scale 0.1 --data-dir ./data
run      --data-dir ./data --query-dir ./queries --output ./results/run-1.json
compare  run-1.json run-2.json --output ./results/report.html
```

数据流:

```
generate:
  teradata Session.withScale(scale)
  → Table.getBaseTables() (24 张)
  → 每张表: Table.getColumns() → TableSchema(列类型映射)
  → Results.constructResults(table, session) 迭代行 (List<String>)
  → 按列类型解析字符串 → Arrow 向量 → 攒批(4096 行) → SimpleTable.writePart
  → StorageManager.createTable(注册 catalog + 落 catalog.json)

run:
  TpcdsTemplateParser(99 个 .tpl) → 99 条 SQL
  → 启动 MiniDbServer(dataDir)
  → JDBC 逐条 executeQuery,计时、统计行数、捕获异常
  → 结果写 JSON {scale, timestamp, queries:[{name, elapsedMs, rowCount, success, error}]}

compare:
  读 2 个 JSON → 对齐查询名 → 生成 report.html(Chart.js 分组柱状图:每查询 2 根柱)
```

## 分层设计

### 1. 数据生成器 `TpcdsDataGenerator`

- 输入:scale(double)、dataDir(Path)。
- `Table.getBaseTables()` 得到 24 张表;`table.getColumns()` 得到 `Column[]`(name、ColumnType(Base + precision/scale)、position)。
- **列类型映射**(`ColumnType.Base` → MiniDB `ColumnType`):

| tpcds Base | MiniDB ColumnType | Arrow 向量 |
|---|---|---|
| INTEGER | INTEGER | IntVector |
| IDENTIFIER | BIGINT | BigIntVector |
| DECIMAL | DECIMAL(precision, scale) | DecimalVector |
| VARCHAR / CHAR | VARCHAR | VarCharVector |
| DATE | DATE | DateDayVector |
| TIME | TIME | TimeMilliVector |

- **值解析**(Results 的值是 `List<String>`,null 用 `session.getNullString()` 或 `null`):
  - INTEGER/IDENTIFIER → `Integer.parseInt`/`Long.parseLong`
  - DECIMAL → `new BigDecimal`(经 `Kernels.scaleTo` 落 DecimalVector)
  - VARCHAR/CHAR → 字符串字节 → VarCharVector
  - DATE(`"YYYY-MM-DD"`)→ `new DateString(s).getDaysSinceEpoch()` → DateDayVector
  - TIME(`"HH:MM:SS"`)→ 转毫秒 → TimeMilliVector
  - null → `setNull`
- 攒批到 `MAX_BATCH_ROWS=4096`,每满一批 `SimpleTable.writePart`;单线程(首版,不并行)。
- 每张表:构造 `TableSchema` → `StorageManager.createTable`(建目录 + 注册 catalog)→ 写 part。`createTable` 内部已触发 catalog 持久化(listener 落 catalog.json),无需额外 flush。

### 2. 查询生成器 `TpcdsTemplateParser`

解析 DSGen `query_templates/*.tpl`(99 个),生成 99 条 SQL。语法子集(从 `qgen.y` 提取,**不含**自定义 `dist` 定义语句——模板里实际未用):

- **define 语句**:`define VAR = expr;` 与 `define _LIMIT = INT;`。
- **expr**:整数字面量、字符串字面量、`[...]` 变量引用、`SCALE`、`function_call`、算术 `+ - * /`。
- **函数**(全部出现在 99 模板里的):
  - `random(a, b, uniform)` → 整数随机
  - `rowcount("name")` / `rowcount("name","col")` → 表行数或分布大小
  - `distmember(dist, [key], idx)` / `dist(dist, ...)` → 分布成员
  - `text({literal, weight}, ...)` → 加权随机选一个字符串
  - `ulist(expr, n)` / `list(expr, n)` / `range(expr, n)` → 生成 n 元素列表(用于 `IN (...)` 或 `UNION`)
  - `date(...)` → 生成合法日期
  - `scalestep()` → scale 槽位
- **keyword**:`uniform`/`sales`/`returns`;预定义值 `[QUERY]`/`[TEMPLATE]`/`[SEED]`/`[STREAM]`。
- **substitution 替换**:`[VAR]`、`[VAR n]`(取变量列表第 n 个)、`[VAR.n]`(后缀),以及特殊 `[_LIMITA]`/`[_LIMITB]`(当 `_LIMIT` 已定义时展开为 `LIMIT n`/`LIMIT n` 或 `TOP n`,按模板上下文)。

**求值简化策略**(不追求与官方 qgen 精确一致,只求「类型正确 + 值在数据范围内」+ 可复现):

- 固定 seed(如 42)的 `java.util.Random`,保证多次生成结果一致。
- `random(a,b,uniform)` → 返回 `[a,b]` 内随机整数。
- `rowcount` → 表名用 `ScalingInfo.getRowCountForScale(scale)`;分布名用硬编码的分布大小表。
- `distmember`/`dist` → 分布值域来自 DSGen `tools/*.dst`(fips/cities/tpcds/items/names 等),实现时解析 `.dst` 或对高频分布(fips_county/categories/cities/marital_status/education/gender/colors/…)硬编码合法候选。
- `text({a,w},...)` → 按权重随机选一个(固定 seed)。
- `ulist/list/range` → 生成 n 个不同整数值。

生成结果写 `--query-dir/queryNN.sql`(99 个文件),供 run 阶段复用(生成一次,跑多次)。

### 3. 执行器 `TpcdsBenchmarkRunner`

- 启动 `MiniDbServer`(端口 0 + dataDir),`loadAll()` 恢复 catalog + part 文件。
- JDBC 连接(`jdbc:minidb://127.0.0.1:port`),对 `query-dir` 里每个 `queryNN.sql`:
  - `System.nanoTime` 计时 → `Statement.execute` / `executeQuery`
  - 统计返回行数(遍历 ResultSet 计数)或受影响行
  - 捕获异常:失败记录 `success=false` + 错误信息(截断),不中断后续查询
- 结果对象 → JSON(用现有 Jackson,minidb-server 已依赖)。结构:

```json
{
  "scale": 0.1,
  "timestamp": "2026-08-17T17:00:00",
  "queries": [
    {"name": "query1", "elapsedMs": 123, "rowCount": 456, "success": true, "error": null},
    {"name": "query2", "elapsedMs": -1, "rowCount": -1, "success": false, "error": "CannotPlan ..."}
  ]
}
```

### 4. 结果对比 `TpcdsCompare`

- 读 2 个 JSON,按查询名对齐(缺的查询补 null)。
- 生成单个自包含 HTML:`<canvas>` + Chart.js(CDN 引用)+ 内嵌数据。分组柱状图:X 轴 = 查询名(99 根),每查询 2 根柱(第 1 次/第 2 次耗时);失败查询柱高 0 且标注。附一个表格列出逐条耗时与失败原因。

## 测试

- **数据生成器**:小 scale(0.01)生成后,断言每张表目录存在 part 文件、`rowCount()` 等于 `ScalingInfo.getRowCountForScale(scale)`、`COUNT(*)` 能查。
- **模板解析器**:对 99 个 `.tpl` 全部解析不抛异常、产出非空 SQL;抽样断言变量被替换(无残留 `[...]`/`define`)、`text`/`random` 结果可复现(同 seed 两次输出一致)。
- **执行器 + 对比**:端到端跑 0.01 scale,断言 JSON 含 99 条、`success` 有真有假、`compare` 产出 HTML 文件且含 `Chart`/`canvas` 标记。
- **MiniDB 回归**:全量 `./mvnw.cmd test` 不回归(minidb-tpcds 是纯新增模块,不改稳定核心)。

## 不在本范围

- 官方 answer set 校验(结果正确性 vs `answer_sets/`),本模块只测「能跑 + 耗时」。
- 并行数据生成(teradata 库的 parallelism/chunk 机制)。
- 数据加载后的 compaction(part 数可能很多,首版不合并)。
- 99 条之外的自定义查询;多 scale 自动对比(compare 只做 2 次 run 结果对比)。
- MiniDB 未支持 SQL 特性的补齐(失败即记录,不扩展内核)。
