# EXPLAIN / EXPLAIN ANALYZE 设计

日期: 2026-08-11
状态: 已确认,待实现

## 目标

为 MiniDB 实现 `EXPLAIN <query>`、`EXPLAIN ANALYZE <query>` 和 `ANALYZE <table>` / `ANALYZE`,提供查询计划可视化与运行时统计。同时顺带实现一个轻量直方图统计信息子系统,供 plain EXPLAIN 估算 Filter 基数。

## 决策汇总

| 决策 | 选择 |
|---|---|
| 输出形态 | 表格化算子树(id / parent_id / operation / rows / batches / elapsed_ms / remarks) |
| ANALYZE 对 DML | 只读,拒绝 DML(plain 与 ANALYZE 均拒绝) |
| 直方图统计 | 本次顺带实现 |
| 采集触发 | 显式 `ANALYZE t` / `ANALYZE` 命令,持久化 `.stats` 文件 |
| 估算谓词 | 等值 + 范围;AND/OR/NOT 独立性;其余默认 1/3 |
| 直方图类型 | 等频(equi-depth),固定 10 桶 + top-10 MCV |
| 统计失效 | 表修改后标记 stale,估算退化为默认并标注 |
| ANALYZE 语法 | `ANALYZE t`(单表)+ `ANALYZE`(全部表) |
| 估算值呈现 | rows 统一列,Filter 估算与 Scan 真实混排,remarks 标注来源 |
| 实现路径 | 路径 A:统计作独立子系统,EXPLAIN 作独立执行路径,现有算子零侵入 |

## 第 1 节:整体架构

新增三个模块,都在 `minidb-server`,与现有算子/执行器平级:

1. **`stats/StatsManager`** — 直方图采集、内存缓存、`.stats` 持久化、stale 标记。依赖 `StorageManager`(拿表数据扫描)。
2. **`stats/Histogram`** — 等频直方图数据结构(桶边界 + MCV + distinct count + null count),及选择率估算方法。
3. **`exec/ExplainExecutor`** — 把 EXPLAIN/EXPLAIN ANALYZE 的算子树转成结果集。

`QueryExecutor.execute(sql)` 顶部增加前缀识别(`EXPLAIN ANALYZE ` / `EXPLAIN ` / `ANALYZE`),在 Calcite parse 之前分流,绕开默认语法无 `ANALYZE` 关键字的限制:

```
QueryExecutor.execute(sql)
  ├─ "EXPLAIN ANALYZE <q>"  → ExplainExecutor.analyze(q)   // 执行 q,采集真实统计
  ├─ "EXPLAIN <q>"          → ExplainExecutor.explain(q)   // 不执行,用统计估算
  ├─ "ANALYZE [t]"          → StatsManager.analyze(t)      // 采集直方图,持久化
  └─ 其余                    → 现有逻辑
```

`ANALYZE t` / `ANALYZE` 返回 `QueryResult.Update(0)`(无结果集,只副作用 + 日志)。

**关键边界**:现有 6 个算子文件(`MiniDbScan`/`MiniDbFilter`/`MiniDbProject`/`MiniDbSort`/`MiniDbValues`/`MiniDbModify`)**完全不改动**。统计估算在 `ExplainExecutor` 里基于 `RelNode` 结构独立算;ANALYZE 的真实统计通过插桩包装器在外部采集,不进算子内部。

## 第 2 节:统计信息子系统

### 2.1 `Histogram` 数据结构(`stats/Histogram.java`)

不可变值对象,描述单列的等频直方图。字段:

- `int bucketCount`(固定 10 桶)
- `Bucket[] buckets` — 每桶 `{lower, upper, rowCount}`,等频即每桶 rowCount ≈ 总行数 / bucketCount
- `List<McValue> mcv` — Most Common Values(高频值 + 频次),最多 top-10
- `long distinctCount`、`long nullCount`、`long totalRows`

选择率方法 `double selectivity(RexNode condition, long inputRows)`,按谓词类型分派:

| 谓词 | 选择率 |
|---|---|
| `col = const` | 命中 MCV → `mcvFreq / totalRows`;否则 `1 / distinctCount`(均匀假设) |
| `col < const`(及 `>` `<=` `>=`) | 在等频直方图里二分定位 const 所在桶,按桶内线性插值估算命中比例 |
| `col BETWEEN a AND b` | 两个范围选择率之差 |
| `a AND b` | `sel(a) * sel(b)`(独立性) |
| `a OR b` | `sel(a) + sel(b) - sel(a)*sel(b)`(独立性 + 容斥) |
| `NOT a` | `1 - sel(a)` |
| 其余(`LIKE` / `IS NULL` / 复杂表达式) | 默认 `0.33`,remarks 标注 "default selectivity" |

只对可解析的 `RexCall`(比较运算 + AND/OR/NOT)建模;解析不了的退默认。类型支持与 `ArrowTypes` 对齐:数值(INT/BIGINT/DOUBLE)、VARCHAR、BOOLEAN、DATE、TIMESTAMP。

### 2.2 `StatsManager`(`stats/StatsManager.java`)

管理所有表的列直方图,依赖 `StorageManager`。

- 内存状态:`Map<String, TableStats>`,`TableStats = { Map<列名, Histogram>, boolean stale }`
- `analyze(String table)`:全表扫一遍,每列排序后建等频直方图 + 采 MCV,存入 `TableStats`,`stale=false`
- `analyzeAll()`:对所有表调 `analyze`
- `tableStats(table)`:取统计;若不存在或 `stale=true`,Filter 估算退默认值
- **stale 触发**:`StatsManager` 暴露 `markStale(table)`,`StorageManager.markDirty()` 调用处(INSERT/UPDATE/DELETE/TRUNCATE 的写路径)同时调 `markStale`。这是唯一的失效入口,集中在写路径,不会漏。
- **持久化**:`.stats` 文件用 Java 序列化(直方图都是简单值对象,序列化够用,不引入新依赖)。`analyze` 完写入 `data/<table>.stats`,启动时 `loadAll()` 加载。表 DROP 时删 `.stats`。

### 2.3 `QueryExecutor` 顶部前缀识别

`execute(sql)` 开头(在 `calcite.parse` 之前):

```java
String trimmed = sql.strip();
String upper = trimmed.toUpperCase(Locale.ROOT);
if (upper.equals("ANALYZE") || upper.startsWith("ANALYZE ")) { ... }   // ANALYZE / ANALYZE t
if (upper.startsWith("EXPLAIN ANALYZE ")) { ... }
if (upper.startsWith("EXPLAIN ")) { ... }
```

前缀识别处理大小写、首尾空格、`EXPLAIN ANALYZE` 与 `EXPLAIN` 的优先级(先判 ANALYZE 子串)。剩余 `<q>` 用现有 `Planner.plan()` 规划成物理算子树,交给 `ExplainExecutor`。

## 第 3 节:`ExplainExecutor`

三个职责:算子树遍历、plain 估算、ANALYZE 真实采集。

### 3.1 算子树遍历与 id 分配

后序遍历 `RelNode`(物理树根必为 `MiniDbRel`),为每个节点分配 `id`(从 1 递增)和 `parent_id`(父节点 id,根为 NULL)。遍历顺序采用前序(先根后子),这样根(id=1)排在第一行、Scan 排最后,符合 PostgreSQL 风格(根在上、叶子在下)。

每个节点的 `operation` 取 `RelNode` 类名去掉 `MiniDb` 前缀(`MiniDbSort` → `Sort`),Scan 带表名(`Scan(t)`)。

结果列固定为 7 列,plain 和 ANALYZE 共用同一 schema:

```
id (INT) | parent_id (INT nullable) | operation (VARCHAR)
rows (BIGINT nullable) | batches (INT nullable)
elapsed_ms (DOUBLE nullable) | remarks (VARCHAR)
```

### 3.2 plain EXPLAIN 估算(不执行)

对每个算子,`rows` 按算子类型算:

| 算子 | rows 估算 | batches | elapsed_ms | remarks |
|---|---|---|---|---|
| Scan | `table.rowCount()`(免费) | `table.batches().size()`(免费) | NULL | 空 |
| Project | 输入 rows(透传) | NULL | NULL | 空 |
| Sort | `输入 - offset` 再 `min(, fetch)`(offset/fetch 是字面量) | NULL | NULL | 空 |
| Values | 元组数(`tuples.size()`) | 1 或 NULL | NULL | 空 |
| Filter | `输入rows × Histogram.selectivity()` | NULL | NULL | "estimated" / "default selectivity" / "stats stale" |
| Modify | 不出现(DML 被拒) |

**plain EXPLAIN 对 DML 的处理**:plain EXPLAIN 遇到 Modify 也报错("EXPLAIN does not support DML"),与 ANALYZE 行为统一。理由:EXPLAIN 语义是展示查询计划,DML 计划结构与查询不同,本次范围聚焦读路径。

Filter 估算依赖统计:`ExplainExecutor` 持有 `StatsManager`。遇到 Filter 时沿 `getInput()` 下溯到 Scan 找表名,取该列的 `Histogram`;若统计缺失或 stale,`rows` 用 `输入rows × 0.33`,remarks 标注对应原因("stats stale" / "no stats")。

### 3.3 EXPLAIN ANALYZE 真实采集(执行)

核心是**插桩影子树**。由于 MiniDb 是拉模式执行、每算子 `execute()` 内部直接调子算子 `execute()`,无法从外部包根迭代器分层计时。因此构造一棵插桩版算子树。

`Instrumenter` 工具递归 `RelNode` 树,对每个 `MiniDbRel` 节点:

1. 递归把子输入替换为已插桩的子节点(走各算子已有的 `copy(traitSet, inputs...)`;Scan 例外,无输入)
2. 用 `InstrumentedRel` 包住复制后的节点

`InstrumentedRel.execute(ctx)` 做三件事:
- 调被包算子的 `execute(ctx)` 拿子迭代器
- 用 `InstrumentedIterator` 包住该迭代器,在 `next()` 时累加 rows/batches、用 `System.nanoTime()` 计 elapsed
- 记录本节点统计到 `Map<RelNode, NodeStats>`

算子仍以为自己调的是子算子(只是子算子被换成了插桩版),无需改算子内部。执行完 `ExplainExecutor` 按 id 顺序从 `NodeStats` 读出真实 rows/batches/elapsed_ms 填入结果集。

ANALYZE 模式先检测根算子是否 `MiniDbModify`,是则抛 "EXPLAIN ANALYZE does not support DML"。

## 第 4 节:协议、客户端与错误处理

### 4.1 协议层 — 零改动

EXPLAIN 和 EXPLAIN ANALYZE 都产出一个普通 `QueryResult.Rows`(7 列 `VectorSchemaRoot`),走现有 `Message.ArrowBatch` 链路。`ANALYZE t` / `ANALYZE` 产出 `QueryResult.Update(0)`,走现有 `Message.UpdateCount`。`minidb-protocol` 模块、`SessionHandler`、`MessageType` 全部不改。

`SessionHandler.handleExecute` 现有逻辑已能处理这两种 `QueryResult`,无需分支。日志复用现有 "query ok: N rows returned" / "N rows affected"。

### 4.2 客户端 — 零改动

`MiniDbStatement.execute(sql)` 直接把 `EXPLAIN ...` / `ANALYZE ...` 原样发给服务端。`MiniDbResultSet` 把 7 列当普通结果集读,`MiniDbResultSetMetaData` 对 VARCHAR/INT/BIGINT/DOUBLE 已有映射。

```java
ResultSet rs = stmt.executeQuery("EXPLAIN SELECT id FROM t WHERE id > 1");
while (rs.next()) {
    System.out.printf("%d %s rows=%s%n",
        rs.getInt("id"), rs.getString("operation"), rs.getObject("rows"));
}
```

`ANALYZE t` 用 `stmt.executeUpdate`(返回 0)或 `execute`(返回 false,`getUpdateCount`=0)。

### 4.3 错误处理

| 情况 | 行为 |
|---|---|
| `EXPLAIN <DML>` / `EXPLAIN ANALYZE <DML>` | 抛 `IllegalArgumentException("EXPLAIN does not support DML")`,走现有 `SessionHandler` catch → `ExecuteResponse.error` |
| `EXPLAIN <语法错的 q>` | 内层 `Planner.plan(q)` 抛原有异常,透传 |
| `ANALYZE` 涉及不存在的表 | `StatsManager.analyze` 抛 `IllegalArgumentException("table not found")` |
| 统计缺失/stale 时 plain EXPLAIN | 不报错,Filter rows 用默认选择率,remarks 标注 |
| ANALYZE 采集时表为空 | 直方图正常建(0 行,buckets 全空),distinctCount=0,等值选择率退默认 |

### 4.4 资源生命周期

- EXPLAIN 结果集 `VectorSchemaRoot` 由 `QueryExecutor.execute` 构造,`SessionHandler.sendRows` 发送后 `close()`(现有逻辑)。
- ANALYZE 插桩影子树:执行完算子 `BatchIterator.close()` 关闭所有 owned batch,插桩包装器不额外持有 Arrow 资源,无需特殊清理。
- `StatsManager.analyze` 扫描用的临时排序结果在方法内释放,不持久化占内存。

## 第 5 节:测试策略

沿用现有风格(JUnit 5 + `@TempDir` + `RootAllocator`,在 `QueryExecutorTest` 同层加测试)。

### 5.1 统计子系统单元测试(`stats/` 下)

- `HistogramTest`:构造已知直方图,断言各谓词选择率(等值命中 MCV、等值未命中走 1/distinct、范围二分插值、AND/OR/NOT 组合、默认 1/3 兜底)。
- `StatsManagerTest`:`analyze(t)` 后能取到各列直方图;`markStale` 后 `tableStats().stale()` 为 true;持久化——`analyze` 后 `close()` 再重新构造 `StatsManager` + `loadAll()`,统计仍在。

### 5.2 EXPLAIN 端到端测试(`QueryExecutorTest` 加用例)

| 测试 | 断言 |
|---|---|
| `explainSelectPlanTree` | `EXPLAIN SELECT id,name FROM t WHERE id>1 ORDER BY id` 返回行数 = 算子数(Sort/Filter/Scan),operation 含 `Sort`/`Filter`/`Scan`,id/parent_id 树结构正确,Scan 的 rows = 插入行数 |
| `explainFilterRowsEstimated` | 统计已采集时,Filter rows > 0 且 < Scan rows,remarks 含 "estimated";统计缺失时 remarks 含 "default" 或 "stale" |
| `explainAnalyzeRunsAndMeasures` | `EXPLAIN ANALYZE SELECT ... WHERE id>1` 每个算子的 rows/batches/elapsed_ms 均非 NULL,Filter rows 等于真实命中行数 |
| `explainRejectsDml` | `EXPLAIN INSERT INTO t VALUES (1)`、`EXPLAIN ANALYZE DELETE FROM t` 抛异常 |
| `analyzeCommandCollectsStats` | `ANALYZE t` 后 `StatsManager.tableStats("t")` 非空且非 stale;`ANALYZE`(全表)对所有已建表都非空 |
| `analyzeStaleAfterModify` | `ANALYZE t` 后 `INSERT INTO t VALUES (...)`,再 plain EXPLAIN,Filter remarks 含 "stale" |
| `statsPersistAcrossRestart` | `ANALYZE t` → 关闭 `StorageManager` → 重建 → `loadAll()` → 直方图仍在 |

### 5.3 不测的

- 协议/客户端:`minidb-jdbc` 侧无需新增测试,EXPLAIN 结果走现有 Arrow 解码路径,已被 `ArrowResultDecoderTest` / `PersistenceTest` 覆盖。若实现时协议确实零改动,不加冗余测试。
- 直方图精确数值:断言关系(范围、比例)而非具体浮点值,避免脆弱。

### 5.4 验收标准

1. `mvnw.cmd test` 全绿(含新增测试)。
2. 现有所有用例不回归。
3. 手动跑一次服务端,用 JDBC 客户端执行 `ANALYZE t`、`EXPLAIN SELECT ...`、`EXPLAIN ANALYZE SELECT ...`,确认结果集可读(因无 CLI,手动验证由用户执行)。

## 不在本次范围

- 基于代价的优化器(CBO):本次统计仅用于 EXPLAIN 估算展示,不改 `Planner` 的物理算子选择。未来若做 CBO,可让 `VolcanoPlanner` 接入 `StatsManager` 的行数估算,届时 plain EXPLAIN 顺带用上同一套估算。
- 直方图自动维护(表变更后自动重采集):本次仅标记 stale + 手动重跑 `ANALYZE`。
- JOIN / 聚合的基数估算:库本身不支持这些,统计子系统不涉及。
