# 统计持久化迁移 + 接入 CBO 设计

日期:2026-08-14
状态:待评审

## 目标

1. 把表统计从 `.stats` 文件(Java 序列化)迁到 `data/catalog.json`(Jackson JSON 元数据存储),与 schema/表定义统一持久化。
2. 把统计接入 `MiniDbCalciteTable.getStatistic()`,让 Calcite 的 `RelMetadataQuery.getRowCount` 等元数据链路读到真实行数。
3. 加入 Cost-Based 规则:join 重排序 + 按代价选 join 算法。

分两阶段交付(依赖关系:①→②→③):

- **阶段一**:① 统计迁入 catalog.json(完整直方图)+ ② 接入 `getStatistic()`。
- **阶段二**:③ CBO 规则。

## 现状

- `StatsManager` 用 `ObjectOutputStream` 把 `TableStats`(每列一个 `Histogram`)写到 `data/<schema>.<table>.stats`,`loadAll` 扫 `*.stats` 反序列化。`Histogram`/`TableStats` 均 `implements Serializable`。
- `JsonCatalogStore` 用 Jackson 把 `CatalogSnapshot(schemas, tables)` 写到 `data/catalog.json`;`MiniDbCatalog` 挂 listener 在每次 DDL 后 `persistCatalog`。
- `MiniDbCalciteTable.getStatistic()` 硬编码返回 `Statistics.UNKNOWN`;它只持有 `TableSchema`,在 `MiniDbRootCalciteSchema`/`MiniDbCalciteSchema.getTableMap()` 里现场 `new`。
- join 由 `MiniDbJoinRule`(单条 ConverterRule)按 `isEqui()` + `coversKeys()` 确定性选 Hash/SortMerge/NestedLoop,无成本备选。
- `ExplainExecutor` 是统计唯一消费者:`filterSelectivity`(范围/等值选择率)、`groupDistinct`/`firstColumnDistinct`(distinct)。

## 阶段一

### ① 统计迁入 catalog.json(完整直方图,方案 B)

#### Histogram 改为 JSON 可序列化

`Histogram` 的 `Bucket(Comparable<?> lower, upper, long)` 和 `McValue(Comparable<?> value, long)` 用 `Comparable<?>` 存值,Jackson 无法反序列化接口类型。改为:

- `Bucket(String lower, String upper, long rowCount)`、`McValue(String value, long frequency)`——值存规范字符串(数值列存十进制表示,`toString` 生成)。
- `Histogram` 新增 `ColumnType type` 字段,反序列化后据此恢复比较语义。
- `Histogram`/`Bucket`/`McValue` 从 `class implements Serializable` 改为 **record**(Jackson 原生支持 record 组件)。
- 移除 `implements Serializable` 与 `serialVersionUID`。

比较逻辑适配:`Histogram.compareValue`/`normalize`/`numericDelta`/`typesCompatible` 现按 `Comparable<?>` 归一化数值。改为按 `type` 把 `String` 直方图值解析回比较形式:

- `type ∈ {INTEGER, BIGINT, DOUBLE, DATE, TIMESTAMP}` → `Double.parseDouble`(DATE 存天数、TIMESTAMP 存毫秒,本身是 int/long,归一化为 double)。
- `type == VARCHAR` → 保持 String。
- `type == BOOLEAN` → `Boolean.parseBoolean`。

字面量侧(RexLiteral)仍走现有归一化(BigDecimal/Integer/Long → Double、String/Boolean 原样)。二者归一化后在同一域比较。

`HistogramBuilder.build` 改产 `String` 值;`read(ValueVector, i, type)` 读出的 Integer/Long/Double/Boolean 用 `toString`。

#### TableStats 增加 rowCount

`TableStats(Map<String, Histogram> columnHistograms, long rowCount, boolean stale)` 新增 `rowCount`(表总行数,取 `arrowTable.rowCount()`)。供 `getStatistic()` 用;`Histogram.totalRows` 目前是非空行数,不直接等于表行数。

#### Stats 归属与持久化

统计归 `MiniDbCatalog` 统一持有(单一事实来源),`StatsManager` 退化为薄分析器:

- `MiniDbCatalog` 新增 `ConcurrentHashMap<String, TableStats> stats`(key 为 `schema.table` 小写)+ `getStats`/`setStats`/`markStatsStale`/`dropStats`。`setStats`(analyze)与 `markStatsStale`(DML)都触发 `notifyChange` → persistCatalog(让 stale 标记持久化,重启后仍是 stale 而非误判新鲜;代价是每次 DML 写一次 catalog.json,玩具库可接受);`dropStats` 不触发(紧随其后的 `dropTable`/`dropSchema` 已触发 notifyChange)。
- `CatalogSnapshot` 新增 `Map<String, TableStats> stats` 字段(key 为 `schema.table`);`snapshot()`/`restore()` 含统计;`restore()` 仍不触发 notifyChange(坑 49)。
- `StatsManager`:
  - `analyze(table)` 从 `storage` 读 ArrowTable、`HistogramBuilder` 建列直方图,组装 `TableStats`,调 `catalog.setStats(...)`(触发 listener → persistCatalog)。
  - `tableStats`/`markStale`/`dropStats` 委托 `catalog`。
  - 删除 `persist`/`read`/`loadAll` 的 `.stats` 文件 I/O;`close()` 仍 no-op。
- 旧的 `.stats` 文件不再读写;数据目录里残留的旧 `.stats` 文件忽略(不做迁移)。

> 包依赖:`CatalogSnapshot`(catalog 包)引用 `TableStats`(stats 包)会形成 `catalog↔stats` 包循环(现已有 `stats→catalog`)。接受 Java 包级循环,或后续把 `TableStats`/`Histogram` 移到 `stats` 之外。本次不做大挪动。

### ② 接入统计到 Calcite 成本模型

Calcite 的 `Statistic` 接口**只承载 `rowCount`(和 keys/collations)**,列级 distinct/null/直方图走另一套扩展点:`RelOptTable.unwrap(BuiltInMetadata.*.Handler.class)`。所以 `MiniDbCalciteTable` 要同时接三条,直方图信息才能全部用起来:

1. **rowCount** → 覆写 `getStatistic()` 返回 `Statistics.of(rowCount, List.of())`(无统计或 stale → `Statistics.UNKNOWN`)。驱动 `RelMdRowCount`(表/join/filter/aggregate 的行数)。
2. **distinctCount** → `MiniDbCalciteTable implements BuiltInMetadata.DistinctRowCount.Handler`,实现 `getDistinctRowCount(RelNode, mq, groupKey, predicate)`:`groupKey` 是单列时返回该列 `Histogram.distinctCount()`,否则 null。驱动 join 基数(`≈ left × right / max(distinct(leftKey), distinct(rightKey))`)、聚合/group-by 去重估算——**这是 join 重排序最关键的信息**。
3. **buckets + mcv + nullCount** → `MiniDbCalciteTable implements BuiltInMetadata.Selectivity.Handler`,实现 `getSelectivity(RelNode, mq, predicate)`:定位 `predicate` 里第一个 `RexInputRef` 对应列,调 `Histogram.selectivity(predicate, totalRows)`(把 `ExplainExecutor.filterSelectivity`/`histogramForCondition` 抽成公共方法复用)。驱动 filter 基数(`rows × selectivity`)。

`MiniDbCalciteTable` 构造改为 `(TableSchema schema, MiniDbCatalog catalog)`,`getStatistic()` 和两个 Handler 都惰性读 `catalog.getStats(schema.schemaName(), schema.name())`;`AbstractTable` 默认 `unwrap` 已按 `isInstance` 返回 this,无需额外实现。`information_schema` 系统表无统计,各接口返回 null/UNKNOWN。

效果:`RelMetadataQuery.getRowCount` / `getDistinctRowCount` / `getSelectivity` 对表扫描都拿到真实值,`RelMdRowCount`/`RelMdSelectivity`/`RelMdDistinctRowCount` 整条链打通,为阶段二 CBO 提供行数、基数、选择率。

### 阶段一验证

- 单测:`analyze` 后 `getStatistic().getRowCount()` 返回表行数;stale 表返回 UNKNOWN;重启后统计从 catalog.json 恢复(相等)。
- EXPLAIN 回归:等值/去重估算不变;范围选择率在重启后仍可用(方案 B 保留完整直方图)。
- 全量 `mvnw test`。

## 阶段二

### ③a Join 重排序

- 在 `LogicalOptimizer.optimize`(HepPlanner 逻辑阶段)加入 `LoptOptimizeJoinRule`(Calcite 自带,`org.apache.calcite.rel.rules.LoptOptimizeJoinRule`)。
- 它依赖 `RelMetadataQuery.getRowCount`/`getCost`(阶段一已供行数),把多表 join 按估算代价重排连接顺序。
- 需确认:`LoptOptimizeJoinRule` 是 multi-rel 规则,适合 HepPlanner 固定点运行;插入位置在去相关 + 现有 HEP 规则之后。
- 验证:3 表 join,人工制造「先 join 大表」的坏顺序,确认重排为「先小表」;结果集正确性不变(join 可交换/结合)。

### ③b 代价选 join 算法

把单条 `MiniDbJoinRule` 拆成三条 ConverterRule,让 VolcanoPlanner 按代价选:

- `MiniDbNestedLoopJoinRule`:匹配任意 `LogicalJoin`,产出 `MiniDbNestedLoopJoin`。
- `MiniDbHashJoinRule`:匹配等值 join(`JoinInfo.isEqui() && !leftKeys.isEmpty()`),产出 `MiniDbHashJoin`。
- `MiniDbSortMergeJoinRule`:匹配等值 join,产出 `MiniDbSortMergeJoin`。

每条规则 `computeSelfCost` 用行数建模(经 `RelMetadataQuery.getRowCount`):

- HashJoin:`leftRows + rightRows`(建表 + 探测)。
- SortMergeJoin:`leftRows + rightRows + (未有序侧 × 排序系数)`——保留现有「输入已有序则跳过内部排序」优化(`leftSorted`/`rightSorted` 在构造时经逻辑侧 `coversKeys` 判)。
- NestedLoopJoin:`leftRows × rightRows`(笛卡尔,代价最高)。

VolcanoPlanner 会在三条规则产出的物理备选中选最便宜者。删除 `MiniDbJoinRule`。

> 关键点:`MiniDbSortMergeJoin` 构造仍要从逻辑输入侧 `RelMetadataQuery.collations()` 判有序(转换后子节点是 RelSubset,报空 collation,坑 37/39)。规则内 `convert(join.getLeft())` 前拿逻辑侧 collation,与现 `MiniDbJoinRule` 一致。

### 阶段二验证

- 单测:大表×小表、等值 vs 非等值,断言选出期望算法(通过 EXPLAIN 或直接断言计划类)。
- 正确性回归:join 策略测试(`JoinStrategyTest`/`CollationJoinTest`)仍绿。
- 全量 `mvnw test`。

## 测试总览

- `StatsPersistenceTest`:analyze → catalog.json 含统计;重启 loadAll 恢复;stale 标记;dropTable/dropSchema 删统计。
- `StatisticWiringTest`(或扩展 PlannerTest):`getStatistic().getRowCount()` 正确。
- `JoinReorderTest`:join 重排序生效且结果正确。
- `JoinCostTest`:代价选算法生效。
- 既有 EXPLAIN/join 测试回归。

## 风险与开放问题

1. **Histogram 比较逻辑改造**是本方案最大不确定点:`String` 直方图值 + `ColumnType` 归一化要覆盖等值/范围/AND/OR 全部现有路径,且 `ExplainExecutor` 的选择率结果不应退化。阶段一需重点回归 EXPLAIN 的选择率。
2. `LoptOptimizeJoinRule` 与 MiniDB 现有逻辑规则的交互(是否与 `FilterJoinRule` 等冲突)需实测。
3. 代价模型是粗粒度的(仅行数),不追求精确,只求「选对明显更优的算法/顺序」。
4. 包循环 `catalog↔stats` 暂接受;若后续想清理,可把 `TableStats`/`Histogram` 移到 `catalog` 或新建中性包。
