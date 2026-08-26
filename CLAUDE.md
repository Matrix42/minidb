# CLAUDE.md — MiniDB 项目指南

本文件供 Claude Code 在后续会话中读取,以快速理解项目、节省 token。内容基于 2026-08-26 的代码状态;若代码与本文冲突,以代码为准。

## 项目概览

MiniDB 是一个基于 Apache Calcite(解析/规划)+ Apache Arrow(列式存储)+ Netty(网络)的自研微型 JDBC 数据库。四模块:

- `minidb-protocol` — Netty wire 协议(Message 编解码),**极简且稳定,改动需极谨慎**。
- `minidb-server` — 服务端:Calcite 解析/规划、Arrow 存储、向量化批式执行、JDBC 协议处理。
- `minidb-jdbc` — 客户端 JDBC 驱动(`jdbc:minidb://host:port`),基于自定义 Netty 协议。
- `minidb-dist` — 发行组装模块(pom packaging 无源码),产出 bin/conf/data/jdbc/tools/libs/ 发行目录与 tar.gz/zip,支持 start/stop/status 守护管理。

**定位**:功能完整的单机数据库。无事务(autoCommit 恒 true)。

## 构建与运行

- **JDK 17 必须**。`JAVA_HOME` 指向 JDK 17(本机为 `C:\Users\Matrix42\.jdks\azul-17.0.15` 或 `C:\Program Files\Java\jdk-17`)。
- **构建命令**(bash 下直接跑 `./mvnw.cmd`,不是 `mvnw.cmd` 也不是 `cmd //c`):
  - 全量测试:`./mvnw.cmd test`
  - 单模块测试:`./mvnw.cmd test -pl minidb-server`
  - 单测试类:`./mvnw.cmd test -pl minidb-server -Dtest=QueryExecutorTest`
  - 编译:`./mvnw.cmd -pl minidb-server -am compile -q`
- **启动服务端**:`./mvnw.cmd -pl minidb-server exec:java`(经 `MiniDbServer.main`,参数 `--port`/`--data`/`--conf`),默认监听 8899,数据目录 `./data`,配置目录 `./conf`。需加 JVM 参数:
  ```
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  ```
- **JDBC 客户端**需加:`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`。
- **发行包**:`./mvnw.cmd -pl minidb-dist -am package` 产出 `minidb-dist/target/minidb-1.0.0/`(+tar.gz/zip),`bin/minidb-server start/stop/status` 守护管理。

## 项目规范(重要)

1. **改完代码就提交**。用 conventional commit 风格(`feat:`/`fix:`/`test:`/`refactor:`/`docs:`),不要 amend,不要 `--no-verify`。
2. **在 `master` 分支工作**。功能分支命名 `<feature-name>`,完成后 fast-forward 合并回 `master` 并删除功能分支。本仓库无远程(纯本地),无 PR 流程。
3. **小步提交**:一个逻辑改动一个 commit,便于回溯。
4. **代码是给人读的,不只是给 AI 读**。命名必须自解释:不用无意义的短缩写(`lk`/`rn`/`i2`/`j2`),用描述性名字(`leftKeyCols`/`leftHasNullKey`/`leftScanOrder`)。注释解释 WHY 而非复述 WHAT;非显然的逻辑必须加注释说明原因。重构时顺带清理死代码与冗余计算。
5. **测试用 JUnit 5 + `@TempDir` + `RootAllocator`**。断言关系/比例而非精确浮点值(选择率单元测试除外,用 1e-9 delta)。
6. **现有物理算子文件和 `minidb-protocol` 模块尽量不改**——它们是稳定核心,扩展应通过新模块(参考 EXPLAIN 用 `ExplainExecutor` + `Instrumenter` 外挂的方式)。规则类在 `rule/physical` 包(逻辑优化规则在 `rule/logical`)。
7. **用中文回复用户**(代码/标识符/路径保持原文)。
8. **不要偷懒,优先做对而不是做快**。当「干净、可扩展」的改法可行时,不要用局部特判/硬编码糊过去。判据:如果「把这个特判复制第二遍」会让你想把它下沉成框架能力,那第一次就不该特判。
9. **类名尽量不用全限定名(FQN)**,用 import + 简单名——除非不得已(如两个同名类需要同时引用)。注释里提及类名不算(那是文档,不需要 import)。

## 架构与关键类

### SQL 执行流水线
```
QueryExecutor.execute(sql, currentSchema)
  → 顶部前缀识别(EXPLAIN / EXPLAIN ANALYZE / ANALYZE / USE SCHEMA,在 calcite.parse 之前)
  → schema DDL(SqlCreateSchema / SqlDropSchema,calcite.parse 之后)
  → CalciteContext.parse(sql) → SqlNode
  → Planner.plan(sql, currentSchema) → RelNode(两阶段:LogicalOptimizer 逻辑优化 → VolcanoPlanner + MiniDbPhysicalRules 物理转换)
  → MiniDbRel.execute(ExecContext[currentSchema]) → BatchIterator(拉模式,批式向量化)
  → QueryResult.Rows / Update / UseSchema
  → SessionHandler[per-channel currentSchema] → Message.ArrowBatch / UpdateCount → wire → 客户端
```

### 关键类(`minidb-server/src/main/java/com/minidb/server/`)

**执行核心:**
- `exec/QueryExecutor` — SQL 入口,前缀分发 + schema DDL + 限定名 DDL + 规划执行。4 参构造:`(catalog, storage, allocator, stats)`。`execute(sql, currentSchema)` 主路径,currentSchema 作参数流经 Planner/CalciteContext(不落共享成员字段,避免跨连接污染)。
- `exec/ExecContext` — 执行上下文(`storage` + `allocator` + `RexInterpreter` + `currentSchema`),每查询新建。`getTable(schema,table)`/`markDirty(schema,table)` schema 感知;裸名委托 `currentSchema` 解析。**瞬态表注册表**(`Map<String,List<Object[]>>`,per-query):供递归 CTE 的 `MiniDbRepeatUnion` 写、`MiniDbScan` 读 working 行。
- `exec/BatchIterator` — 拉模式批迭代器接口(`hasNext`/`next`/`close`)。
- `exec/QueryResult` — sealed 接口:`Rows(VectorSchemaRoot)` / `Update(long)` / `UseSchema(String)`。
- `exec/RexInterpreter` — RexNode 表达式求值**薄壳**:RexInputRef/RexLiteral 求值 + AND/OR/NOT/CAST/CASE/TRIM 专用 handler;其余函数经 `functions.lookup(call.getOperator())` 走 `exec/functions` 框架(见坑 25)。双参构造 `(allocator, FunctionRegistry)`,单参默认 `BuiltInFunctions.newRegistry()`。
- `exec/RowCopier` — 跨 VectorSchemaRoot 按行/按值拷贝。

**规划与算子(`plan/` + `rule/`):**
- `plan/Planner` — 编排两阶段规划:先 `LogicalOptimizer.optimize(logical)`(HepPlanner 逻辑优化)→ 再 VolcanoPlanner 物理转换。`plan(sql, currentSchema)` 透传 currentSchema。物理阶段注册 `ConventionTraitDef` + `RelCollationTraitDef`(先于 `RelOptCluster.create`)+ `MiniDbPhysicalRules.ALL`。
- `plan/logical/LogicalOptimizer` — HepPlanner 直接优化 Calcite Logical* 树,跑 `rule/logical` 规则集。
- `rule/logical/MiniDbLogicalRules` — 分两组:`HEP`(FilterJoinRule/FilterProjectTranspose/ProjectMerge/FilterMerge/聚合下推合并消除/排序换位化简/UnionEliminator 等,HepPlanner 阶段);`SORT`(SortRemoveRule,VolcanoPlanner 阶段,依赖 `RelCollationTraitDef`)。
- `plan/physical/`——物理算子,均 `implements MiniDbRel`:
  - `MiniDbScan` — 表扫描(含瞬态表路径 + 索引扫描,见索引节)。`MiniDbFilter` — 过滤,自分配输出 batch。`MiniDbProject` — 投影,窗口函数(RexOver)走 eager 路径。`MiniDbSort` — eager 物化+排序,offset/fetch 处理。`MiniDbValues` — VALUES 字面量。`MiniDbModify` — INSERT/UPDATE/DELETE,写路径调 `storage.markDirty`。`MiniDbAggregate` — eager 流式分组聚合,`LinkedHashMap` 保序;`SELECT DISTINCT` 天然支持。`MiniDbUnion` — UNION/UNION ALL,eager merge 后 close。`MiniDbSetOp` — INTERSECT/EXCEPT,行级 key 计数。`MiniDbCalc` — 防御性 Calc(Project+Filter 泛化),流式 + eager 窗口路径。`MiniDbJoin` — 抽象基类,INNER/LEFT/RIGHT/FULL 支持;三个子类:`MiniDbHashJoin`(等值,HASH 左建表右探测)、`MiniDbSortMergeJoin`(等值+双侧有序)、`MiniDbNestedLoopJoin`(任意条件)。`MiniDbRepeatUnion` — 递归 CTE,eager 迭代不动点,瞬态表传递。`MiniDbTableSpool` — 纯透传,供 `LogicalTableSpool` 转 MINIDB 约定。`RowVectors` — 行↔Arrow 转换工具。`WindowFunctions` — 窗口函数静态工具(聚合/排名/偏移/首末值),在 MiniDbProject 内嵌执行。
- `rule/physical/`——每个规则一个类,均 `extends ConverterRule`,构造器链:`Config.INSTANCE.withConversion(...).withRuleFactory(XxxRule::new)`。join 由三条规则按代价选:非等值只有 NestedLoopJoinRule;等值三条都产出备选,靠 `computeSelfCost`(见坑 29)排名。

**索引(`storage/IndexManager` + `plan/physical/MiniDbScan`):**
- `IndexManager` — 二级索引管理:索引表 = LSMTable(schema=(索引列..., PK列...)),存于 `data/<schema>/<table>/.indexes/<name>/`。`createIndex`/`dropIndex`/`populateFromTable`(全量灌入)/`onInsert`/`onDelete`/`onUpdate`(DML 维护)/`rebuildFromDisk`(启动恢复)/`renameTable`/`clearIndexes`(TRUNCATE)/`dropIndexesForTable`(DROP TABLE)。`indexSchema` 静态方法合成索引表 schema。
- `MiniDbScan` — `usedIndex` 字段:构造期 `selectIndex` 从 pushedFilter 选覆盖最多列的索引;`indexLookup` 前缀扫描索引表→收集主键→回表 getByKey→residual 过滤。`explainTerms` 输出 `index=<name>`。`literalValue` 将 Calcite DECIMAL 字面量转 Integer(匹配 IntVector 返回类型)。
- `ConstraintChecker` — UNIQUE 索引校验:INSERT 时 null 跳过、批内自体去重、前缀扫描索引表。`IndexManager.populateFromTable` 存量重复校验(seen 集)。
- `MiniDbModify` — `validateUpdateUnique`:UPDATE 新索引键前缀扫描,排除被更新行旧 PK 后仍冲突则拒绝。
- `MiniDbCalciteTable` — `tableSchema()` 从 catalog 取最新元数据(DDL 后缓存过期);`keys()` 含 UNIQUE 索引列供 CBO 基数估计。
- `AlterTableHandler` — `checkColumnNotConstrained`:DROP COLUMN 拒绝索引列;RENAME COLUMN 同步更新索引 def 列名。
- `MiniDbCatalog.renameTable` — 重建 TableSchema 保留 `tableType` + `indexes`。
- 索引列类型限制:SMALLINT/INTEGER/BIGINT/VARCHAR(L1);VARCHAR 索引暂不启用(LSM MemTable KEY_COMPARATOR 要求 Comparable,Arrow Text 不兼容)。
- 索引一致性:先写数据后写索引(索引失败时数据已落盘,下次 DDL 重建可恢复)。

**存储(`storage/`):**
- `storage/StorageManager` — 表目录 + Arrow IPC 持久化。schema 感知:存储 key 为 `schema.table`,文件路径 `data/<schema>/<table>.arrow`(子目录)。`loadAll` 两级目录遍历,从 Arrow schema metadata 恢复 schemaName。`markDirty` 触发 `StatsManager.markStale`。
- `storage/ArrowTable` — 单表:`CopyOnWriteArrayList<VectorSchemaRoot>` batches,`rowCount()`(O(批数)),`appendBatch`(MAX_BATCH_ROWS=4096)。
- `storage/CatalogStore`(接口)+ `storage/JsonCatalogStore`(实现)——元数据独立持久化,写 `data/catalog.json`(见坑 28)。`catalog.addListener(this::persistCatalog)` 在 DDL 变更后同步写盘。

**目录(`catalog/`):**
- `catalog/MiniDbCatalog` — 线程安全 schema/table 元数据:`ConcurrentHashMap<schema, ConcurrentHashMap<table, TableSchema>>`。构造自动建 `"public"` 并注册 `information_schema`。`snapshot()`/`restore(CatalogSnapshot)` 供持久化(restore 不通知,snapshot 排除系统 schema)。
- `catalog/InformationSchemaCatalog` — `information_schema` 系统 schema 元数据(3 表:`schemata`/`tables`/`columns`)。供 `MiniDbScan`/`QueryExecutor` 判系统 schema、`exec/InformationSchema` 物化。
- `catalog/TableSchema` — `record(schemaName, name, List<ColumnMeta>, primaryKey, uniqueKeys, foreignKeys, indexes, storageFormat, tableType)`。`columnIndex` 大小写不敏感。
- `catalog/ColumnMeta` — `record(name, ColumnType, nullable)`。
- `catalog/ColumnType` — 枚举:INTEGER/BIGINT/DOUBLE/VARCHAR/BOOLEAN/DATE/TIMESTAMP。
- `catalog/ArrowTypes` — SqlTypeName↔ColumnType↔ArrowType 互转。

**Calcite 集成(`calcite/`):**
- `calcite/CalciteContext` — parser 配置(MYSQL lex,大小写不敏感,DDL parser factory,`SqlLibraryOperatorTable` 含 STANDARD+MYSQL+POSTGRESQL)。`parse(sql)` → SqlNode;`planInCluster(sql, cluster, currentSchema)` → RelRoot。catalog reader 搜索路径 `["minidb"]`;列名大小写不敏感靠 `CalciteCatalogReader` 的连接属性 `caseSensitive=false`(见坑 23)。
- `calcite/MiniDbRootCalciteSchema` — 容器 schema,接 `(catalog, currentSchema)`,暴露当前 schema 的表 + 所有子 schema(含 `information_schema`)。
- `calcite/MiniDbCalciteSchema` — 单 schema 实例。`calcite/MiniDbCalciteTable` — 把 catalog 表暴露给 Calcite,提供 `getStatistic()`(唯一键供 CBO)。

**统计(`stats/`,EXPLAIN 附属子系统):**
- `stats/Histogram` — 不可变单列等频直方图(10 桶 + top-10 MCV + distinct/null/total)。`selectivity(RexNode, long)` 选择率模型,Serializable。`stats/HistogramBuilder` — 从 `List<ValueVector>` 建直方图。`stats/StatsManager` — 管理所有表的 `TableStats`,`.stats` 文件 Java 序列化持久化。`stats/TableStats` — `record(Map<String,Histogram> columnHistograms, boolean stale)`。

**EXPLAIN(`exec/`):**
- `exec/ExplainExecutor` — `explain(sql, currentSchema)`(估算)/`analyze(sql, currentSchema)`(插桩执行)。7 列结果集,折叠 trivial Project。`exec/Instrumenter` — 用 `copy()` 构造插桩影子树,`InstrumentedRel` 包裹每个节点,`start` 必须在 `wrapped.execute()` 之前。`exec/InformationSchema` — 从内存 catalog 物化 `information_schema` 系统表行。

**网络 + 协议 + JDBC 客户端:**
- `MiniDbServer` — 启动:`storage.loadAll()` → `StatsManager` → `QueryExecutor` → Netty `ServerBootstrap`。
- `netty/SessionHandler` — 持有 per-channel `currentSchema`(默认 public)。`handleExecute` 调 `executor.execute(sql, currentSchema)`,`UseSchema` 更新自身 currentSchema。`MetadataExecutor`(外挂)服务 `getSchemas`/`getTables`/`getColumns`。
- `minidb-protocol` — `Message` sealed records(Handshake/ExecuteRequest/ArrowBatch/UpdateCount/元数据请求等),`MessageEncoder`/`MessageDecoder` Netty 编解码。
- `minidb-jdbc/` — `MiniDbClient`(Netty 客户端,按 requestId 路由响应)/`MiniDbConnection`/`MiniDbStatement`/`MiniDbResultSet`。客户端**不引用**服务端类(纯网络)。

## 踩过的坑(经验教训)

1. **`./mvnw.cmd` 在 bash 下直接跑**,不要用 `mvnw.cmd`(PATH 找不到)、`cmd //c mvnw.cmd`(输出编码乱)、`mvn`(没装)。
2. **`EXPLAIN`/`ANALYZE` 前缀拦截顺序**:`EXPLAIN ANALYZE ` 必须先于 `EXPLAIN `,`ANALYZE`(exact)先于 `ANALYZE <table>`,在 `calcite.parse` 之前。
3. **EXPLAIN trivial Project 折叠**:必须 `isTrivialProject`(全 RexInputRef 且索引连续才折叠),窗口 Project 非 identity 不能折叠。
4. **`MiniDbSort.execute()` 是 eager**:物化+排序在 `execute()` 内完成,插桩 `start` 必须在 `wrapped.execute()` 之前,否则 Sort 耗时漏掉。
5. **Scan batch 所有权**:batch 归迭代器所有,不归表所有。`it.close()` 级联释放,不能 `b.close()` 发出的 batch(会 double-close)。**LSM MergeScanIterator 已修坑**:`next()` 原在返回新批前 close 上一批,但上一批已返回调用方,use-after-close 致内存泄漏(每条 join 残留 16MB);修为累积到 emitted list、close 统一释放。
6. **Histogram 三个坑**:① 单列模型——复合跨列条件对非匹配列走 `DEFAULT_SELECTIVITY` 兜底;② 泛型擦除——`(Comparable<Object>)(Comparable)x` raw 中转;③ 插值——`Double.compareTo` 只返回 -1/0/1,必须用 `numericDelta` 算真实差值。
7. **`currentSchema` 不能放共享成员字段**:`QueryExecutor` 是单例,`currentSchema` 作参数流经 `execute(sql, currentSchema)` → `Planner.plan` → `CalciteContext.planInCluster`。`SessionHandler` 持有 per-channel `currentSchema`。
8. **`SqlIdentifier.getSimple()` 对复合名静默返回首段**:限定名解析必须用 `node.name.names`(ImmutableList),schema=`names.get(0)`,table=`names.get(names.size()-1)`。
9. **Calcite schema 树"提升表"副作用**:`MiniDbRootCalciteSchema.getTableMap()` 把当前 schema 的表暴露在 `minidb` 容器层,导致 `RelOptTable.getQualifiedName()` 对 unqualified 表返回 2 段 `["minidb","t"]`(丢失 schema),对 `other.t` 返回 3 段。`MiniDbScan`/`MiniDbModify` 据此分流:`size>=3` 用倒数第二段作 schema,`size==2` 调 `ctx.getTable(裸名)` 由 `ExecContext.currentSchema` 解析。
10. **标识符引号用双引号**:驱动 `getIdentifierQuoteString()` 返回 `"`,Calcite parser 必须 `withQuoting(Quoting.DOUBLE_QUOTE)`(Lex.MYSQL 默认只认反引号)。代价:反引号不再支持。
11. **`AggregateCall` 参数存储**:纯列引用参数(如 `SUM(id)`)时 `rexList` 为空、索引存 `argList`;表达式参数才填 `rexList`。取参数必须两个都查,只查 `rexList` 会静默丢掉参数(SUM 恒 NULL)。
12. **UNION 执行顺序**:先收集所有输入 batches,**merge 完成后**才 `close()` 输入迭代器——先 close 会导致向量 valueCount 归零。`VectorSchemaRoot.of(vectors)` 的 rowCount 取第一个 vector 的 valueCount,必须先 `setValueCount` 再 `of()`。
13. **JOIN 关键点**:NULL 键等值永不匹配,三种算法都要显式处理。join 输出顺序随算法不同,测试断言前必须排序。`RexInterpreter` 需 stringDomain 分支支持字符串列比较。
14. **窗口函数两个易漏坑(乱序输入才暴露)**:① comparator 的 ORDER BY 键必须是**主排序键**,行索引只作 `thenComparingInt` tiebreak——写成 `comparingInt(i->i).thenComparing(键)` 会让行索引成主键;② `computeOver` 结果要按**原始行索引**落位,不能用 `ArrayList.add(行索引, 值)` 把行索引当插入位置。窗口在 MiniDbProject 内嵌执行(Calcite 1.42 无 LogicalWindow)。
15. **递归 CTE**:Calcite 把非递归 CTE 直接内联;递归 CTE 生成 `LogicalRepeatUnion` + 两个 `LogicalTableSpool`。瞬态表用 `ExecContext` 的 `Map<String,List<Object[]>>` 传递,`MiniDbScan` 靠 qualified name 段数==1 识别瞬态表。
16. **`RelMetadataQuery.collations()` 替代 `getCollation`**:1.42 只有 `collations(RelNode)`(返回 `List<RelCollation>`)。`RelCollationTraitDef.INSTANCE` 必须在 `RelOptCluster.create` 之前注册,否则 traitSet 缺 collation 分量抛 AssertionError。
17. **派生表 ORDER BY 无 LIMIT 可能被优化掉**:子查询 `(SELECT * FROM t ORDER BY a)` 不带 LIMIT 时 ORDER BY 无语义,Calcite 丢弃 Sort → join 选不了 SortMergeJoin。要保输入有序必须带 `LIMIT n`。
18. **`ProjectRemoveRule`/`CalcRemoveRule` 会丢列别名,不要注册**:它们按「索引恒等」判 trivial,不看字段名。`SELECT a.id AS aid` 别名被当 no-op 删除。
19. **排序移除规则的分裂放置**:`SortRemoveConstantKeysRule` 只在**无** `RelCollationTraitDef` 的 HepPlanner 阶段触发,放 `HEP`;`SortRemoveRule` 只在**有** trait 的 VolcanoPlanner 阶段触发,放 `SORT`。放反了规则静默失效。
20. **`FilterProjectTransposeRule` + `ProjectFilterTransposeRule` 互为逆操作,同时注册会振荡 → StackOverflowError**:只留 `FilterProjectTransposeRule`(filter 下推方向)。
21. **新增物理算子时要同步补优化规则**:检查 CoreRules 里对应的换位/下推/合并规则,否则该算子周围永远少一层优化。加规则前要确认物理算子能承接规则产出的形状。
22. **Calcite 字符串字面量默认字符集是 ISO-8859-1**:含中文的字面量在 `SqlToRelConverter` 抛 `Failed to encode`。修法:子类 `Utf8SqlTypeFactory extends SqlTypeFactoryImpl` 覆写 `getDefaultCharset()` 返回 UTF-8。别用 `-Dcalcite.default.charset` 系统属性(静态 final 初始化时机不可控)。
23. **列名大小写敏感由 `CalciteCatalogReader` 的连接配置控制,不在 `SqlValidator.Config`**:在 `CalciteContext.buildCatalogReader` 构造 `CalciteConnectionConfigImpl` 前 `props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false")`。
24. **列式标量函数框架(`exec/functions`)三个坑**:① **register-overwrite**——`register` 用 `Map.put`,一个 `SqlOperator` 只对应一个 `Function`,所有重载必须收进那一个 `Function` 的 `overloads` 列表;② **真混型 INTEGER/BIGINT 需跨型重载**——Calcite 对比较跨 family 插 CAST 但同 family 不 CAST,算术不插 CAST,必须为跨 family 注册重载(整型×DOUBLE→DOUBLE 等);③ **`ReduceExpressionsRule` 常量折叠绕过 kernel**——计划期用 Janino 编译 `SqlFunctions` 折叠常量,若内核语义与 `SqlFunctions` 不一致,常量与列求值会分歧。新增函数时要么对齐 Calcite 语义,要么确认该函数无折叠实现。
25. **元数据独立持久化(catalog.json)与空表存活**:`TableSchema` 持久化到 `data/catalog.json`,独立于 `.arrow` 数据文件,解决空表重启即丢。`MiniDbCatalog.restore()` 必须**不触发** `notifyChange`;`StorageManager.loadAll()` 先 `restoreCatalog()` 再遍历 `.arrow`,restored=true 时不重复 `catalog.createTable`。
26. **JDBC 元数据 LIKE 的转义字符 `\` 必须处理**:驱动 `getSearchStringEscape()` 返回 `\`,DataGrip 等工具把 `_` 转义成 `\_`。`compileLike` 遍历时遇 `\` 取下一个字符按字面量输出,其余字符 regex 转义。
27. **`VolcanoCost.isLt` 只比较 rowCount 分量,cpu/io 被忽略**(Calcite 多年未实现的 TODO):代价选 join 算法时把工作量编码进 rowCount 分量 `makeCost(work, 0, 0)`(NestedLoop=`left×right`、Hash=`left+right`、SortMerge=`left+right+sort`)。
28. **视图(VIEW)**:`ViewExpander` 放在 `Planner`,捕获本次规划的 `VolcanoPlanner` + `typeFactory`,展开视图时用**同一个 planner** `RelOptCluster.create` 重建 cluster,保证 traitSet 一致。`expandView` 取 `schemaPath` 最后一段作视图所在 schema,递归支持视图套视图。
29. **约束(主键/唯一/NOT NULL/外键)**:`TableSchema` 存 `primaryKey`/`uniqueKeys`/`foreignKeys`,`ColumnMeta.nullable`。列级约束 Calcite 不支持,`rewriteColumnConstraints` 在 parse 前用字符串扫描提升为表级。外键 DDL 同样 parse 前剥离;执行期 INSERT/UPDATE 校验外键值存在、DELETE 校验不被引用(RESTRICT)。
30. **`SELECT * ORDER BY <expr>` 的临时列泄露**:`Planner.plan` 必须用 `root.project()`(非 `root.rel`),`project()` 在 fields 非平凡时创建 `LogicalProject` 裁剪 ORDER BY 临时列。`ProjectRemoveRule` 只按「索引恒等」判 trivial,无法处理「列数减少」的裁剪。
31. **窗口 `AVG(DECIMAL)` 被 Calcite 重写为 `SUM/COUNT` 除法 + 外层 CAST 截断精度**:`VectorCasts.cast` DECIMAL 分支,当源 `DecimalVector.scale > 目标 scale` 时保留源 scale(精度优先)。Calcite 不为 AVG 提升 scale,需在物理层各路径补。
32. **`JoinReorderer` 贪心平手取首见,大表当选种子把非等值 NestedLoop 推到大表 join 结果上**:种子与每步选择在连接度平手时按「叶子行数最小」破平手。无统计时 `rowCount()` 必须 try-catch 回退 1e8(Calcite 1.42 的 `RelMdUtil.estimateFilteredRows` 对 null selectivity 直接 unboxing 抛 NPE)。

## 文档与计划

- `README.md` — 用户向说明(特性/类型/限制)。
- `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md` — 设计 spec(brainstorming 产出)。
- `docs/superpowers/plans/YYYY-MM-DD-<topic>.md` — 实现计划(writing-plans 产出)。
- `.claude/settings.local.json` — 本地权限白名单。
- `.superpowers/sdd/` — SDD 工作区(gitignored,临时 ledger/brief/report,合并后可删)。