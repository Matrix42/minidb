# CLAUDE.md — MiniDB 项目指南

本文件供 Claude Code 在后续会话中读取,以快速理解项目、节省 token。内容基于 2026-08-11 的代码状态;若代码与本文冲突,以代码为准。

## 项目概览

MiniDB 是一个基于 Apache Calcite(解析/规划)+ Apache Arrow(列式存储)+ Netty(网络)的自研微型 JDBC 数据库。三模块:

- `minidb-protocol` — Netty wire 协议(Message 编解码),**极简且稳定,改动需极谨慎**。
- `minidb-server` — 服务端:Calcite 解析/规划、Arrow 存储、向量化批式执行、JDBC 协议处理。
- `minidb-jdbc` — 客户端 JDBC 驱动(`jdbc:minidb://host:port`),基于自定义 Netty 协议。

**定位**:学习/玩具级数据库,内存型。无事务(autoCommit 恒 true)、无 UPDATE/DELETE 之外的复杂 DML、无 JOIN、无聚合。结果集客户端一次性物化,无服务端分页。

## 构建与运行

- **JDK 17 必须**。`JAVA_HOME` 指向 JDK 17(本机为 `C:\Users\Matrix42\.jdks\azul-17.0.15` 或 `C:\Program Files\Java\jdk-17`)。
- **构建命令**(bash 下直接跑 `./mvnw.cmd`,不是 `mvnw.cmd` 也不是 `cmd //c`):
  - 全量测试:`./mvnw.cmd test`
  - 单模块测试:`./mvnw.cmd test -pl minidb-server`
  - 单测试类:`./mvnw.cmd test -pl minidb-server -Dtest=QueryExecutorTest`
  - 编译:`./mvnw.cmd -pl minidb-server -am compile -q`
- **启动服务端**:`./mvnw.cmd -pl minidb-server exec:java`,默认监听 8899,数据目录 `./data`。需加 JVM 参数:
  ```
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  ```
- **JDBC 客户端**需加:`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`(Arrow 的 MemoryUtil 用了 Unsafe)。

## 项目规范(重要)

1. **改完代码就提交**。用 conventional commit 风格(`feat:`/`fix:`/`test:`/`refactor:`/`docs:`),不要 amend,不要 `--no-verify`。
2. **在 `master` 分支工作**。功能分支命名 `<feature-name>`(如 `explain-analyze`),完成后 fast-forward 合并回 `master` 并删除功能分支。本仓库无远程(纯本地),无 PR 流程。
3. **小步提交**:一个逻辑改动一个 commit,便于回溯。
4. **不要在文档/注释里写废话注释**(解释 WHAT 而非 WHY)。代码自解释优先。
5. **测试用 JUnit 5 + `@TempDir` + `RootAllocator`**。断言关系/比例而非精确浮点值(选择率单元测试除外,用 1e-9 delta)。
6. **现有 7 个物理算子文件(`MiniDbScan`/`Filter`/`Project`/`Sort`/`Values`/`Modify`/`Aggregate`)和 `minidb-protocol` 模块尽量不改**——它们是稳定核心,扩展应通过新模块(参考 EXPLAIN 用 `ExplainExecutor` + `Instrumenter` 外挂的方式,零侵入算子)。规则类在 `rule` 包(见下)。
7. **用中文回复用户**(代码/标识符/路径保持原文)。

## 架构与关键类

### SQL 执行流水线
```
QueryExecutor.execute(sql, currentSchema)
  → 顶部前缀识别(EXPLAIN / EXPLAIN ANALYZE / ANALYZE / USE SCHEMA,在 calcite.parse 之前)
  → schema DDL(SqlCreateSchema / SqlDropSchema,calcite.parse 之后)
  → CalciteContext.parse(sql) → SqlNode
  → Planner.plan(sql, currentSchema) → RelNode(VolcanoPlanner + MiniDbRules 生成物理算子树)
  → MiniDbRel.execute(ExecContext[currentSchema]) → BatchIterator(拉模式,批式向量化)
  → QueryResult.Rows / Update / UseSchema
  → SessionHandler[per-channel currentSchema] → Message.ArrowBatch / UpdateCount → wire → 客户端
```

### 关键类(`minidb-server/src/main/java/com/minidb/server/`)

**执行核心:**
- `exec/QueryExecutor` — SQL 入口,前缀分发(EXPLAIN / EXPLAIN ANALYZE / ANALYZE / USE SCHEMA,在 calcite.parse 之前)+ schema DDL(`SqlCreateSchema`/`SqlDropSchema`)+ 限定名 CREATE/DROP/TRUNCATE + 规划执行。4 参构造:`(catalog, storage, allocator, stats)`。`execute(sql)` 委托 `execute(sql, "public")`;`execute(sql, currentSchema)` 主路径,currentSchema 作参数流经 Planner/CalciteContext(不落共享成员字段,避免跨连接污染)。USE SCHEMA 返回 `QueryResult.UseSchema(name)`。
- `exec/ExecContext` — 执行上下文(`storage` + `allocator` + `RexInterpreter` + `currentSchema`),每查询新建。`getTable(schema,table)`/`markDirty(schema,table)` schema 感知;裸名 `getTable(table)`/`markDirty(table)` 用 `currentSchema` 解析(供算子的 size==2 路径)。
- `exec/BatchIterator` — 拉模式批迭代器接口(`hasNext`/`next`/`close`,`empty()` 工厂)。
- `exec/QueryResult` — sealed 接口:`Rows(VectorSchemaRoot)` / `Update(long)` / `UseSchema(String schemaName)`(USE SCHEMA 切换,SessionHandler 据此更新 per-channel currentSchema)。
- `exec/RexInterpreter` — RexNode 表达式求值(比较/算术/逻辑/CAST),供 Filter/Project 用。
- `exec/RowCopier` — 跨 VectorSchemaRoot 按行/按值拷贝。

**物理算子(`plan/`,均 `implements MiniDbRel`):**
- `plan/MiniDbRel` — 接口:`BatchIterator execute(ExecContext)`。每个算子自己 `execute()` 内部调子算子 `execute()`(拉模式)。
- `plan/MiniDbScan` — 表扫描,返回表 owned 的 batches(close 是 no-op,batches 归表所有)。
- `plan/MiniDbFilter` — 过滤,自己分配输出 batch,`owned` 队列跟踪待关闭。
- `plan/MiniDbProject` — 投影,RexInterpreter 求值后重命名列。
- `plan/MiniDbSort` — **eager**:`execute()` 内全量物化+排序,返回惰性迭代器。offset/fetch 字面量处理。
- `plan/MiniDbValues` — VALUES 字面量。
- `plan/MiniDbModify` — INSERT/UPDATE/DELETE,写路径调 `storage.markDirty(tableName)`(此处触发 stats stale 钩子)。
- `plan/MiniDbAggregate` — 聚合,`Aggregate` 子类。**eager**:`execute()` 拉取输入全量,流式分组聚合,输出单批。分组 key 为 `List<Object>` 规范化值(含 null),`LinkedHashMap` 保首见顺序;每 `AggregateCall` 一个 `Accumulator`(COUNT=long,SUM 按参数 long/double,AVG=sum+count,MIN/MAX=Comparable;`DISTINCT` 聚合用 `DistinctAcc` 维护 `LinkedHashSet` 去重后按类型聚合)。NULL 语义:聚合忽略 NULL;`COUNT(*)` 计所有行;空输入无 GROUP BY → 1 行(COUNT=0 其余 NULL);有 GROUP BY 空表 → 0 行。输出列类型按 `getRowType()`(Calcite 推导)。`SELECT DISTINCT` 由 Calcite 规划为无 aggCalls 的分组聚合,天然支持。
- `plan/MiniDbUnion` — UNION/UNION ALL,`Union` 子类。**eager**:先收集所有输入 batches 再 merge(必须 merge 后才 `close()` 输入迭代器,否则 Project/Filter owned batch 被释放)。`all=true` 级联;`all=false` 用 `LinkedHashSet<List<Object>>` 行级去重。输出单批,`VectorSchemaRoot.of()` 前必须 `setValueCount`(of 的 rowCount 取第一个 vector 的 valueCount)。
- `plan/MiniDbSetOp` — INTERSECT/EXCEPT,`SetOp` 子类,kind 字段区分(`SqlKind.INTERSECT`/`MINUS`,SetOp 自带 kind+all,无 INTERSECT_ALL 枚举)。**eager**:每输入统计行级 key 计数(LinkedHashMap 保序),INTERSECT 取跨输入 min 计数、EXCEPT 取 count0-Σrest,>0 保留;`all=false` 每 key 输出 1 行、`all=true` 输出 n 行。输出顺序 = 第一输入首见顺序。
- `plan/MiniDbCalc` — Calc(Project+Filter 泛化),`Calc` 子类,**防御性算子**:当前规划器配置下常规 SQL 不产生 LogicalCalc(全部 Project/Filter),未来若有 Calc 节点即可处理。**lazy 流式**:program.getProjectList() 逐个 expandLocalRef + getCondition(),eval 求值,按条件行过滤后 renameFiltered 拷入新向量。
- `plan/MiniDbJoin` — JOIN,`Join` 子类,`Strategy{AUTO,HASH,SORT_MERGE,NESTED_LOOP}`。**eager 物化**两侧为 List<Object[]>。HASH:左建表右探测(等值键);SORT_MERGE:两侧按键排序归并(null 排最后,显式不匹配);NESTED_LOOP:逐对行构造 1 行拼接 root eval 全条件。AUTO=纯等值→HASH,否则→NESTED_LOOP。INNER/LEFT/RIGHT/FULL 支持,SEMI/ANTI 抛错。输出单批,getRowType()=左+右。
- `plan/WindowFunctions` — 窗口函数静态工具:`materialize` + `computeOver(RexOver, rows)`。按 RexWindow 的 partitionKeys 分组、orderKeys 排序,逐行计算:聚合 SUM/AVG/COUNT/MIN/MAX over 帧、ROW_NUMBER/RANK/DENSE_RANK(peers)、LEAD/LAG(offset+default)、FIRST_VALUE/LAST_VALUE。帧=RexWindowBound 转行位置(RANGE/ROWS 同位置语义)。**窗口执行在 MiniDbProject 内嵌**:检测投影含 RexOver → eager 路径,`RexShuttle.visitOver` 提取为窗口列引用后 eval 改写表达式。Calcite 1.42 无 LogicalWindow(ProjectToWindowRule 不可靠),无窗口算子/规则。
- `plan/Planner` — VolcanoPlanner + `MiniDbRules.ALL`(ConverterRule 把 Logical* 转成 MiniDb*)。`plan(sql)` 委托 `plan(sql, "public")`,透传 currentSchema 给 `CalciteContext.planInCluster`。
- `plan/MiniDbRules` — 转换规则集聚合类,`rule` 包(见下)。
- `rule/` 包(`com.minidb.server.rule`,plan 同级)——每个规则一个类:`MiniDbScanRule`/`MiniDbFilterRule`/`MiniDbProjectRule`/`MiniDbSortRule`/`MiniDbValuesRule`/`MiniDbModifyRule`/`MiniDbAggregateRule`/`MiniDbUnionRule`/`MiniDbIntersectRule`/`MiniDbExceptRule`/`MiniDbCalcRule`/`MiniDbJoinRule`,均 `extends ConverterRule`,构造器链:`this(Config.INSTANCE.withConversion(...).withRuleFactory(XxxRule::new))` + 私有 `(Config)` 构造器 `super(config)`。`MiniDbRules.ALL` 聚合引用。

**存储(`storage/`):**
- `storage/StorageManager` — 表目录 + Arrow IPC 持久化。**schema 感知**:存储 key 为 `schema.table`(小写),文件路径 `data/<schema>/<table>.arrow`(子目录),`flushTable` 必须 `createDirectories(file.getParent())`。`loadAll` 两级目录遍历,从 Arrow schema metadata 恢复 schemaName。`getTable(schema,table)`/`dropTable(schema,table)`/`markDirty(schema,table)`/`truncateTable(schema,table)`/`dropSchema(name)`(级联删表+文件,catalog 抛 public/missing);裸名重载委托 `public`。持有 `volatile StatsManager` 引用,`markDirty` 触发 `markStale`(key 为 `schema.table`),`dropTable`/`dropSchema` 触发 `dropStats`。
- `storage/ArrowTable` — 单表:`CopyOnWriteArrayList<VectorSchemaRoot>` batches。构造 Arrow `Schema` 时附 `{"schema" → schemaName}` metadata(跨 IPC 流到客户端 `getSchemaName()`)。`rowCount()`(O(批数)非 O(行数))、`batches()`、`newBatchRoot()`、`appendBatch`(MAX_BATCH_ROWS=4096)、`replaceBatches`、`clear`。

**目录(`catalog/`):**
- `catalog/MiniDbCatalog` — 线程安全 schema/table 元数据:`ConcurrentHashMap<schema-lowercase, ConcurrentHashMap<table-lowercase, TableSchema>>`。构造自动建 `"public"`。`createSchema`/`dropSchema`(public 不可删,级联删表)/`schemaNames`/`tableNames(schema)`/`getTable(schema,table)`/`hasTable(schema,table)`/`dropTable(schema,table)`;裸名重载(`getTable(name)`/`hasTable(name)`/`tableNames()`/`dropTable(name)`)委托 `public`,向后兼容。
- `catalog/TableSchema` — `record(schemaName, name, List<ColumnMeta>)`;便捷构造 `TableSchema(name, cols)` 委托 `schemaName="public"`。`columnIndex` 大小写不敏感。
- `catalog/ColumnMeta` — `record(name, ColumnType)`。
- `catalog/ColumnType` — 枚举:INTEGER/BIGINT/DOUBLE/VARCHAR/BOOLEAN/DATE/TIMESTAMP。
- `catalog/ArrowTypes` — SqlTypeName↔ColumnType↔ArrowType 互转,`field(...)` 工厂。

**Calcite 集成(`calcite/`):**
- `calcite/CalciteContext` — parser 配置(MYSQL lex,大小写不敏感,DDL parser factory)。`parse(sql)` → SqlNode;`planInCluster(sql, cluster, currentSchema)` → RelRoot(旧 2 参重载委托 `public`)。catalog reader 搜索路径 `["minidb"]`;`MiniDbRootCalciteSchema` 挂在 `minidb` 下,其 `getTableMap()` 返回当前 schema 的表(支持 unqualified 名),`getSubSchemaMap()` 返回所有 schema(支持 `schema.table` 限定名)。
- `calcite/MiniDbRootCalciteSchema` — 容器 schema,接 `(catalog, currentSchema)`,既暴露当前 schema 的表也暴露所有子 schema。
- `calcite/MiniDbCalciteSchema` — 单 schema 实例,`getTableMap()` 返回该 schema 的表。
- `calcite/MiniDbCalciteTable` — 把 catalog 表暴露给 Calcite。

**统计(`stats/`,EXPLAIN 附属子系统):**
- `stats/Histogram` — 不可变单列等频直方图(10 桶 + top-10 MCV + distinct/null/total)。`selectivity(RexNode, long)` 选择率模型(等值/范围/AND/OR/NOT/默认0.33)。**Serializable**。跨列类型不兼容时 rangeSelectivity 走 `DEFAULT_SELECTIVITY` 兜底(不抛 ClassCastException)。
- `stats/HistogramBuilder` — 从 `List<ValueVector>` 建 Histogram(排序+等频分桶+MCV)。
- `stats/StatsManager` — 管理所有表的 `TableStats`,`analyze`/`analyzeAll`/`tableStats`/`markStale`/`dropStats`/`loadAll`。`.stats` 文件 Java 序列化持久化,`read()` 失败 LOG.warn。`AutoCloseable`(close no-op)。
- `stats/TableStats` — `record(Map<String,Histogram> columnHistograms, boolean stale)`,Serializable。map 键为小写列名。

**EXPLAIN(`exec/`):**
- `exec/ExplainExecutor` — `explain(sql, currentSchema)`(估算,不执行)/`analyze(sql, currentSchema)`(插桩执行),透传 currentSchema 给 `planner.plan`(旧单参重载委托 public)。7 列结果集:`id INT, parent_id INT nullable, operation VARCHAR, rows BIGINT nullable, batches INT nullable, elapsed_ms DOUBLE nullable, remarks VARCHAR`。plain 和 ANALYZE 都折叠 Calcite 插入的 trivial Project 节点,保证两者行集一致。内部 `storage.getTable(table)` 用裸名,非 public 表的统计降级(坑 18)。
- `exec/Instrumenter` — ANALYZE 用:用各算子 `copy()` 构造插桩影子树,`InstrumentedRel extends AbstractRelNode implements MiniDbRel` 包裹每个节点,`execute()` 返回的 measured 迭代器在 `next()` 累加 rows/batches、`close()` 记 elapsed。`start` 必须在 `wrapped.execute()` 之前设置(否则 Sort 的 eager 物化时间不计)。stats 按**原始**节点 keyed 到 `IdentityHashMap`。
- `exec/NodeStats` — 可变 `long rows; int batches; double elapsedMs;`。

**网络:**
- `MiniDbServer` — 启动:`storage.loadAll()` → `StatsManager` 构造 + `setStatsManager` + `loadAll` → `QueryExecutor` → Netty `ServerBootstrap`。
- `netty/SessionHandler` — 持有 per-channel `currentSchema` 字段(默认 public)。`handleExecute`:调 `executor.execute(sql, currentSchema)` → `QueryResult.UseSchema` 更新自身 currentSchema 并回 `Message.UpdateCount(0)`;`Rows` 走 `sendRows`(Arrow IPC 编码成 `Message.ArrowBatch`)然后 `close()`;`Update` 走 `Message.UpdateCount`。异常 → `Message.ExecuteResponse.error`。**注意**:`rows.data().close()` 不在 finally 里(pre-existing,所有 Rows 路径都这样,非 EXPLAIN 特有)。
- `MetadataExecutor`(外挂,`catalog+allocator`)服务 `getSchemas`/`getTables`/`getColumns` 协议请求,`SessionHandler.handleMetadata` 走 `sendRows`。

**协议(`minidb-protocol`):**
- `Message` — sealed records:`Handshake`/`HandshakeAck`/`ExecuteRequest(requestId, sql)`/`SchemasRequest(requestId, schemaPattern)`/`TablesRequest(requestId, schemaPattern, tableNamePattern, types)`/`ColumnsRequest(requestId, schemaPattern, tableNamePattern, columnNamePattern)`/`CloseRequest`/`ExecuteResponse`/`ArrowBatch(requestId, lastBatch, data)`/`UpdateCount(requestId, count)`。
- `MessageType` — byte 常量。`MessageEncoder`/`MessageDecoder` — Netty 编解码。

**JDBC 客户端(`minidb-jdbc/`):**
- `MiniDbClient` — Netty 客户端,`execute(sql) → ClientResult.Rows/Update`。按 requestId 路由响应(`ConcurrentHashMap<Long, CompletableFuture>`),连接断开时 failAllPending。
- `MiniDbConnection`/`MiniDbStatement`/`MiniDbResultSet`/`MiniDbPreparedStatement`(客户端参数替换)。
- 客户端**不引用** `QueryExecutor`(纯通过网络),所以服务端构造器变化不影响 minidb-jdbc。

## 踩过的坑(经验教训)

1. **`./mvnw.cmd` 在 bash 下直接跑**,不要用 `mvnw.cmd`(PATH 找不到)、`cmd //c mvnw.cmd`(输出编码乱)、`mvn`(没装)。就是 `./mvnw.cmd ...`。
2. **Calcite 默认语法无 `ANALYZE` 关键字**。`EXPLAIN`/`EXPLAIN ANALYZE`/`ANALYZE` 必须在 `calcite.parse` 之前字符串前缀拦截(`QueryExecutor.execute` 顶部)。前缀匹配顺序:`EXPLAIN ANALYZE ` 必须先于 `EXPLAIN `,`ANALYZE`(exact)先于 `ANALYZE <table>`。
3. **Calcite 总是插入 `MiniDbProject`** 做列选择(`SELECT id,name FROM ...`)。EXPLAIN 要折叠 trivial Project 否则行数与预期不符;但 `estimate` 仍要遍历真实树(含 Project)算 childRows。
4. **`MiniDbSort.execute()` 是 eager**——物化+排序在 `execute()` 内完成,返回惰性迭代器。插桩计时 `start` 必须在 `wrapped.execute()` 之前,否则 Sort 的耗时漏掉。Sort 在物化后会 `input.close()`(此时子节点 elapsed 被记录)。
5. **Scan 的 batches 是表 owned**——`MiniDbScan.execute()` 的迭代器 `close()` 是 no-op。EXPLAIN ANALYZE 不能 `b.close()` 发出的 batch,靠 `it.close()` 级联(Filter/Sort 在自己 `close()` 里关 owned batches)。`MiniDbSort`/`MiniDbFilter` 分配自己的输出 batch 并在 `close()` 关闭。
6. **`FieldVector` 只有 no-arg `allocateNew()`**;要预设容量用 `setInitialCapacity(n); allocateNew();`,不是 `allocateNew(n)`。
7. **`Histogram` 的选择率是单列模型**——`ExplainExecutor.histogramForCondition` 按条件里第一个 `RexInputRef` 选列的直方图,然后对**整个**条件求选择率。复合跨列条件(如 `id>1 AND name<'m'`)对非匹配列走 `DEFAULT_SELECTIVITY` 兜底(已加 `typesCompatible` 守卫,不会抛 ClassCastException)。等值用 `Objects.equals`(类型安全)。
8. **`Histogram.compareValue` 泛型擦除坑**:`(Comparable<Object>)(Comparable<Double>)x` javac 拒绝(Comparable 不变),要 `(Comparable<Object>)(Comparable)x` raw 中转,加 `@SuppressWarnings`。
9. **`Histogram.rangeSelectivity` 插值**:用 `Double.compareTo` 只返回 -1/0/1,不能当数值差用——必须用 `numericDelta` 算真实差值再除以 `spanSize`。否则多单位桶插值全错。
10. **持久化统计用 Java 序列化**——`Histogram`/`Bucket`/`McValue`/`TableStats` 都要 `implements Serializable` + `serialVersionUID`。类字段变更会使旧 `.stats` 失效(`read()` 捕获后 LOG.warn 返回 null,降级为无统计)。
11. **`QueryExecutor` 4 参构造**`(catalog, storage, allocator, stats)`——移除旧 3 参构造会断 `MiniDbServer` 编译,改构造器时要同步改 `MiniDbServer.start()`。
12. **minidb-jdbc 的 `NoClassDefFoundError` 测试失败是环境问题**(Calcite 不在测试 classpath),与服务端改动无关——minidb-jdbc 不引用任何服务端类。别误判为回归。
13. **stale 钩子集中在 `StorageManager.markDirty`**——INSERT/UPDATE/DELETE/TRUNCATE 全走 `markDirty`,所以只需在那里加一次 `markStale`。`MiniDbModify` 的 no-match 早返回路径(`rewriteTable` 行 96)正确跳过 `markDirty`(表未变)。
14. **断开连接的快速失败**:`MiniDbClient`/`MiniDbConnection.isClosed()/isValid()` 依赖 `channelInactive` 标记 `connected=false`,连接池不用试查询就知道连接死了。
15. **`currentSchema` 不能放共享成员字段**——`QueryExecutor` 是所有连接共享的单例,`CalciteContext`/`Planner` 是其 final 成员。若 `currentSchema` 是 `CalciteContext` 可变字段,一个客户端 `USE SCHEMA` 会污染所有并发连接。必须作参数流经 `QueryExecutor.execute(sql, currentSchema)` → `Planner.plan(sql, currentSchema)` → `CalciteContext.planInCluster(sql, cluster, currentSchema)`。`SessionHandler` 持有 per-channel `currentSchema` 字段(默认 public),`USE SCHEMA` 返回 `QueryResult.UseSchema` 让其更新。
16. **`SqlIdentifier.getSimple()` 对复合名静默返回首段**——断言关闭时(JVM 默认)`getSimple()` 对 `public.users` 不抛而返回 `"public"`。限定名解析必须用 `node.name.names`(ImmutableList)分解:schema=`names.get(0)`,table=`names.get(names.size()-1)`。
17. **Calcite schema 树"提升表"副作用**——`MiniDbRootCalciteSchema.getTableMap()` 把当前 schema 的表暴露在 `minidb` 容器层(让 unqualified `t` 解析),导致 `RelOptTable.getQualifiedName()` 对 unqualified 表返回 `["minidb","t"]`(2 段,丢失 schema),对 `other.t` 返回 `["minidb","other","t"]`(3 段)。`MiniDbScan`/`MiniDbModify` 据此分流:`size>=3` 用倒数第二段作 schema,`size==2` 调 `ctx.getTable(裸名)` 由 `ExecContext.currentSchema` 解析。这是算子适配 schema 的唯一改动点(2/6 算子)。
18. **持久化子目录 + StatsManager key 语义**——文件路径 `data/<schema>/<table>.arrow`,`flushTable` 必须 `createDirectories(file.getParent())`(旧扁平格式不兼容)。`StatsManager` 零代码改动但 key 语义从 `table` 变 `schema.table`,`resolveKey(table)` 兼容裸名(默认 public)。`StatsManager.analyze` 当前只支持 public 表(`storage.getTable(DEFAULT_SCHEMA, table)`),非 public 表的 EXPLAIN ANALYZE 统计降级为无统计——分阶段交付,可接受。
19. **JDBC 元数据走专用协议消息**——`getSchemas`/`getTables`/`getColumns` 不复用 `ExecuteRequest`+伪SQL,而是 `minidb-protocol` 的 `SchemasRequest`/`TablesRequest`/`ColumnsRequest` 三条独立消息(响应复用 `ArrowBatch`)。服务端 `MetadataExecutor`(外挂,持 `catalog+allocator`,不依赖 storage/stats)从 `MiniDbCatalog` 物化 Arrow 行,`SessionHandler.handleMetadata` 走现有 `sendRows`。`getSchemas` 的 `TABLE_CATALOG` 恒 null、`getTables`/`getColumns` 的 `TABLE_CAT` 恒 null(MiniDB 无 catalog 概念,`getCatalog()=null`);`NULLABLE` 恒 1(列全可空);`getColumns` 24 列完整 JDBC 规范,无语义列填默认(`COLUMN_SIZE=0`/`NUM_PREC_RADIX=10`仅整数/`IS_NULLABLE="YES"` 等)。LIKE 过滤(`_`/`%`)在 `MetadataExecutor.compileLike` 转正则,`null` pattern 跳过过滤。
20. **标识符引号用双引号**——驱动 `getIdentifierQuoteString()` 返回 `"`,SQL IDE(DBeaver 等)据此生成 `select * from "public"."t"` 形式 SQL。Calcite parser 必须 `withQuoting(Quoting.DOUBLE_QUOTE)`(Lex.MYSQL 默认只认反引号,IDE SQL 会报 `ParseException: Encountered "\""`)。代价:**反引号标识符不再支持**(Quoting 是单值枚举,无双引号+反引号共存;现有代码/测试无反引号依赖)。
21. **Calcite 1.42 的 `AggregateCall` 参数存储**:纯列引用参数(如 `SUM(id)`)时 `rexList` 为空、索引存 `argList`;表达式参数(如 `SUM(id*2)`)才填 `rexList`。聚合算子取参数必须两个都查(先 `rexList` 后 `argList` 索引取输入列)。只查 `rexList` 会静默丢掉参数(SUM 恒 NULL,COUNT 退化为 COUNT(*))。
22. **Calcite 1.42 聚合类型推导**:`SUM(INTEGER)`→INTEGER(非 BIGINT)、`AVG(INTEGER)`→INTEGER(截断)、`SUM(DOUBLE)`→DOUBLE、`COUNT`→BIGINT、`MIN/MAX`→同参数。实现按 `Aggregate.getRowType()` 落地,断言/测试别假设 SUM(INT)→BIGINT。
23. **既有 bug:含 DOUBLE 字面量的 VALUES INSERT 失败**(与 aggregate 无关):Calcite 对多行 `VALUES (1, 10.5), ...` 或含 CAST 的单行生成 `LogicalUnion`(每行一个 `Project(CAST)` over 占位 `Values`),MiniDB 无 Union 规则 → `CannotPlan`。同源:NULL 字面量(`nullLiteral` 对 INTEGER 生成 BigIntVector)与 Project 字面量(literalVector 统一 BigIntVector)复制到 IntVector 列时 `RowCopier.copyRow` 抛 `MinorType` 不一致。**可行的数据构造**:单列任意类型逐行/多行 INSERT、多列多行同型(无 CAST)INSERT、`UPDATE ... SET col = NULL`(走 `writeValue` 类型转换路径)。
24. **DISTINCT 的实现落点**:`SELECT DISTINCT` 是 Calcite 规划的 `MiniDbAggregate(groupSet=全列, aggCalls=[])`,上轮分组实现天然支持(无需新代码);`COUNT(DISTINCT x)` 等是 `AggregateCall.isDistinct()`,需在 factoryFor 分流到 `DistinctAcc`(LinkedHashSet 去重,NULL 不参与)。注意:**只测 `COUNT(id)` 无法发现 distinct 未实现**(表行数碰巧等于去重数时会掩盖),要用含重复数据 + 期望去重后的值。
25. **UNION 执行顺序坑**(MiniDbUnion):先收集所有输入 batches,**merge 完成后**才 `close()` 输入迭代器——Project/Filter 的 close 释放自己 owned 的 batch,先 close 会导致向量 valueCount 归零(IndexOutOfBounds 或空结果)。Sort 同款:merge 后 close。
26. **`VectorSchemaRoot.of(vectors)` 的 rowCount 取第一个 vector 的 valueCount**——必须先 `v.setValueCount(dst)` 再 `of()`,否则 root.getRowCount() 恒 0(向量有数据但 root 显示 0 行)。Aggregate/Union 都是先 setValueCount 后 of。`root.setRowCount(n)` 会同时设置 root.rowCount 和所有 vector 的 valueCount。
27. **Calcite 的 `Union` 用公共字段 `all`**,无 `isAll()` getter——引用时用 `union.all`(LogicalUnion 同样)。
28. **UNION 去重实现**:Calcite 1.42 不注册 UnionToDistinctRule,`UNION` 保持 `LogicalUnion(all=false)`,去重在 MiniDbUnion 内部做(`LinkedHashSet<List<Object>>` 行级,规范化值含 null)。UNION 两侧列名不同没关系(RowCopier.copyRow 按索引拷),但类型必须兼容(Calcite 已校验/CAST)。
29. **SetOp 的 kind 语义**:Calcite `SetOp` 自带 `public final SqlKind kind` + `public final boolean all`,构造器 `(cluster, traits, inputs, kind, all)`。`INTERSECT`/`INTERSECT ALL` 的 kind 都是 `SqlKind.INTERSECT`(无 INTERSECT_ALL 枚举,靠 all 区分);`EXCEPT` 解析为 `LogicalMinus`,kind=`SqlKind.MINUS`(EXCEPT 是 MINUS 别名)。INTERSECT/EXCEPT 不去重展开,语义(去重/ALL 计数)在 MiniDbSetOp 内部完成。
30. **LogicalCalc 在 MiniDB 规划下不出现**:仅注册 ConverterRule、无优化规则时,常规 SQL(表达式/CASE/CAST/IN 子查询/EXISTS/行值/LIMIT/ALL 比较等 20+ 变体)全部规划为 LogicalProject/LogicalFilter,无 LogicalCalc。MiniDbCalc 是防御性算子(未来加 CalcMergeRule 或 Calcite 版本变化时用)。测试需绕过 planner 手工构造 RexProgram(RexProgramBuilder.addProject(RexNode,String)/addCondition(RexNode),无 register 方法;RexBuilder 无 makeLiteral(int),用 makeExactLiteral(BigDecimal, type) 保类型;SqlStdOperatorTable 在 sql.fun 包)。
31. **JOIN 关键点**:Calcite 对 INNER/LEFT/RIGHT/FULL/非等值都规划为 `LogicalJoin(condition, joinType)`,列索引跨两侧;`JoinInfo.of(left,right,condition)` 现成等值对分析。join 输出顺序随算法不同(HASH 右驱动、SORT_MERGE 排序序),测试断言前必须排序。NULL 键等值永不匹配(SQL 语义),三种算法都要显式处理。`RexInterpreter.comparison` 原只支持数值域,字符串列比较(join 条件/Filter 字符串等值)会 asLong 抛异常——已加 stringDomain 分支(toString 比较)。`COALESCE` 解析为 CASE,RexInterpreter 不支持(既有缺失)。
32. **窗口函数**:Calcite 1.42 把窗口函数放在 **Project 表达式里的 RexOver**(无 LogicalWindow 节点,除非优化规则);空分区保护包成 `CASE(>(COUNT(x) OVER (...), 0), SUM(x) OVER (...), null)`。ProjectToWindowRule 单独注册 NPE(缺 RelOptSchema)、与 ConverterRule 协同不触发——**别走那条路**,在 MiniDbProject 里检测 RexOver 后 eager 提取(visitOver 改写为窗口列引用)再 eval。窗口实现=WindowFunctions 静态工具。RexFieldCollation 无 getFieldIndex,用 `((RexInputRef) fc.left).getIndex()`。
33. **EXPLAIN trivial Project 折叠**:`ExplainExecutor` 原无条件折叠所有 MiniDbProject(列选择),窗口 Project(非 identity)也被折叠 → EXPLAIN 无 Project 行。已改为 `isTrivialProject`(全 RexInputRef 且索引连续才折叠)。
34. **RexInterpreter 的 CASE**:窗口 null 保护 `CASE(cond, then, else)` 需要求值;caseExpr 按行选第一个为真的分支,输出向量类型 = call.getType()(newVector 按 SqlTypeName 建 Int/BigInt/Float8/VarChar/Bit/Date/Timestamp)。同时覆盖 `COALESCE`(解析为 CASE)——坑 31 里说的 CASE 缺失已修复。

## 文档与计划

- `README.md` — 用户向说明(特性/类型/限制)。
- `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md` — 设计 spec(brainstorming 产出)。
- `docs/superpowers/plans/YYYY-MM-DD-<topic>.md` — 实现计划(writing-plans 产出)。
- `.claude/settings.local.json` — 本地权限白名单。
- `.superpowers/sdd/` — SDD 工作区(gitignored,临时 ledger/brief/report,合并后可删)。
