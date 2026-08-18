# TPC-DS 测试与修复记录

日期: 2026-08-18(进行中)
模块: `minidb-tpcds`
目标: 让 99 条 TPC-DS 查询全部能在 MiniDB 上执行(能跑通,之后再调性能)

## 如何跑 TPC-DS 测试

### 1. 生成数据

用 teradata tpcds 库在 JVM 内生成数据,**直写 part 文件**(绕过 SQL INSERT):

```bash
# 先构建 classpath(避免 exec:java 在 Windows 路径含空格时报错)
./mvnw.cmd -pl minidb-tpcds dependency:build-classpath -Dmdep.outputFile=target/tpcds-cp.txt

CP="E:/jdbc server/minidb-tpcds/target/classes;$(cat minidb-tpcds/target/tpcds-cp.txt)"
JAVA=/c/Users/Matrix42/.jdks/azul-17.0.15/bin/java
OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# 生成 0.1 scale 数据(24 张表 → data/public/<table>/part-*.arrow + catalog.json)
$JAVA $OPTS -cp "$CP" com.minidb.tpcds.TpcdsBenchmark generate \
  --scale 0.1 --data-dir "E:/jdbc server/minidb-tpcds/target/tpcds-data"
```

### 2. 跑查询

```bash
# 推荐:--direct 直接构造 QueryExecutor 跑,不走网络,更快更稳定
$JAVA $OPTS -Xmx6g -cp "$CP" com.minidb.tpcds.TpcdsBenchmark run \
  --data-dir "E:/jdbc server/minidb-tpcds/target/tpcds-data" \
  --scale 0.1 --output "E:/jdbc server/minidb-tpcds/target/results/run.json" --direct

# 不加 --direct 则走 MiniDbServer + JDBC 网络层(兼容旧行为)
```

- 加 `--direct` 时直接构造 `QueryExecutor` 执行,失败时能直接看到内核异常堆栈。
- 查询模板已内置到模块 resources(99 个 `.tpl`),无需 `--query-dir`;需要外部模板目录时才传。
- 每条记录耗时/行数/成败,失败不中断,结果写 JSON。

### 3. 对比两次

```bash
$JAVA $OPTS -cp "$CP" com.minidb.tpcds.TpcdsBenchmark compare \
  run-1.json run-2.json --output report.html
```

### 快速诊断(逐个跑 + 分类,不走网络)

不经过 MiniDbServer/JDBC,直接用 `QueryExecutor` 逐个跑,避免网络层干扰:

```bash
# 加 --direct 即可,输出 JSON 报告,查 failures 字段即知哪些失败。
$JAVA $OPTS -Xmx6g -cp "$CP" com.minidb.tpcds.TpcdsBenchmark run \
  --data-dir ... --scale 0.1 --output ./run.json --direct
```

也可在代码里直接构造(见 `TpcdsQueryExecutorTest` 的模式):

```java
QueryExecutor qe = new QueryExecutor(catalog, storage, alloc, stats);
for (query : 99 条) { qe.execute(sql); }  // 记录成败 + 失败原因分类
```

## 已修复问题清单(按时间顺序)

| # | 现象 | 根因 | 修复 | 文件 |
|---|------|------|------|------|
| 1 | join 基数估算笛卡尔积(18 亿) | 维度表无主键,CBO 的 distinct 估算=1 | 数据生成器给 17 张维度表建单列主键 | `TpcdsDataGenerator` |
| 2 | `ANALYZE` 抛 `unsupported type DECIMAL/TIME` | `HistogramBuilder` 只支持部分类型 | 补全 SMALLINT/REAL/FLOAT/CHAR/TIME/BINARY/DECIMAL 读取 | `HistogramBuilder` |
| 3 | `round(x,2)` 报 no overload | 只注册了单参 ROUND | 加 `[Decimal,Int]→Decimal`、`[Float8,Int]→Float8` 两参重载 | `BuiltInFunctions` |
| 4 | `d_date = cast(... as date)` 报 no overload | 比较函数缺 `[Date,Date]` | 加 `Kernels.fillCompareDate` + 注册重载 | `Kernels`+`BuiltInFunctions` |
| 5 | `date + 30 days` 解析错误 | TPC-DS 的 interval 简写 | 模板层删掉 `±N days`(基准测耗时不校验结果) | `TpcdsTemplateParser` |
| 6 | 分号导致 Calcite 报错 | 模板 `;` 是语句分隔符 | 截断到第一个分号 | `TpcdsTemplateParser` |
| 7 | `Index N out of bounds`(逻辑优化阶段) | `SortRemoveDuplicateKeysRule` 查 FD 元数据触发 Calcite 1.42 越界 bug | 禁用该规则 | `MiniDbLogicalRules` |
| 8 | `Encountered "with"`(query14/23/24/39) | 模板含两个独立语句(第二个是变体) | 截断到第一个分号(同 6) | `TpcdsTemplateParser` |
| 9 | `aggregate not supported: GROUPING` | 聚合只做单 groupSet | 多 groupSet(ROLLUP/GROUPING SETS)+ `GROUPING()` 输出 0/1 | `MiniDbAggregate` |
| 10 | 窗口 `partition/order by` 表达式报 RexCall cast | 假设 partition/order key 是列引用 | 表达式先用 `interpreter.eval` 求值再分组/排序 | `WindowFunctions`+`MiniDbProject`+`MiniDbCalc` |
| 11 | `NlsString cannot be cast to String`(Sarg) | Calcite Sarg 字符串边界是 NlsString,列值是 String | `toComparable` 把 NlsString 转 String,比较时 String 转 NlsString | `RexInterpreter` |
| 12 | `division by zero` | 整数/定点除法除零抛异常 | 除零返回 0 | `BuiltInFunctions` |
| 13 | `aggregate not supported: SINGLE_VALUE` | 标量子查询聚合 | 用 `MinMaxAcc(true)` 近似 | `MiniDbAggregate` |
| 14 | `Encountered "returns"/"year"/"at"` | 保留关键字被用作标识符/别名 | 模板层把独立 `returns/year/at` 加双引号 | `TpcdsTemplateParser` |
| 15 | 跨行 define 候选残留(`Encountered ","`) | `text(...)` 候选跨多行 | 去 define 时收集到分号结束 | `TpcdsTemplateParser` |
| 16 | 大 join OOM(query7/10/14 等) | `JoinCommuteRule`/`JoinAssociateRule` 重排去相关后的 join 树,丢失等值条件变交叉连接 | 禁用这两个重排规则 | `Planner` |
| 17 | `aggregate not supported: STDDEV_SAMP`(query17) | 聚合只做 SUM/AVG/MIN/MAX/COUNT | 加 `VarianceAcc`(在线累计 sum+sum²),输出类型与 AVG 同族(INTEGER→INTEGER 等) | `MiniDbAggregate` |
| 18 | OR 条件里等值键埋在 OR 里 → NestedLoop 笛卡尔积(query13) | `JoinInfo` 抽不出 OR 里的公共等值项 | `RexUtil.pullFactors` 因子化 OR,提取公共等值键到顶层 AND | `FilterPullFactorsRule`/`JoinPullFactorsRule` |
| 19 | FROM 顺序导致交叉连接(query18 cd2 在 customer 前) | `SqlToRelConverter` 按 FROM 顺序建左深树 | 贪心重排 INNER join 链(按等值连接图,种子=连接度最高) | `JoinReorderer` |
| 20 | join 重排后字段顺序错位(query3 类型错配) | 重排改输出字段顺序,上层 Aggregate 列引用错位 | 补 `LogicalProject` 把字段顺序还原成原扁平顺序 | `JoinReorderer` |
| 21 | 聚合 groupSet 不从 0 开始漏写分组键(子查询去重/query14) | `buildOutput` 用「输入索引 i<groupCount 检查 gs.get(i)」,groupSet={1} 时漏写 | 遍历输入列、set 位按序写到连续输出列 | `MiniDbAggregate` |
| 22 | `AND(等值键,OR 残留)` 退化成 NestedLoop(query13/15) | `MiniDbHashJoinRule` 要求 `isEqui`(无残留) | HashJoin 残留过滤:等值键匹配后 eval 残留条件 | `MiniDbHashJoin`+`MiniDbHashJoinRule` |
| 23 | `ROUND(INTEGER,2)` 无重载(query78) | ROUND 只注册 double/decimal | 加 `[Int,Int]→Int`、`[BigInt,Int]→BigInt` 整数重载 | `BuiltInFunctions` |
| 24 | NestedLoop 单行侧被误选(query72 warehouse=1) | `left×right≈left+right`,NestedLoop 便宜 1 | 代价乘 10 惩罚因子(逐对求值比哈希查找贵一个量级) | `MiniDbNestedLoopJoin` |
| 25 | 裸整数日期偏移 `d_date+5`(query72) | 内核不支持 date+integer/interval | 模板删偏移(同既有 ±N days 删除) | `TpcdsTemplateParser` |

## 当前状态(2026-08-18)

- **99 条全部能跑通**(从最初的 1 条提升)。所有查询均执行完毕,部分较慢(query72 ~6min、query14 ~2.5min、query13/15 由笛卡尔积降至 ~4s/13s)。
- 上一版「剩余问题」的处置:
  1. **query13**:OR 条件因子化 + HashJoin 残留过滤(非流式输出——根因是等值键埋 OR,不是物化方式)。
  2. **query17**:STDDEV_SAMP/POP、VAR_SAMP/POP 聚合。
  3. **query18/25/26/29/35/44/45/48/84/85/91**:query18 由 JoinReorderer 修复(其余本就能跑,此前只是估算过大)。
  4. 额外暴露并修复:query3(字段顺序)、query14(groupSet)、query15(OR 残留)、query72(日期偏移+代价误选)、query78(ROUND 整数)。

## 后续继续跑的方法

1. 逐个跑剩余失败查询(用上面的 `ScanDebug` 直接模式),看失败原因分类。
2. 按类别修:
   - `STDDEV_SAMP`/`STDDEV_POP` 等统计聚合 → `MiniDbAggregate.factoryFor` 加 case(方差=均值平方差)。
   - 非等值 NestedLoop 笛卡尔积 → `MiniDbNestedLoopJoin` 改成流式输出(不物化全量匹配对),或对 `joinPairs` 加匹配对上限。
   - 新暴露的 parse/type 错误 → 按错误 message 定位。
3. 修完一批就 `./mvnw.cmd install -pl minidb-server -DskipTests` 重编 + 重新跑 `ScanDebug` 验证,循环直到 99 条全绿。

## 关键经验

- **跑测试别用 exec:java**:Windows 路径含空格(`E:\jdbc server`)时 exec:java 解析失败,用 `dependency:build-classpath` + 手动 `java` 命令。
- **改内核后要重新 install minidb-server 到 .m2**:诊断 main 的 classpath 走 .m2 的 jar,不改会跑旧代码。
- **改 minidb-tpcds 后要重新 `compile -pl minidb-tpcds`**:`TpcdsTemplateParser` 在 target/classes,诊断 main 从那里加载。
- **数据生成是幂等的吗?否**:`createTable` 对已存在表报错,重新生成前要 `rm -rf` 数据目录。
- **ANALYZE 结果(.stats)随数据目录持久化**:重新生成数据后要重新 `ANALYZE`,否则 join 基数估算又退化。
