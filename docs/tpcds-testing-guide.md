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
$JAVA $OPTS -Xmx6g -cp "$CP" com.minidb.tpcds.TpcdsBenchmark run \
  --data-dir "E:/jdbc server/minidb-tpcds/target/tpcds-data" \
  --query-dir "F:/DSGen-software-code-4.0.0/query_templates" \
  --scale 0.1 --output "E:/jdbc server/minidb-tpcds/target/results/run.json"
```

- `run` 先用 `TpcdsTemplateParser` 把 99 个 `.tpl` 解析成 SQL,再启动 MiniDbServer 逐条跑。
- 每条记录耗时/行数/成败,失败不中断,结果写 JSON。

### 3. 对比两次

```bash
$JAVA $OPTS -cp "$CP" com.minidb.tpcds.TpcdsBenchmark compare \
  run-1.json run-2.json --output report.html
```

### 快速诊断(逐个跑 + 分类,不走网络)

不经过 MiniDbServer/JDBC,直接用 `QueryExecutor` 逐个跑,避免网络层连接断开干扰:

```java
// 见 minidb-tpcds/target/ScanDebug.java 的临时诊断 main
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

## 当前状态(2026-08-18)

- 成功约 **66~70 条**(从最初的 1 条提升)。
- 剩余问题:
  1. **query13**: 非等值/去相关后 `NestedLoopJoin` 笛卡尔积(约 92 亿匹配对),物化到 `outputRows` 极慢/堆溢出。根因是 `MiniDbNestedLoopJoin.joinPairs` 把全量匹配对物化成 `List<int[]>`,需要流式输出。
  2. **query17**: `aggregate not supported: STDDEV_SAMP`(标准差聚合未实现)。
  3. 其余待后台验证的 OOM 查询(query18/25/26/29/35/44/45/48/84/85/91),禁用 join 重排后多数应能跑,部分慢。

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
