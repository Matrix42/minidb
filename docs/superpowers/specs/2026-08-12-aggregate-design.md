# MiniDB Aggregate 支持设计

日期:2026-08-12

## 目标

SELECT 聚合查询:COUNT/SUM/AVG/MIN/MAX,多列 GROUP BY,空 GROUP BY(全局聚合),HAVING。

## 范围

- 聚合函数:`COUNT(*)`、`COUNT(col)`、`SUM`、`AVG`、`MIN`、`MAX`。不含 DISTINCT 聚合与 GROUP_CONCAT(后续可加)。
- 聚合参数可为表达式(如 `SUM(price * 2)`),在输入 batch 上按现有 `RexInterpreter` 逐行求值。
- `GROUP BY` 支持多列;空 `GROUP BY`(无分组键的全局聚合)。
- `HAVING`:Calcite 将其规划为 Aggregate 之上的 `Filter`(条件引用聚合输出列),MiniDbFilter + RexInterpreter 已具备求值能力,无需新代码,仅测试锁定。
- `EXPLAIN` / `EXPLAIN ANALYZE` 支持新算子(estimate + Instrumenter 插桩)。

## 架构

### 新物理算子 `plan/MiniDbAggregate extends Aggregate implements MiniDbRel`

- **eager 执行**:`execute(ctx)` 拉取输入全量,流式分组聚合,输出单批。
- **分组**:key 为 `List<Object>`(各分组列规范化包装值,`null` 保留为 null);`LinkedHashMap` 保持组首见顺序。分组列值从输入 batch 向量读取(IntVector→Integer、BigInt→Long、Float8→Double、VarChar→String、Bit→Integer、Date→Integer、Timestamp→Long)。
- **累加器**:每个 `AggregateCall` 一个 `Accumulator` 实例(`COUNT`=long;`SUM` 按参数类型 long 或 double 累加;`AVG`=sum+count;`MIN`/`MAX`=Comparable best)。组状态为累加器列表,由 per-call 工厂按组创建。
- **NULL 语义**:聚合忽略 NULL;`COUNT(*)` 计所有行;空输入且无 GROUP BY → 1 行(`COUNT`=0,其余 NULL);空输入且有 GROUP BY → 0 行;分组键为 NULL 的值自成一组。
- **参数求值**:每批对 `call.rexList` 逐元素 `ctx.interpreter().eval(rex, batch)`,逐行喂给累加器,用后 `close()`。
- **输出**:按 `getRowType()` 字段建 `VectorSchemaRoot`(group 列 + 聚合列,字段名/类型来自 Calcite 推导),每组一行,单批返回。写值按目标向量类型(`RowCopier.writeValue` 同款 switch 思路)。

### 类型约定(Calcite 推导,实现按 rowType 落地)

- COUNT → BIGINT;SUM(整数)→ BIGINT;SUM(浮点)→ DOUBLE;AVG 按 Calcite 推导(整数 AVG 截断);MIN/MAX 同参数类型。

### 新规则 `rule/MiniDbAggregateRule`(rule 包)

- `LogicalAggregate` → `MiniDbAggregate`(groupSet/groupSets/aggCalls 透传,输入 `convert` 到 MINIDB convention)。`MiniDbRules.ALL` 末尾追加。

### Instrumenter / ExplainExecutor

- `Instrumenter.copyWithInputs` 加 `MiniDbAggregate` 分支:`agg.copy(traits, inputs.get(0), agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList())`。
- `ExplainExecutor.estimate` 加 `MiniDbAggregate` 分支:无 group → 1 行;有 group → `min(childRows, 第一分组列直方图 distinct)`(无 stats 时取 childRows 上界),remarks "estimated"。`operationName` 自动输出 "Aggregate"。

## 测试

- `PlannerTest`:`SELECT COUNT(*) FROM t` 规划根为 `MiniDbAggregate`。
- `QueryExecutorTest` 新增用例(或独立测试类):
  - 五函数数值正确性(整数/浮点/VARCHAR MIN/MAX)
  - COUNT(col) 跳过 NULL、COUNT(*) 全计
  - 单列/多列 GROUP BY;分组键 NULL 自成一组
  - 空 GROUP BY 全局聚合;空表:全局 COUNT=0、其余 NULL、GROUP BY 空表 0 行
  - HAVING 过滤
  - 表达式参数 `SUM(id * 2)`
  - EXPLAIN 含 Aggregate 行;EXPLAIN ANALYZE 行数与实际一致

## 不做(后续可加)

DISTINCT 聚合、GROUP_CONCAT、聚合内嵌套聚合、窗口函数。
