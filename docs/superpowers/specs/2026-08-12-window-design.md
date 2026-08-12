# 2026-08-12 — 窗口函数设计

## 目标

支持窗口函数:`SUM/AVG/COUNT/MIN/MAX OVER`、`ROW_NUMBER/RANK/DENSE_RANK`、`LEAD/LAG`(offset + default)、`FIRST_VALUE/LAST_VALUE`;PARTITION BY、ORDER BY、ROWS 帧。RANGE/ROWS 帧都用行位置(无 peer 扩展)。

## 规划形状(实测 Calcite 1.42)

- 窗口函数出现在 **Project 表达式的 RexOver** 中,且空分区保护被包成 `CASE(>(COUNT(x) OVER (...), 0), SUM(x) OVER (...), null)` ——**没有 LogicalWindow 节点**(规划器无优化规则,ProjectToWindowRule 也不可靠:依赖 RelBuilder/RelOptSchema,单独注册 NPE,与 ConverterRule 协同不触发)
- 因此最终实现:**MiniDbProject 内嵌 RexOver 路径**,不引入 Window 算子/规则

## 实现

### `plan/WindowFunctions`(静态工具)

- `materialize(RelNode, ExecContext) → List<Object[]>`(规范化行)
- `computeOver(RexOver, List<Object[]>) → List<Object>`:按 `RexWindow` 的 partitionKeys(RexInputRef 索引)分组(LinkedHashMap 保序)→ orderKeys(RexFieldCollation,`fc.left` 取索引,null 排最后)排序 → 逐行计算:
  - 聚合:帧内求和/计数/min/max,输出类型按 `over.getType()` 浮点/整数分流
  - ROW_NUMBER/RANK(peers 按 order keys,有 gap)/DENSE_RANK(无 gap)
  - LEAD/LAG:offset(operand 1,默认 1)+ default(operand 2)
  - FIRST_VALUE/LAST_VALUE:帧首/尾
  - 帧:`RexWindowBound` → 行位置(UNBOUNDED_PRECEDING=0、CURRENT_ROW=pos、N PRECEDING/FOLLOWING=pos∓N、UNBOUNDED_FOLLOWING=size-1),裁剪边界

### `MiniDbProject` 窗口路径

- `execute()` 检测投影表达式含 RexOver → `windowExecute`(eager):materialize 输入 → `RexShuttle.visitOver` 提取所有 RexOver 并按序替换为 `RexInputRef(inputCols + i)`(窗口列引用)→ 计算窗口列 → 构建拼接 batch(输入列 + 窗口列)→ eval 改写后表达式(现在只剩普通算子 + 列引用,如 CASE/GREATER_THAN)→ rename 输出
- lazy 路径不变(无窗口时零开销)

### 顺带修复

- **`RexInterpreter` CASE 支持**(窗口 null 保护的 CASE 必须能求值;一般 CASE WHEN 也受益)
- **`ExplainExecutor` trivial Project 折叠判定**:原无条件折叠所有 MiniDbProject,窗口 Project(非 identity)也被折叠 → EXPLAIN 无 Project 节点。改为 `isTrivialProject`(全 RexInputRef 且索引连续才折叠),窗口 Project 保留在 EXPLAIN/ANALYZE 树中
- **`ArrowTypes.field(RelDataType, String)`**:窗口列(over.getType() 是裸 RelDataType,无 RelDataTypeField 包装)建向量用

## 测试

`QueryExecutorTest` + 11 个:SUM over 分区、运行和(ORDER BY 默认帧)、ROW_NUMBER、RANK/DENSE_RANK(peers+gap)、COUNT(*) OVER ()、LAG/LEAD、LAG offset+default、ROWS BETWEEN 1 PRECEDING AND CURRENT ROW、FIRST/LAST_VALUE、窗口+WHERE 组合(窗口在过滤后)、EXPLAIN ANALYZE 窗口 Project 行数。全量 190 个通过。

## 坑

- Calcite 1.42 无 LogicalWindow(除非优化规则);ProjectToWindowRule 单独注册 NPE(缺 RelOptSchema)、与 ConverterRule 协同不触发——别走那条路,直接在 Project 里处理 RexOver
- RexOver 的 CASE 包装:CASE(>COUNT, SUM, null) —— RexInterpreter 必须先支持 CASE
- RexFieldCollation 无 getFieldIndex(),用 `((RexInputRef) fc.left).getIndex()`
- RANK peers 判定按 order keys 值相等;无 ORDER BY 时 RANK 全 1(标准)

## 不做

NTILE/CUME_DIST/PERCENT_RANK、DISTINCT 窗口、ignoreNulls、RANGE 帧的 peer 扩展、窗口 + GROUP BY 同时使用。
