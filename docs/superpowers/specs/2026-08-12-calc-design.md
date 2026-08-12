# 2026-08-12 — Calc 算子设计

## 目标

支持 `LogicalCalc` 节点。**实测结论:当前规划器配置(仅 ConverterRule,无优化规则)下常规 SQL 全部规划为 LogicalProject/LogicalFilter,不产生 LogicalCalc**(已用 20+ 条 SQL 变体验证,含表达式/CASE/CAST/ROW/IN 子查询/EXISTS/行值 IN/标量子查询/LIMIT/ALL 比较)。本实现是**防御性支持**:一旦 Calcite 版本变化或未来引入优化规则(如 CalcMergeRule)产生 LogicalCalc,规划器即可处理。

## 实现

### `plan/MiniDbCalc`(新算子,extends `Calc implements MiniDbRel`)

- `Calc` 构造器 `(cluster, traits, hints, child, program)`(hints 用空列表);rowType 由 `program.getOutputRowType()` 决定
- **lazy 流式**(仿 MiniDbProject + MiniDbFilter):对每个输入 batch:
  1. `program.getProjectList()`(List<RexLocalRef>)逐个 `expandLocalRef` 还原为 RexNode
  2. `program.getCondition()`(RexLocalRef,可 null)同样 expand
  3. `RexInterpreter.eval` 求值投影与条件(BitVector)
  4. 过滤行(条件 null 则全保留),`renameFiltered` 把满足条件的行 `copyFromSafe` 拷到按 Calc rowType 命名的新向量
  5. `kept==0` 返回 null → hasNext 继续拉下一批
- 输出 batch `VectorSchemaRoot.of(vectors)` + `setRowCount(kept)`;owned 队列跟踪待关闭
- **语义 = Project + Filter**:条件引用输入行,过滤发生在投影之后(值已求值,仅行选择)

### `rule/MiniDbCalcRule`

`LogicalCalc` → `MiniDbCalc`(program 透传)。`MiniDbRules.ALL` 追加;`Instrumenter` 加 `MiniDbCalc` 分支(`calc.copy(traits, input, calc.getProgram())`);`ExplainExecutor.estimate` 仿 Project(childRows)。

## 测试

`CalcTest`(exec 包)+ 3 个:用 `RexProgramBuilder` 手工构造 program 直接驱动 `MiniDbCalc`:
- 投影 `id*2` + 条件 `id>1` → `[4,6]`(过滤+表达式+重命名)
- 无条件投影 → `[1,2,3]`
- 条件全 false → 空结果

测试从 `planner.plan("SELECT id FROM t")` 提取 MiniDbScan 作输入(绕过 planner 直接测算子)。

## 踩坑

- `RexBuilder.makeLiteral(int)` 不存在 → `makeExactLiteral(BigDecimal, RelDataType)` 保类型
- `RexProgramBuilder` 无 `register(RexNode, boolean)` → 直接 `addProject(RexNode, String)` / `addCondition(RexNode)`
- `SqlStdOperatorTable` 在 `org.apache.calcite.sql.fun` 包

## 不做

Calc 合并优化规则(FilterProject 合并为 Calc)、与现有 Project/Filter 的去重替代(当前 Project/Filter 已覆盖常规 SQL)。
