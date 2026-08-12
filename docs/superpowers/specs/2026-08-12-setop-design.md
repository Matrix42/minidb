# 2026-08-12 — INTERSECT / EXCEPT 设计

## 目标

支持 `INTERSECT` / `INTERSECT ALL` / `EXCEPT` / `EXCEPT ALL`。沿聚合/UNION 的扩展模式:算子 + 规则 + 插桩/估算 + 测试。

## 规划形状(实测 Calcite 1.42)

- `INTERSECT` / `INTERSECT ALL` → `LogicalIntersect(all=…)`(kind=SqlKind.INTERSECT,无 INTERSECT_ALL 枚举)
- `EXCEPT` / `EXCEPT ALL` → `LogicalMinus(all=…)`(EXCEPT 是 MINUS 别名,kind=SqlKind.MINUS)
- Calcite 不去重展开,语义全部在算子内完成

## 实现

### `plan/MiniDbSetOp`(新算子,extends `SetOp implements MiniDbRel`)

SetOp 自带 `public final SqlKind kind` + `public final boolean all`,构造器 `(cluster, traits, inputs, kind, all)`,无需自定义区分字段。

- **eager**:每输入统计行级 key(`List<Object>` 规范化,含 null)计数 `LinkedHashMap`(保首见顺序)
- **语义**:INTERSECT 每 key 取 `min(count)` 跨输入;EXCEPT 取 `count0 - sum(rest)`,>0 保留;`all=false` 每 key 输出 1 行,`all=true` 输出 n 行;输出顺序 = 第一输入首见顺序
- 输出单批:getRowType() 建向量,writeObject 从 key 值直写(无需 RowCopier);**先 setValueCount 再 `VectorSchemaRoot.of`**(of 的 rowCount 取第一个 vector 的 valueCount,同 Union 坑 26)
- 空输入语义自动正确:INTERSECT 任一输入空 → 空;EXCEPT 第一输入空 → 空

### 规则

- `rule/MiniDbIntersectRule`:`LogicalIntersect` → MiniDbSetOp(kind=op.kind)
- `rule/MiniDbExceptRule`:`LogicalMinus` → MiniDbSetOp(kind=op.kind)
- `MiniDbRules.ALL` 追加;`Instrumenter` 加 `MiniDbSetOp` 分支(EXPLAIN ANALYZE);`ExplainExecutor.estimate`:INTERSECT → min(输入行数),EXCEPT → 第一输入行数依次减,remarks="estimated"

## 测试

`QueryExecutorTest` + 9 个:INTERSECT 去重、INTERSECT ALL min 计数(含重复 3)、EXCEPT 去重、EXCEPT ALL 计数相减、EXCEPT ALL 空结果、INTERSECT 无交集空结果、VARCHAR 列、EXPLAIN 含 SetOp、EXPLAIN ANALYZE 行数。全量 160 个通过。

## 不做

混合类型隐式 CAST、多层 EXCEPT 的 Calcite 嵌套语义验证(实现已按 n 输入统一处理)。
