# 2026-08-12 — UNION / UNION ALL 设计

## 目标

支持 `UNION`(去重)与 `UNION ALL`(保留重复)。作为聚合功能的姐妹特性,沿用同一套扩展模式:新算子 + 新规则 + 插桩/估算 + 测试。

## 规划形状(实测 Calcite 1.42)

- `UNION ALL` → `LogicalUnion(all=true)` over 各分支 `LogicalProject over Scan`
- `UNION` / `UNION DISTINCT` → `LogicalUnion(all=false)` over 同样的分支
- Calcite **不**自动把 `all=false` 展开为"聚合去重"(无 UnionToDistinctRule 注册),所以去重必须在算子内完成

## 实现

### `plan/MiniDbUnion`(新算子,extends `Union implements MiniDbRel`)

- **eager**:`execute()` 拉取所有输入迭代器的全部 batches(collect-then-merge)
- `all=true` → 级联输出;`all=false` → `LinkedHashSet<List<Object>>` 行级去重(NULL 参与,规范化值同聚合的 readObject)
- 输出单批 root,按 `getRowType()` 建向量;行拷贝用 `RowCopier.copyRow`(按索引,列名差异无碍)
- **顺序坑**:必须先 merge 再 `close()` 输入迭代器——Project/Filter 的 close 释放自己 owned 的 batch,Sort 同样是 merge 后 close
- **rowCount 坑**:`VectorSchemaRoot.of(vectors)` 的 rowCount 取第一个 vector 的 **valueCount**,所以必须先 `v.setValueCount(dst)` 再 `of()`(Aggregate 同款顺序);先 of 后 setValueCount → root.getRowCount() 恒 0

### `rule/MiniDbUnionRule`

`LogicalUnion` → `MiniDbUnion`,各输入递归 convert,`all` 透传(`union.all` 公共字段,Calcite Union 无 isAll())。

### 接线

- `MiniDbRules.ALL` 追加(Scan/Filter/Project/Sort/Values/Modify/Aggregate/Union)
- `Instrumenter.copyWithInputs` 加 `MiniDbUnion` 分支(EXPLAIN ANALYZE)
- `ExplainExecutor.estimate` 加分支:`all=true` → 各输入行数之和;`all=false` → `min(sum, 第一输入第一列直方图 distinct)`,无 stats 兜底 `max(1, sum/2)`,remarks="estimated"

## 测试

`QueryExecutorTest` + 8 个:UNION ALL 保重复(6 行)、UNION 去重(4 行)、单侧空输入、双侧空输入(0 行)、UNION+ORDER BY、多列 UNION ALL、EXPLAIN 含 Union、EXPLAIN ANALYZE Union 行数。全量 151 个通过。

## 不做

INTERSECT / EXCEPT、UNION 列类型不一致时的隐式 CAST(依赖既有 INSERT 字面量 bug 修复)。
