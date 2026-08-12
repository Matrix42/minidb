# 2026-08-12 — JOIN 设计(nested loop / sort merge / hash)

## 目标

支持 `JOIN`,三种算法:NestedLoop(任意条件)、SortMerge(等值)、Hash(等值);INNER/LEFT/RIGHT/FULL OUTER。SEMI/ANTI 不支持(明确抛错)。

## 规划形状(实测 Calcite 1.42)

- 所有 join 类型 → `LogicalJoin(condition=[RexNode], joinType=[inner/left/right/full])`,列索引跨两侧(左 0..L-1,右 L..L+R-1)
- 多表 join 树形嵌套;`SELECT a.id, b.val` 顶层 Project 裁剪列
- `JoinInfo.of(left, right, condition)` 提供现成等值对分析(`leftKeys`/`rightKeys`/`isEqui()`)

## 实现

### `plan/MiniDbJoin`(新算子,extends `Join implements MiniDbRel`)

`enum Strategy { AUTO, HASH, SORT_MERGE, NESTED_LOOP }`;规则默认 AUTO。AUTO = 纯等值(isEqui,即 AND 叶子全是 `lcol = rcol`)→ HASH,否则 → NESTED_LOOP。

- **物化**:两侧输入全量物化为 `List<Object[]>`(规范化值,含 null),输出单批
- **HASH**:左建表(`Map<List<Object>, List<Integer>>`,等值键列),右探测;NULL 键不建表不匹配;LEFT/RIGHT/FULL 补未匹配行(null 另一侧)
- **SORT_MERGE**:两侧按等值键排序(null 排最后),归并等值组交叉输出;NULL 键显式不匹配
- **NESTED_LOOP**:逐对行构造 1 行拼接 root(左列+右列),`RexInterpreter.eval` 求值条件(BitVector);O(L×R×列数)
- 输出:`getRowType()`(左+右)建向量,writeObject 直写;**先 setValueCount 再 of()**(坑 26)
- outer 语义:LEFT 保留左未匹配、RIGHT 保留右未匹配、FULL 两者;`leftPreserved`/`rightPreserved` 标志
- SEMI/ANTI → `UnsupportedOperationException`

### `rule/MiniDbJoinRule`

`LogicalJoin` → `MiniDbJoin(strategy=AUTO)`。`MiniDbRules.ALL` 追加;`Instrumenter` 分支(`join.copy(traits, condition, left, right, joinType, false)`);`ExplainExecutor.estimate` = 左右行数积 × 0.1(remarks="estimated")。

### 顺带修复:`RexInterpreter.comparison` 字符串域

`comparison` 原来只支持数值(long/double),VARCHAR 列比较(如 `a.name = b.val`)走 `asLong` 抛异常——影响 NESTED_LOOP 与 Filter 字符串等值。新增 `stringDomain` 分支(toString 比较)。

## 测试

- `QueryExecutorTest` + 12 个:INNER/LEFT/RIGHT/FULL、非等值、多条件 AND、NULL 键不匹配、三表、WHERE 组合、逗号连接、EXPLAIN/EXPLAIN ANALYZE
- `JoinStrategyTest` + 4 个:三种算法(排除 AUTO)在 INNER/LEFT(含 NULL 键)/FULL/多列等值上输出**集合等价**(join 不保证顺序,比较前排序)

## 坑

- join 输出顺序随算法不同(HASH 右驱动、SORT_MERGE 排序序)——断言前排序
- `COALESCE` 解析为 CASE 表达式,RexInterpreter 不支持(既有缺失,测试避开)
- NULL 键在等值连接中永不匹配(SQL 语义),HASH/SORT_MERGE/NESTED_LOOP 三处都要处理

## 不做

SEMI/ANTI join、残余条件过滤(非等值附加条件时整体走 NESTED_LOOP)、hash 建表侧自动选小、CASE/COALESCE 表达式、自相关 join。
