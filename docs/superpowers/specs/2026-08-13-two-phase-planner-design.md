# 2026-08-13 — 两阶段规划 + 逻辑/物理分层 + join 拆分

## 目标

把单阶段规划(一个 VolcanoPlanner 直接把 Calcite `Logical*` 转 `MiniDb*`)重构为**两阶段规划**:

1. **逻辑阶段**:HepPlanner 用逻辑优化规则(如 FilterPushDown)优化 Calcite `Logical*` 树 —— 逻辑计划层复用 Calcite Logical*,不自建逻辑节点。
2. **物理阶段**:VolcanoPlanner 用 ConverterRule 把优化后的逻辑树转成物理算子。

同时:
- `plan` 与 `rule` 包各拆出 `logical`/`physical` 子包(物理算子移入 `plan/physical`,ConverterRule 移入 `rule/physical`)。
- `MiniDbJoin` 拆分:抽象基类 + 三个实现类(HashJoin / SortMergeJoin / NestedLoopJoin)。
- **collation 感知 join 选择**:输入数据有序时选 SortMergeJoin,内部跳过排序 —— 引入 `RelCollationTraitDef` + 物理节点声明输出 collation,`MiniDbJoinRule` 用 `RelMetadataQuery.getCollation` 判断输入 collation 是否覆盖 join 键。

## 现状(重构前)

- `Planner.plan(sql, schema)`:建单个 VolcanoPlanner,注册 `MiniDbRules.ALL`(12 个 ConverterRule),`changeTraits(logical, convention)` → `findBestExp()` 一步完成逻辑→物理。
- 物理算子(`MiniDbRel` 实现)与 `Planner`/`WindowFunctions`/`MiniDbConvention` 平铺在 `plan/`;规则平铺在 `rule/`。
- `MiniDbJoin` 单类 + `enum Strategy{AUTO,HASH,SORT_MERGE,NESTED_LOOP}`,内部三个私有实现方法;规则默认 AUTO(等值→HASH,否则→NESTED_LOOP)。
- 无 collation trait 体系;`MiniDbSort` 把 `RelCollation` 仅作构造参数。

## 实现

### 包结构

```
plan/                            rule/
  Planner.java        (编排,保留)   logical/
  logical/                          MiniDbLogicalRules.java  (HepPlanner 规则集,新增)
    LogicalOptimizer.java (新增)    physical/
  physical/                         MiniDbScanRule.java ... MiniDbJoinRule.java (移入 12 个)
    MiniDbRel.java       (移入)     MiniDbPhysicalRules.java (MiniDbRules 更名移入)
    MiniDbConvention.java(移入)
    MiniDbScan/Filter/Project/Sort/Values/Modify/Aggregate/Union/SetOp/Calc.java (移入)
    WindowFunctions.java (移入)
    MiniDbJoin.java      (移入 → 抽象基类)
    MiniDbHashJoin / MiniDbSortMergeJoin / MiniDbNestedLoopJoin.java (新增)
```

- 类名全部保留(仅改包);`MiniDbRules` 更名 `MiniDbPhysicalRules` 与 logical 侧对称。
- `MiniDbRel`/`MiniDbConvention` 属物理层,一并移入 `plan/physical`。

### 两阶段规划(`plan/Planner`)

```
plan(sql, currentSchema):
  RelRoot root = calcite.planInCluster(sql, cluster, currentSchema)   // 逻辑树(Calcite Logical*)
  RelNode logical = root.rel
  RelNode optimized = LogicalOptimizer.optimize(logical)              // 阶段1: HepPlanner(rule/logical)
  VolcanoPlanner volc = new VolcanoPlanner()                          // 阶段2: 物理转换
    + ConventionTraitDef + RelCollationTraitDef(新增)
    + MiniDbPhysicalRules.ALL
  RelNode converted = volc.changeTraits(optimized, MiniDbConvention.INSTANCE)
  volc.setRoot(converted)
  RelNode best = volc.findBestExp()
  return best
```

### `plan/logical/LogicalOptimizer`(新增)

- `static RelNode optimize(RelNode logical)`:新建 `HepPlanner(program)` 跑 `MiniDbLogicalRules.ALL`(HepMatchOrder 默认),返回 `findBest()`。
- HepPlanner 与 VolcanoPlanner 共享 cluster(若 `new HepPlanner(program)` 跨 cluster 有隔离问题,用 `volc.createContext()` 构造)。

### `rule/logical/MiniDbLogicalRules`(新增)

复用 Calcite 内置逻辑规则(证明正确的 FilterPushDown 等),初始集:
`FilterJoinRule.FILTER_ON_JOIN`(FilterPushDown 进 join)、`FilterProjectTransposeRule`、`ProjectMergeRule`、`FilterMergeRule`。

### 物理节点声明 collation

- VolcanoPlanner 注册 `RelCollationTraitDef`;所有物理节点 traitSet 含 collation 分量:
  - `MiniDbSort` → 声明自身排序 collation(构造时 `traitSet.replace(collation)`)。
  - `MiniDbFilter` → 透传输入 collation(排序保持)。
  - `MiniDbProject` → 全 `RexInputRef` 时把输入 collation 经投影映射,否则 EMPTY。
  - 其余(Scan/Values/Aggregate/Union/SetOp/Modify/Calc)→ EMPTY。
- 逻辑树侧无需声明(Calcite Logical* 默认 EMPTY,`RelMdCollation` 已处理 Sort/Project/Filter 派生)。

### join 拆分(`plan/physical`)

- **`MiniDbJoin`(抽象基类)** — 共享:构造字段、`execute()` 编排(物化两侧 → 调抽象策略 → buildOutput → 惰性迭代器)、全部辅助方法(materialize/readObject/writeObject/buildOutput/containsNull/keyOf/concat/compareKeys/sortedIndices)。抽象方法 `List<Object[]> joinRows(left, right, info, type, ctx)`。
- **`MiniDbHashJoin`** — 原 `hashJoin` 逻辑(等值)。
- **`MiniDbNestedLoopJoin`** — 原 `nestedLoopJoin` 逻辑(任意条件)。
- **`MiniDbSortMergeJoin`** — 原 `sortMergeJoin` 逻辑 + **collation 感知**:
  - 构造时用 `RelMetadataQuery.getCollation(input)` 检查左右输入 collation 是否覆盖 join 键(升序、字段索引前缀匹配)。
  - 已覆盖侧用输入序(identity 索引列表,跳过内部排序),未覆盖侧才 `sortedIndices` —— 保留"SORT_MERGE 对任意输入可用"的现有行为,同时实现"输入有序 → 避免排序"。
  - 三个子类各自实现 `copy()`,返回自身类型(Instrumenter 的 shadow 树不受影响)。

### join 选择(`rule/physical/MiniDbJoinRule`)

`convert(LogicalJoin)` 内:
1. 非等值(`!JoinInfo.of(...).isEqui()`)→ `MiniDbNestedLoopJoin`
2. 等值且两侧输入(逻辑树 `RelMetadataQuery.getCollation`)均覆盖 join 键 → `MiniDbSortMergeJoin`
3. 其余等值 → `MiniDbHashJoin`

## 波及文件

- 主代码:9 个物理算子 + WindowFunctions + MiniDbRel + MiniDbConvention + 12 规则类 = 纯包迁移(改 import);`Planner` 重写为两阶段。
- `QueryExecutor`/`ExplainExecutor` 接口不变(仍 `planner.plan(sql, schema)` → MiniDbRel)。
- `Instrumenter`/`ExplainExecutor`:`instanceof MiniDbJoin` 基类仍命中三个子类;`join.copy(...)` 由各子类返回自身类型。
- 测试:`JoinStrategyTest` 重写(Strategy 枚举移除 → 三个 join 类直接构造);`CalcTest`/`PlannerTest`/`QueryExecutorTest` 等仅改 import;`ExplainExecutorTest` 的 EXPLAIN 行期望可能变化(FilterPushDown 改变物理树形态),实施时逐条核对。

## 测试

- **`LogicalOptimizerTest`(新增)**:FilterPushDown 确实把 Filter 推进 join(计划形态断言)+ 端到端结果正确;FilterMerge 合并相邻 Filter。
- **`CollationJoinTest`(新增)**:子查询 `(SELECT * FROM t ORDER BY a) JOIN u ON t.a=u.b` 走 planner,断言产出 `MiniDbSortMergeJoin` 且结果正确(验证"输入有序 → 避免排序")。
- **`JoinStrategyTest`(重写)**:三个 join 类各自构造执行、断言结果(替代 Strategy 枚举驱动);等值→HashJoin、非等值→NestedLoop、排序输入→SortMergeJoin 的计划形态断言。断言前排序(join 输出顺序随算法不同)。

## 坑(新增到 CLAUDE.md)

1. join 输出顺序随算法不同(既有,继续)—— 断言前排序。
2. collation 是 trait 分量:注册 `RelCollationTraitDef` 后物理节点 traitSet 必须含 collation(否则 VolcanoPlanner 抛 `AssertionError: ...collation...`)。构造时 `traitSet.replace(convention).replace(collation)`。
3. `RelMdCollation` 只对 Sort/Project/Filter 派生 collation,其余(含 MiniDbScan)默认 EMPTY;`RelMetadataQuery.getCollation` 在逻辑树侧已足够判断 join 输入有序性,无需物理节点额外实现。
4. `MiniDbProject` 透传 collation 仅限全 `RexInputRef` 投影(列裁剪/重排);表达式投影排序不保证,必须 EMPTY。
5. `MiniDbSort` 声明输出 collation 时,`Sort.getCollation()` 与 traitSet 同步;ConverterRule 构造 Sort 时用 `traitSet.replace(convention).replace(collation)`。
6. HepPlanner 阶段复用 `calcite.planInCluster` 产出的逻辑树;`RelRoot` 的 fields/collation 字段在 Hep 优化后可能失效,但 Planner 只用 `root.rel`。

## 不做

- 不写自定义 MiniDB 逻辑节点(复用 Calcite Logical*,HepPlanner 直接优化)。
- 不引入纯 cost 模型 / 多条 join 转换规则 / VolcanoPlanner 自动插入排序转换器(方案 B 的内容)。
- 不实现 SortRemove / 排序消除(未来 collation trait 就绪后可做)。
- 不删 `MiniDb` 前缀(类名保持,仅改包)。
- SEMI/ANTI join(既有限制,保持抛错)。