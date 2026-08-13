# 两阶段规划 + 逻辑/物理分层 + join 拆分 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把单阶段规划重构为「HepPlanner 逻辑优化 + VolcanoPlanner 物理转换」两阶段,物理算子移入 `plan/physical`、转换规则移入 `rule/physical`,新增 `plan/logical`/`rule/logical`,并把 `MiniDbJoin` 拆成抽象基类 + 三个实现类,引入 collation trait 使「输入有序 → sort merge join 避免排序」可被规划器选中。

**Architecture:** 阶段1 `LogicalOptimizer` 用 HepPlanner + `rule/logical/MiniDbLogicalRules`(FilterPushDown 等 Calcite 内置逻辑规则)优化 Calcite `Logical*` 树;阶段2 VolcanoPlanner 注册 `ConventionTraitDef` + `RelCollationTraitDef`,用 `rule/physical/MiniDbPhysicalRules`(12 个 ConverterRule)转物理。`MiniDbJoinRule` 在 `convert()` 内用 `RelMetadataQuery.collations()` 判断输入是否已按 join 键有序:等值 + 双侧覆盖 → `MiniDbSortMergeJoin`(内部跳过排序),等值 → `MiniDbHashJoin`,非等值 → `MiniDbNestedLoopJoin`。join 共享逻辑(物化/输出/辅助)留在抽象基类 `MiniDbJoin`,三种算法各一个子类。

**Tech Stack:** Java 17、Apache Calcite 1.42.0(VolcanoPlanner / HepPlanner / ConverterRule / RelMetadataQuery / RelCollationTraitDef)、Apache Arrow、JUnit 5、Maven(`./mvnw.cmd`)。

**Spec:** `docs/superpowers/specs/2026-08-13-two-phase-planner-design.md`(本计划从 spec 立论;执行者两篇都读)

## Global Constraints

- JDK 17 必须;构建命令一律 `./mvnw.cmd ...`(bash 下直接跑,不带路径前缀,不用 `cmd //c`)。
- Calcite 1.42.0 API 注意:**内置规则无静态 INSTANCE 字段**,用 `XxxRule.Config.DEFAULT.toRule()` 或显式构造器;`RelMetadataQuery` 用 `collations(RelNode)` 返回 `ImmutableList<RelCollation>`(无 `getCollation`);`HepPlanner` 无 `(program, cluster)` 构造器,用 `new HepPlanner(program)` + `setRoot(rel)` + `findBest()`(Calcite `Programs.ofRules` 同款)。
- 物理算子类名不改(保留 `MiniDb` 前缀),只改包名。
- 遵守「改完即提交」:conventional commit,一个逻辑改动一个 commit,不 amend、不 `--no-verify`,在 `master` 分支。
- 物理算子改动需保证既有测试全绿;`ExplainExecutorTest` 的 EXPLAIN 行期望可能因 FilterPushDown 变化,以「语义等价的新计划」为准更新。

---

### Task 1: 物理算子移入 `plan/physical`

**Files:**
- Move(14 个文件,`git mv` + 改 package 声明):
  - `plan/MiniDbRel.java` → `plan/physical/MiniDbRel.java`
  - `plan/MiniDbConvention.java` → `plan/physical/MiniDbConvention.java`
  - `plan/MiniDbScan.java`, `plan/MiniDbFilter.java`, `plan/MiniDbProject.java`, `plan/MiniDbSort.java`, `plan/MiniDbValues.java`, `plan/MiniDbModify.java`, `plan/MiniDbAggregate.java`, `plan/MiniDbUnion.java`, `plan/MiniDbSetOp.java`, `plan/MiniDbCalc.java`, `plan/MiniDbJoin.java`, `plan/WindowFunctions.java` → `plan/physical/` 同名
- Modify(import 更新,`plan.X` → `plan.physical.X`):全部 `rule/*.java`(12 个规则类)、`exec/Instrumenter.java`、`exec/ExplainExecutor.java`、`exec/QueryExecutor.java`
- Modify(import 更新,仅 MiniDb* 那几行,`plan.Planner` 不动):测试 `exec/CalcTest.java`、`exec/JoinStrategyTest.java`

**Interfaces:**
- Consumes: 现有 14 个物理类内容不变,仅 package 声明 `com.minidb.server.plan.physical`。
- Produces: `MiniDbRel`/`MiniDbConvention` 及全部 `MiniDb*` 算子位于 `plan.physical`,可供后续任务引用。

- [ ] **Step 1: 建目录并 git mv**

```bash
mkdir -p "E:/jdbc server/minidb-server/src/main/java/com/minidb/server/plan/physical"
cd "E:/jdbc server/minidb-server/src/main/java/com/minidb/server/plan"
git mv MiniDbRel.java MiniDbConvention.java MiniDbScan.java MiniDbFilter.java MiniDbProject.java MiniDbSort.java MiniDbValues.java MiniDbModify.java MiniDbAggregate.java MiniDbUnion.java MiniDbSetOp.java MiniDbCalc.java MiniDbJoin.java WindowFunctions.java physical/
```

- [ ] **Step 2: 改 14 个文件 package 声明**

每个被移动文件首行 `package com.minidb.server.plan;` → `package com.minidb.server.plan.physical;`。用编辑工具逐文件改(勿用 sed)。

- [ ] **Step 3: 更新主代码 import**

以下文件里所有 `import com.minidb.server.plan.MiniDbXxx;` / `import com.minidb.server.plan.WindowFunctions;` 改为 `import com.minidb.server.plan.physical.MiniDbXxx;` / `...physical.WindowFunctions;`(`plan.Planner` 的 import 不改):
- `rule/` 下 12 个规则类(每个约 2 行:`MiniDbConvention` + 各自目标算子)
- `exec/Instrumenter.java`(12 行:MiniDbAggregate/Calc/Filter/Join/Project/Rel/Scan/SetOp/Sort/Union/Values)
- `exec/ExplainExecutor.java`(13 行:同 Instrumenter 的 12 个 MiniDb* + `Planer` 不动)
- `exec/QueryExecutor.java`(`MiniDbModify`、`MiniDbRel` → physical;`Planner` 不动)

- [ ] **Step 4: 更新测试 import**

- `exec/CalcTest.java`:`MiniDbCalc`/`MiniDbConvention`/`MiniDbScan` → physical;`Planner` 不动。
- `exec/JoinStrategyTest.java`:`MiniDbJoin` → `plan.physical.MiniDbJoin`;`Planner` 不动。

- [ ] **Step 5: 编译 + 全量测试**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: BUILD SUCCESS,全部测试通过(纯包迁移,行为不变)。

- [ ] **Step 6: Commit**

```bash
git add -A minidb-server/src
git commit -m "refactor: 物理算子移入 plan/physical 包"
```

---

### Task 2: 转换规则移入 `rule/physical`,规则集更名

**Files:**
- Move(13 个文件):`rule/MiniDbScanRule.java` … `rule/MiniDbJoinRule.java`(12 个规则类)→ `rule/physical/` 同名;`rule/MiniDbRules.java` → `rule/physical/MiniDbPhysicalRules.java`
- Modify: `plan/Planner.java`(import + `MiniDbRules.ALL` 引用)

**Interfaces:**
- Consumes: Task 1 的 `plan.physical` 类。
- Produces: `rule.physical.MiniDbPhysicalRules.ALL`(12 个 ConverterRule 列表),供 Task 4 的 Planner 使用。

- [ ] **Step 1: 建目录 + git mv**

```bash
mkdir -p "E:/jdbc server/minidb-server/src/main/java/com/minidb/server/rule/physical"
cd "E:/jdbc server/minidb-server/src/main/java/com/minidb/server/rule"
git mv MiniDbScanRule.java MiniDbFilterRule.java MiniDbProjectRule.java MiniDbSortRule.java MiniDbValuesRule.java MiniDbModifyRule.java MiniDbAggregateRule.java MiniDbUnionRule.java MiniDbIntersectRule.java MiniDbExceptRule.java MiniDbCalcRule.java MiniDbJoinRule.java MiniDbRules.java physical/
git mv physical/MiniDbRules.java physical/MiniDbPhysicalRules.java
```

- [ ] **Step 2: 改 package 声明 + 类名**

- 12 个规则类:`package com.minidb.server.rule;` → `package com.minidb.server.rule.physical;`
- `MiniDbPhysicalRules.java`:package 同上;`class MiniDbRules` → `class MiniDbPhysicalRules`;类内 `private MiniDbRules()` → `private MiniDbPhysicalRules()`。

- [ ] **Step 3: 更新 `plan/Planner.java`**

```java
import com.minidb.server.rule.physical.MiniDbPhysicalRules;
...
for (org.apache.calcite.plan.RelOptRule rule : MiniDbPhysicalRules.ALL) {
    planner.addRule(rule);
}
```

- [ ] **Step 4: 编译 + 全量测试**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: BUILD SUCCESS,全部测试通过。

- [ ] **Step 5: Commit**

```bash
git add -A minidb-server/src
git commit -m "refactor: 转换规则移入 rule/physical,MiniDbRules 更名 MiniDbPhysicalRules"
```

---

### Task 3: `MiniDbJoin` 拆为抽象基类 + 三个实现

**Files:**
- Modify: `plan/physical/MiniDbJoin.java` → 抽象基类(保留共享逻辑)
- Create: `plan/physical/MiniDbHashJoin.java`、`plan/physical/MiniDbSortMergeJoin.java`、`plan/physical/MiniDbNestedLoopJoin.java`
- Modify: `rule/physical/MiniDbJoinRule.java`(构造子类替代 `Strategy.AUTO`)
- Test: `exec/JoinStrategyTest.java`(重写)

**Interfaces:**
- Consumes: Task 1 的 `plan.physical` 布局。
- Produces:
  - `abstract class MiniDbJoin extends Join implements MiniDbRel`:`protected` 构造 `(cluster, traitSet, left, right, condition, joinType)`;`final execute(ExecContext)`;`protected abstract List<Object[]> joinRows(List<Object[]> left, List<Object[]> right, JoinInfo info, JoinRelType type, ExecContext ctx)`;`protected JoinInfo analyzeCondition()`;共享辅助(materialize/readObject/buildOutput/writeObject/containsNull/keyOf/concat/sortedIndices/nullFlag/compareKeys/compareValues 均 `protected`);`public static boolean coversKeys(List<RelCollation> collations, List<Integer> keys)`(Task 5 用,现在先放好)。
  - `MiniDbHashJoin`/`MiniDbSortMergeJoin`/`MiniDbNestedLoopJoin`:`public` 构造 `(cluster, traitSet, left, right, condition, joinType)`;各自实现 `copy(...)` 返回自身类型;实现 `joinRows(...)`。
  - `MiniDbSortMergeJoin`:`public boolean leftInputSorted()`/`rightInputSorted()`(Task 5 填充,Task 3 先返回 false 占位)。

- [ ] **Step 1: 把 `MiniDbJoin` 改成抽象基类**

读取现 `plan/physical/MiniDbJoin.java`,改造成:

```java
package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/**
 * Join base class. Subclasses implement one strategy (MiniDbHashJoin,
 * MiniDbSortMergeJoin, MiniDbNestedLoopJoin); this class owns materialization
 * of both inputs, output building, and the single-batch lazy iterator.
 * Rows are normalized to Object[]; output is a single batch.
 */
public abstract class MiniDbJoin extends Join implements MiniDbRel {

    protected MiniDbJoin(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode left, RelNode right, RexNode condition,
                         JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, Set.of(), joinType);
    }

    @Override
    public final BatchIterator execute(ExecContext ctx) {
        JoinRelType type = getJoinType();
        if (type == JoinRelType.SEMI || type == JoinRelType.ANTI) {
            throw new UnsupportedOperationException("semi/anti join not supported");
        }
        List<Object[]> leftRows = materialize(getLeft(), ctx);
        List<Object[]> rightRows = materialize(getRight(), ctx);
        JoinInfo info = analyzeCondition();
        List<Object[]> out = joinRows(leftRows, rightRows, info, type, ctx);
        VectorSchemaRoot root = buildOutput(out, ctx);

        boolean[] done = {false};
        return new BatchIterator() {
            @Override
            public boolean hasNext() {
                return !done[0];
            }

            @Override
            public VectorSchemaRoot next() {
                done[0] = true;
                return root;
            }

            @Override
            public void close() {
                root.close();
            }
        };
    }

    /** Strategy-specific join implementation. */
    protected abstract List<Object[]> joinRows(
            List<Object[]> left, List<Object[]> right,
            JoinInfo info, JoinRelType type, ExecContext ctx);

    protected JoinInfo analyzeCondition() {
        return JoinInfo.of(getLeft(), getRight(), getCondition());
    }

    // ---- shared helpers (verbatim from original MiniDbJoin.java) ----

    protected final List<Object[]> materialize(RelNode input, ExecContext ctx) {
        // 原文 materialize(...) 逐字搬移
    }

    protected static Object readObject(ValueVector v, int row) {
        // 原文 readObject(...) 逐字搬移
    }

    protected VectorSchemaRoot buildOutput(List<Object[]> rows, ExecContext ctx) {
        // 原文 buildOutput(...) 逐字搬移
    }

    protected static void writeObject(org.apache.arrow.vector.FieldVector out, int row, Object o) {
        // 原文 writeObject(...) 逐字搬移
    }

    protected static boolean containsNull(Object[] row, List<Integer> keys) {
        // 原文 containsNull(...) 逐字搬移
    }

    protected static List<Object> keyOf(Object[] row, List<Integer> keys) {
        // 原文 keyOf(...) 逐字搬移
    }

    protected static Object[] concat(Object[] l, Object[] r) {
        // 原文 concat(...) 逐字搬移
    }

    protected static List<Integer> sortedIndices(List<Object[]> rows, List<Integer> keys) {
        // 原文 sortedIndices(...) 逐字搬移
    }

    protected static int nullFlag(Object[] row, List<Integer> keys) {
        // 原文 nullFlag(...) 逐字搬移
    }

    protected static int compareKeys(Object[] a, List<Integer> ak,
                                     Object[] b, List<Integer> bk) {
        // 原文 compareKeys(...) 逐字搬移
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static int compareValues(Object a, Object b) {
        // 原文 compareValues(...) 逐字搬移
    }

    /** True if any collation covers {@code keys} as an ascending prefix. */
    public static boolean coversKeys(List<RelCollation> collations, List<Integer> keys) {
        for (RelCollation c : collations) {
            List<RelFieldCollation> fcs = c.getFieldCollations();
            if (fcs.size() < keys.size()) {
                continue;
            }
            boolean ok = true;
            for (int i = 0; i < keys.size(); i++) {
                RelFieldCollation fc = fcs.get(i);
                RelFieldCollation.Direction d = fc.getDirection();
                if (fc.getFieldIndex() != keys.get(i)
                        || (d != RelFieldCollation.Direction.ASCENDING
                            && d != RelFieldCollation.Direction.STRICTLY_ASCENDING)) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return true;
            }
        }
        return false;
    }
}
```

> 原 `MiniDbJoin.java` 的 `hashJoin`/`sortMergeJoin`/`nestedLoopJoin`/`buildProbeRoot`/`writeProbeRow` 移出到对应子类;`Set.of()` 保留在基类构造。删掉原 `Strategy` 枚举。

- [ ] **Step 2: 写 `MiniDbHashJoin`**

```java
package com.minidb.server.plan.physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import com.minidb.server.exec.ExecContext;

/** Hash join: builds a hash table on the left input keyed by the equi
 *  columns and probes with the right input. Equi-join only. */
public class MiniDbHashJoin extends MiniDbJoin {

    public MiniDbHashJoin(RelOptCluster cluster, RelTraitSet traitSet,
                          RelNode left, RelNode right, RexNode condition,
                          JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbHashJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<Object[]> joinRows(List<Object[]> left, List<Object[]> right,
                                      JoinInfo info, JoinRelType type, ExecContext ctx) {
        // 原文 hashJoin(left, right, info, type) 方法体逐字搬移:
        //   List<Integer> lk = info.leftKeys; List<Integer> rk = info.rightKeys;
        //   Map<List<Object>, List<Integer>> hash = new HashMap<>();
        //   ... 直到 return out;
    }
}
```

- [ ] **Step 3: 写 `MiniDbSortMergeJoin`**

```java
package com.minidb.server.plan.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import com.minidb.server.exec.ExecContext;

/** Sort-merge join: merges equal-key groups after sorting both sides by the
 *  equi columns (null keys last). Task 5 adds collation-aware skip-sorting. */
public class MiniDbSortMergeJoin extends MiniDbJoin {

    public MiniDbSortMergeJoin(RelOptCluster cluster, RelTraitSet traitSet,
                               RelNode left, RelNode right, RexNode condition,
                               JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbSortMergeJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<Object[]> joinRows(List<Object[]> left, List<Object[]> right,
                                      JoinInfo info, JoinRelType type, ExecContext ctx) {
        List<Integer> lk = info.leftKeys;
        List<Integer> rk = info.rightKeys;
        List<Integer> lorder = sortedIndices(left, lk);
        List<Integer> rorder = sortedIndices(right, rk);
        // 原文 sortMergeJoin(left, right, info, type) 方法体其余部分逐字搬移:
        //   boolean leftPreserved = ...; boolean rightPreserved = ...;
        //   boolean[] leftMatched = ...; boolean[] rightMatched = ...;
        //   Object[] nullLeft = ...; Object[] nullRight = ...;
        //   ... 直到 return out;
    }
}
```

- [ ] **Step 4: 写 `MiniDbNestedLoopJoin`**

```java
package com.minidb.server.plan.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.ExecContext;

/** Nested-loop join: evaluates the full RexNode condition per candidate pair
 *  (correctness over speed). Works for any condition. */
public class MiniDbNestedLoopJoin extends MiniDbJoin {

    public MiniDbNestedLoopJoin(RelOptCluster cluster, RelTraitSet traitSet,
                                RelNode left, RelNode right, RexNode condition,
                                JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbNestedLoopJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<Object[]> joinRows(List<Object[]> left, List<Object[]> right,
                                      JoinInfo info, JoinRelType type, ExecContext ctx) {
        // 原文 nestedLoopJoin(left, right, ctx) 方法体逐字搬移:
        //   JoinRelType t = getJoinType(); boolean leftPreserved = ...;
        //   ... buildProbeRoot(ncols, ctx) ... writeProbeRow(probe, l, r) ...
        //   ValueVector cond = ctx.interpreter().eval(getCondition(), probe); ...
        //   ... 直到 return out;
    }

    private VectorSchemaRoot buildProbeRoot(int ncols, ExecContext ctx) {
        // 原文 buildProbeRoot(ncols, ctx) 逐字搬移(getRowType().getFieldList())
    }

    private void writeProbeRow(VectorSchemaRoot probe, Object[] l, Object[] r) {
        // 原文 writeProbeRow(probe, l, r) 逐字搬移
    }
}
```

- [ ] **Step 5: 更新 `MiniDbJoinRule` 构造子类**

`rule/physical/MiniDbJoinRule.java`:

```java
package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;

public final class MiniDbJoinRule extends ConverterRule {

    public MiniDbJoinRule() {
        this(ConverterRule.Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE,
                        MiniDbConvention.INSTANCE, "MiniDbJoinRule")
                .withRuleFactory(MiniDbJoinRule::new));
    }

    private MiniDbJoinRule(ConverterRule.Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        RelTraitSet traits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
        RelNode left = convert(join.getLeft(), MiniDbConvention.INSTANCE);
        RelNode right = convert(join.getRight(), MiniDbConvention.INSTANCE);
        JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        if (info.isEqui()) {
            return new MiniDbHashJoin(join.getCluster(), traits, left, right,
                    join.getCondition(), join.getJoinType());
        }
        return new MiniDbNestedLoopJoin(join.getCluster(), traits, left, right,
                join.getCondition(), join.getJoinType());
    }
}
```

行为与旧 `Strategy.AUTO` 一致(等值→HASH,非等值→NESTED_LOOP)。

- [ ] **Step 6: 重写 `JoinStrategyTest`**

`exec/JoinStrategyTest.java`:把 `for (MiniDbJoin.Strategy s : ...)` 循环换成三个构造器的列表:

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbNestedLoopJoin;
import com.minidb.server.plan.physical.MiniDbSortMergeJoin;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies all three join strategies (HASH / SORT_MERGE / NESTED_LOOP) produce
 * identical results on the same equi-join, including outer joins with NULL
 * keys. Each strategy is applied by rebuilding the plan's MiniDbJoin node with
 * the corresponding concrete implementation.
 */
class JoinStrategyTest {

    @TempDir
    Path dataDir;

    private static final List<Function<MiniDbJoin, MiniDbJoin>> MAKERS = List.of(
            j -> new MiniDbHashJoin(j.getCluster(), j.getTraitSet(),
                    j.getLeft(), j.getRight(), j.getCondition(), j.getJoinType()),
            j -> new MiniDbSortMergeJoin(j.getCluster(), j.getTraitSet(),
                    j.getLeft(), j.getRight(), j.getCondition(), j.getJoinType()),
            j -> new MiniDbNestedLoopJoin(j.getCluster(), j.getTraitSet(),
                    j.getLeft(), j.getRight(), j.getCondition(), j.getJoinType()));

    @Test
    void allStrategiesProduceSameInnerResult() {
        run("SELECT a.id, b.val FROM a JOIN b ON a.id = b.id ORDER BY a.id");
    }

    @Test
    void allStrategiesProduceSameLeftJoinWithNullKeys() {
        run("SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.id = b.id ORDER BY aid");
    }

    @Test
    void allStrategiesProduceSameFullJoin() {
        run("SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.id = b.id");
    }

    @Test
    void allStrategiesProduceSameMultiColumnJoin() {
        run("SELECT a.id, b.id AS bid FROM a JOIN b ON a.id = b.id AND a.name = b.val ORDER BY a.id");
    }

    private void run(String sql) {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage, allocator, dataDir);
            storage.setStatsManager(stats);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'y'), (NULL, 'z')");
                executor.execute("INSERT INTO b VALUES (2, 'y'), (3, 'y'), (4, 'w'), (NULL, 'z')");

                List<String> expected = null;
                for (Function<MiniDbJoin, MiniDbJoin> maker : MAKERS) {
                    RelNode plan = new Planner(catalog).plan(sql);
                    MiniDbJoin join = findJoin(plan);
                    MiniDbJoin forced = maker.apply(join);
                    List<String> rows = new ArrayList<>(executeRows(forced, storage, allocator));
                    rows.sort(String::compareTo); // join output order is not guaranteed
                    if (expected == null) {
                        expected = rows;
                    } else {
                        assertEquals(expected, rows, "strategy diverged");
                    }
                }
            } finally {
                storage.close();
            }
        }
    }

    private static MiniDbJoin findJoin(RelNode node) {
        if (node instanceof MiniDbJoin join) {
            return join;
        }
        for (RelNode in : node.getInputs()) {
            MiniDbJoin found = findJoin(in);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static List<String> executeRows(MiniDbJoin join, StorageManager storage,
                                            BufferAllocator allocator) {
        // 原文 executeRows(...) 逐字保留(MiniDbJoin 签名不变)
    }
}
```

- [ ] **Step 7: 编译 + 全量测试**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: BUILD SUCCESS,`JoinStrategyTest` 4 个用例 + 其余全绿。

- [ ] **Step 8: Commit**

```bash
git add -A minidb-server/src
git commit -m "refactor: MiniDbJoin 拆为抽象基类 + Hash/SortMerge/NestedLoop 三个实现"
```

---

### Task 4: 两阶段规划(HepPlanner 逻辑优化 + VolcanoPlanner 物理转换)

**Files:**
- Create: `plan/logical/LogicalOptimizer.java`
- Create: `rule/logical/MiniDbLogicalRules.java`
- Modify: `plan/Planner.java`(重写为两阶段)
- Test: Create `plan/LogicalOptimizerTest.java`

**Interfaces:**
- Consumes: Task 2 的 `rule.physical.MiniDbPhysicalRules.ALL`;Task 1 的 `plan.physical.MiniDbRel`/`MiniDbConvention`;现有 `CalciteContext.planInCluster(sql, cluster, schema)`。
- Produces:
  - `plan.logical.LogicalOptimizer`:`public static RelNode optimize(RelNode logical)`。
  - `rule.logical.MiniDbLogicalRules`:`public static final List<RelOptRule> ALL`。
  - `Planner.plan(sql, currentSchema)` 返回物理 MiniDbRel 根(接口不变,调用方 QueryExecutor/ExplainExecutor 无需改)。

- [ ] **Step 1: 写 `rule/logical/MiniDbLogicalRules`**

```java
package com.minidb.server.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;

public final class MiniDbLogicalRules {

    /** HepPlanner 逻辑优化规则:FilterPushDown 进 join + 相邻算子合并/换位。 */
    public static final List<RelOptRule> ALL = List.of(
            new FilterJoinRule.FilterIntoJoinRule(false, RelFactories.LOGICAL_BUILDER,
                    FilterJoinRule.TRUE_PREDICATE),   // FilterPushDown into join
            FilterProjectTransposeRule.Config.DEFAULT.toRule(),
            ProjectMergeRule.Config.DEFAULT.toRule(),
            FilterMergeRule.Config.DEFAULT.toRule());

    private MiniDbLogicalRules() {
    }
}
```

> 若 `FilterProjectTransposeRule.Config.DEFAULT.toRule()` / `ProjectMergeRule.Config.DEFAULT.toRule()` 触发大量 EXPLAIN 行变动或行为异常,先删掉这两条,只留 `FilterIntoJoinRule` + `FilterMergeRule`,跑通后再逐个加回。

- [ ] **Step 2: 写 `plan/logical/LogicalOptimizer`**

```java
package com.minidb.server.plan.logical;

import com.minidb.server.rule.logical.MiniDbLogicalRules;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;

public final class LogicalOptimizer {

    private LogicalOptimizer() {
    }

    /** Runs the logical optimization rules over the Calcite Logical* tree. */
    public static RelNode optimize(RelNode logical) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleCollection(MiniDbLogicalRules.ALL);
        HepPlanner hep = new HepPlanner(builder.build());
        hep.setRoot(logical);
        return hep.findBest();
    }
}
```

- [ ] **Step 3: 重写 `plan/Planner.java` 为两阶段**

```java
package com.minidb.server.plan;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.logical.LogicalOptimizer;
import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbRel;
import com.minidb.server.rule.physical.MiniDbPhysicalRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

public class Planner {

    private final CalciteContext calcite;

    public Planner(MiniDbCatalog catalog) {
        this.calcite = new CalciteContext(catalog);
    }

    public RelNode plan(String sql) {
        return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public RelNode plan(String sql, String currentSchema) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        for (RelOptRule rule : MiniDbPhysicalRules.ALL) {
            planner.addRule(rule);
        }
        SqlTypeFactoryImpl typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        RelRoot root = calcite.planInCluster(sql, cluster, currentSchema);
        RelNode logical = root.rel;
        // Phase 1: logical optimization (HepPlanner over Calcite Logical* tree)
        RelNode optimized = LogicalOptimizer.optimize(logical);
        // Phase 2: physical conversion (VolcanoPlanner)
        RelNode converted = planner.changeTraits(optimized,
                optimized.getTraitSet().replace(MiniDbConvention.INSTANCE));
        planner.setRoot(converted);
        RelNode best = planner.findBestExp();
        if (!(best instanceof MiniDbRel)) {
            throw new IllegalStateException(
                    "planner produced non-physical root: " + best);
        }
        return best;
    }
}
```

> 本任务先不加 `RelCollationTraitDef`(Task 5 加)。

- [ ] **Step 4: 写 `LogicalOptimizerTest`**

`plan/LogicalOptimizerTest.java`:

```java
package com.minidb.server.plan;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.plan.physical.MiniDbFilter;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogicalOptimizerTest {

    @TempDir
    Path dataDir;

    @Test
    void filterIsPushedIntoJoinInputs() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage, allocator, dataDir);
            storage.setStatsManager(stats);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')");
                executor.execute("INSERT INTO b VALUES (2, 'u'), (3, 'v'), (4, 'w')");

                String sql = "SELECT a.id, b.val FROM a JOIN b ON a.id = b.id WHERE a.id > 2";
                RelNode plan = new Planner(catalog).plan(sql);
                MiniDbJoin join = findJoin(plan);
                assertTrue(join != null, "plan has no join: " + plan);
                // FilterPushDown: join 的直接输入应是 MiniDbFilter(或含之),而非 join 之上
                assertFalse(isDirectInput(join, MiniDbFilter.class),
                        "filter should be pushed into join inputs, plan=" + plan);

                List<String> rows = rows(executor, sql);
                assertEquals(1, rows.size());
                assertEquals("3|v", rows.get(0));
            } finally {
                storage.close();
            }
        }
    }

    private static MiniDbJoin findJoin(RelNode node) {
        if (node instanceof MiniDbJoin join) {
            return join;
        }
        for (RelNode in : node.getInputs()) {
            MiniDbJoin found = findJoin(in);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** True if any direct input of the join is an instance of clazz. */
    private static boolean isDirectInput(MiniDbJoin join, Class<?> clazz) {
        for (RelNode in : join.getInputs()) {
            if (clazz.isInstance(in)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> rows(QueryExecutor executor, String sql) {
        QueryResult result = executor.execute(sql);
        VectorSchemaRoot root = ((QueryResult.Rows) result).data();
        List<String> out = new ArrayList<>();
        try {
            for (int r = 0; r < root.getRowCount(); r++) {
                StringBuilder sb = new StringBuilder();
                for (int c = 0; c < root.getFieldVectors().size(); c++) {
                    if (c > 0) {
                        sb.append('|');
                    }
                    sb.append(root.getVector(c).isNull(r)
                            ? "NULL" : root.getVector(c).getObject(r));
                }
                out.add(sb.toString());
            }
        } finally {
            root.close();
        }
        return out;
    }
}
```

- [ ] **Step 5: 跑新测试 + 全量测试,核对 EXPLAIN 期望**

Run: `./mvnw.cmd test -pl minidb-server`
Expected:
- `LogicalOptimizerTest` 通过(证明 FilterPushDown 生效 + 结果正确)。
- 若 `ExplainExecutorTest` 失败:逐条检查失败断言,确认新 EXPLAIN 行是 FilterPushDown 后的**语义等价**计划(Filter 被推入 join 输入),更新期望值,不要改查询语义或放宽断言。
- 其余既有测试应保持通过。

- [ ] **Step 6: Commit**

```bash
git add -A minidb-server/src
git commit -m "feat: 两阶段规划 —— HepPlanner 逻辑优化(FilterPushDown)+ VolcanoPlanner 物理转换"
```

---

### Task 5: Collation trait + 有序输入自动选 SortMergeJoin(避免排序)

**Files:**
- Modify: `plan/Planner.java`(注册 `RelCollationTraitDef`)
- Modify: `rule/physical/MiniDbSortRule.java`(MiniDbSort traitSet 带 collation)
- Modify: `plan/physical/MiniDbSortMergeJoin.java`(构造时算左右输入是否已有序,执行时跳过内部排序)
- Modify: `rule/physical/MiniDbJoinRule.java`(用 `RelMetadataQuery.collations` 选择 SortMergeJoin)
- Test: Create `exec/CollationJoinTest.java`

**Interfaces:**
- Consumes: Task 3 的 `MiniDbJoin.coversKeys(...)` 与三个子类;Task 4 的两阶段 Planner。
- Produces:
  - `MiniDbSortMergeJoin.leftInputSorted()` / `rightInputSorted()`:`public boolean`,构造时由输入 collation 覆盖判断填充。
  - `MiniDbJoinRule` 行为:等值 + 双侧输入 collation 覆盖 join 键 → `MiniDbSortMergeJoin`,否则等值 → `MiniDbHashJoin`,非等值 → `MiniDbNestedLoopJoin`。

- [ ] **Step 1: Planner 注册 `RelCollationTraitDef`**

`plan/Planner.java` 在 `planner.addRelTraitDef(ConventionTraitDef.INSTANCE);` 后加一行(必须在 `RelOptCluster.create` / `planInCluster` **之前**,否则逻辑 rel 的 traitSet 缺 collation 分量):

```java
planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
```

import 已含 `RelCollationTraitDef`(Task 4 Step 3 已加)。

- [ ] **Step 2: `MiniDbSortRule` 给 Sort 声明 collation trait**

`rule/physical/MiniDbSortRule.java` 的 `convert`:

```java
@Override
public RelNode convert(RelNode rel) {
    LogicalSort sort = (LogicalSort) rel;
    return new MiniDbSort(sort.getCluster(),
            sort.getTraitSet()
                    .replace(MiniDbConvention.INSTANCE)
                    .replace(sort.getCollation()),
            convert(sort.getInput(), MiniDbConvention.INSTANCE),
            sort.getCollation(), sort.offset, sort.fetch);
}
```

`RelTraitSet.replace(RelTrait)` 按 trait def 替换;`sort.getCollation()` 是 `RelCollation`(其 trait def 即 `RelCollationTraitDef`)。若 `replace(sort.getCollation())` 编译报歧义,改用 `sort.getTraitSet().replace(RelCollationTraitDef.INSTANCE, sort.getCollation())`。

- [ ] **Step 3: `MiniDbSortMergeJoin` 加 collation 感知**

在 Task 3 的 `MiniDbSortMergeJoin` 基础上改造:

```java
package com.minidb.server.plan.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import com.minidb.server.exec.ExecContext;

/** Sort-merge join: merges equal-key groups. If an input's declared collation
 *  already covers the join keys it is used as-is (no internal sort); otherwise
 *  that side is sorted internally. */
public class MiniDbSortMergeJoin extends MiniDbJoin {

    private final boolean leftSorted;
    private final boolean rightSorted;

    public MiniDbSortMergeJoin(RelOptCluster cluster, RelTraitSet traitSet,
                               RelNode left, RelNode right, RexNode condition,
                               JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
        JoinInfo info = JoinInfo.of(left, right, condition);
        RelMetadataQuery mq = RelMetadataQuery.instance();
        this.leftSorted = coversKeys(mq.collations(left), info.leftKeys);
        this.rightSorted = coversKeys(mq.collations(right), info.rightKeys);
    }

    public boolean leftInputSorted() {
        return leftSorted;
    }

    public boolean rightInputSorted() {
        return rightSorted;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                     RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new MiniDbSortMergeJoin(getCluster(), traitSet, left, right,
                conditionExpr, joinType);
    }

    @Override
    protected List<Object[]> joinRows(List<Object[]> left, List<Object[]> right,
                                      JoinInfo info, JoinRelType type, ExecContext ctx) {
        List<Integer> lk = info.leftKeys;
        List<Integer> rk = info.rightKeys;
        List<Integer> lorder = leftSorted ? identity(left.size()) : sortedIndices(left, lk);
        List<Integer> rorder = rightSorted ? identity(right.size()) : sortedIndices(right, rk);
        // 原文 sortMergeJoin(left, right, info, type) 方法体其余部分逐字搬移
    }

    private static List<Integer> identity(int n) {
        List<Integer> order = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            order.add(i);
        }
        return order;
    }
}
```

> 注:`coversKeys` 定义在 `MiniDbJoin` 基类(public static),此处可直接用。`RelMetadataQuery.instance()` 是线程本地默认查询,`collations(RelNode)` 对物理 `MiniDbSort`(extends Sort)经 `RelMdCollation.sort` 返回其排序 collation,对 `MiniDbProject`(extends Project)经 `RelMdCollation.project` 透传。

- [ ] **Step 4: `MiniDbJoinRule` 按 collation 选 SortMergeJoin**

`rule/physical/MiniDbJoinRule.java` 的 `convert` 改为:

```java
@Override
public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    RelTraitSet traits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
    RelNode left = convert(join.getLeft(), MiniDbConvention.INSTANCE);
    RelNode right = convert(join.getRight(), MiniDbConvention.INSTANCE);
    JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
    if (!info.isEqui()) {
        return new MiniDbNestedLoopJoin(join.getCluster(), traits, left, right,
                join.getCondition(), join.getJoinType());
    }
    RelMetadataQuery mq = RelMetadataQuery.instance();
    boolean leftSorted = MiniDbJoin.coversKeys(mq.collations(left), info.leftKeys);
    boolean rightSorted = MiniDbJoin.coversKeys(mq.collations(right), info.rightKeys);
    if (leftSorted && rightSorted) {
        return new MiniDbSortMergeJoin(join.getCluster(), traits, left, right,
                join.getCondition(), join.getJoinType());
    }
    return new MiniDbHashJoin(join.getCluster(), traits, left, right,
            join.getCondition(), join.getJoinType());
}
```

新增 import:`com.minidb.server.plan.physical.MiniDbSortMergeJoin`、`com.minidb.server.plan.physical.MiniDbJoin`、`org.apache.calcite.rel.metadata.RelMetadataQuery`。

- [ ] **Step 5: 写 `CollationJoinTest`**

`exec/CollationJoinTest.java`:

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.plan.physical.MiniDbJoin;
import com.minidb.server.plan.physical.MiniDbSortMergeJoin;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollationJoinTest {

    @TempDir
    Path dataDir;

    @Test
    void preSortedInputsPickSortMergeJoinAndSkipSorting() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage, allocator, dataDir);
            storage.setStatsManager(stats);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
                executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
                executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')");
                executor.execute("INSERT INTO b VALUES (2, 'u'), (3, 'v'), (4, 'w')");

                String sql = "SELECT s.id, t.id AS bid "
                        + "FROM (SELECT * FROM a ORDER BY id) s "
                        + "JOIN (SELECT * FROM b ORDER BY id) t ON s.id = t.id";
                RelNode plan = new Planner(catalog).plan(sql);
                MiniDbSortMergeJoin join = findSortMergeJoin(plan);
                assertTrue(join != null, "expected MiniDbSortMergeJoin, plan=" + plan);
                assertTrue(join.leftInputSorted(), "left input should be pre-sorted");
                assertTrue(join.rightInputSorted(), "right input should be pre-sorted");

                List<String> rows = executeRows(join, storage, allocator);
                rows.sort(String::compareTo); // join output order not guaranteed
                assertEquals(List.of("2|2", "3|3"), rows);
            } finally {
                storage.close();
            }
        }
    }

    private static MiniDbSortMergeJoin findSortMergeJoin(RelNode node) {
        if (node instanceof MiniDbSortMergeJoin join) {
            return join;
        }
        for (RelNode in : node.getInputs()) {
            MiniDbSortMergeJoin found = findSortMergeJoin(in);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static List<String> executeRows(MiniDbJoin join, StorageManager storage,
                                            BufferAllocator allocator) {
        // 与 JoinStrategyTest.executeRows 相同的逐字实现
    }
}
```

> **测试构造注意事项**:若 Calcite 把派生表里的无 `LIMIT` `ORDER BY` 优化掉(规划后找不到 Sort),在子查询加 `LIMIT 1000`(`(SELECT * FROM a ORDER BY id LIMIT 1000)`)保 Sort 节点;并先跑 `findSortMergeJoin` 断言失败时打印 plan 定位。

- [ ] **Step 6: 编译 + 全量测试**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: BUILD SUCCESS,`CollationJoinTest` 通过(规划器在有序输入下产出 SortMergeJoin 且结果正确),既有测试全绿。

- [ ] **Step 7: Commit**

```bash
git add -A minidb-server/src
git commit -m "feat: collation trait 驱动 join 选择 —— 有序输入自动用 sort merge join 避免排序"
```

---

### Task 6: 更新文档

**Files:**
- Modify: `CLAUDE.md`

**Steps:**

- [ ] **Step 1: 更新 CLAUDE.md 包结构段落**

- 架构段落「物理算子(plan/,均 implements MiniDbRel)」→ 说明 `plan/logical`(LogicalOptimizer)+ `plan/physical`(MiniDb* 算子)+ `rule/logical`(MiniDbLogicalRules)+ `rule/physical`(ConverterRule + MiniDbPhysicalRules)。
- `plan/MiniDbJoin` 条目 → `plan/physical/MiniDbJoin`(抽象基类)+ `MiniDbHashJoin`/`MiniDbSortMergeJoin`/`MiniDbNestedLoopJoin`(Task 3/5 行为)。
- `plan/Planner` 条目 → 两阶段流程(HepPlanner 逻辑优化 → VolcanoPlanner 物理转换,collation trait)。

- [ ] **Step 2: 新增坑位**

把 spec「坑」节的 6 条追加到 CLAUDE.md「踩过的坑」:HepPlanner 无 (program,cluster) 构造器、Calcite 1.42 内置规则无静态 INSTANCE、`RelMetadataQuery.collations()` 替代 `getCollation`、collation trait 注册时机(先于 cluster 创建)、`MiniDbProject`/`MiniDbFilter` 物理层暂不声明 collation(RelMetadataQuery 已覆盖 join 决策)、派生表 ORDER BY 无 LIMIT 可能被优化掉。

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: 两阶段规划/逻辑物理分层/join 拆分后的 CLAUDE.md 更新"
```

---

## Self-Review

**1. Spec coverage:**
- 包结构(plan/logical、plan/physical、rule/logical、rule/physical)→ Task 1/2/4。
- 两阶段规划(LogicalOptimizer + MiniDbLogicalRules + Planner 重写)→ Task 4。
- FilterPushDown 逻辑规则 → Task 4(FilterIntoJoinRule)。
- Collation trait 驱动(RelCollationTraitDef + Sort 声明 + join 规则用 RelMetadataQuery)→ Task 5。
- join 拆 3 类 → Task 3。
- 有序输入 → SortMergeJoin 避免排序 → Task 5(CollationJoinTest)。
- 测试(LogicalOptimizerTest/CollationJoinTest/JoinStrategyTest 重写)→ Task 3/4/5。
- 文档更新 → Task 6。
- spec「不做」:不写自定义逻辑节点、不做纯 cost/多规则/自动排序转换器、不删 MiniDb 前缀、SEMI/ANTI 抛错不变 —— 均未违反。

**2. Placeholder scan:** 无 TBD/TODO。「逐字搬移」均指向原 `MiniDbJoin.java` 的具体方法名,执行者可对照原文;`executeRows(...)` 复用 JoinStrategyTest 原文,唯一见占位风险点在 Task 5 测试构造注意事项已给出应对。

**3. Type consistency:** `coversKeys(List<RelCollation>, List<Integer>)` 定义于基类 `MiniDbJoin`(Task 3),SortMergeJoin(Step 3)与 JoinRule(Step 4)均引用同签名;`leftInputSorted()`/`rightInputSorted()` 定义(Step 3)与测试调用(Step 5)一致;`RelMetadataQuery.instance().collations(RelNode)` 全计划统一。
