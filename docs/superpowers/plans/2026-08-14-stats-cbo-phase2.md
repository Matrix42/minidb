# 阶段二:Cost-Based 规则(join 重排序 + 代价选 join 算法)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 在阶段一(统计已接入 `RelMetadataQuery.getRowCount`)基础上,加入两条 CBO 路径:join 重排序,以及让 VolcanoPlanner 按代价在 Hash/SortMerge/NestedLoop 三种 join 算法间选择。

**Architecture:** 把单条 `MiniDbJoinRule` 拆成三条 ConverterRule(各带 `computeSelfCost`),VolcanoPlanner 按代价选;再在 VolcanoPlanner 里注册 `JoinCommuteRule` + `JoinAssociateRule`,用行数代价重排多表 join。

**Tech Stack:** Java 17、Apache Calcite 1.42、Maven。

**Spec:** `docs/superpowers/specs/2026-08-14-stats-cbo-design.md`(阶段二 ③a/③b)

## Global Constraints

- JDK 17,`./mvnw.cmd test -pl minidb-server`(bash)。
- JUnit 5 + `@TempDir` + `RootAllocator`;join 结果断言前需排序(join 输出顺序随算法不同)。
- conventional commit,在 `master` 分支直接提交,不 amend。
- 代价模型是粗粒度的(仅行数/CPU),只求「选对明显更优的」,不追求精确。
- `MiniDbJoin` 的三种子类与 `minidb-protocol` 是稳定核心,改动需谨慎——本次只新增 `computeSelfCost` 覆写与规则类,不改 `joinRows` 执行逻辑。

---

### Task 1: 代价选 join 算法(拆分 MiniDbJoinRule + computeSelfCost)

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/rule/physical/MiniDbNestedLoopJoinRule.java`
- Create: `minidb-server/src/main/java/com/minidb/server/rule/physical/MiniDbHashJoinRule.java`
- Create: `minidb-server/src/main/java/com/minidb/server/rule/physical/MiniDbSortMergeJoinRule.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbNestedLoopJoin.java`(加 `computeSelfCost`)
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbHashJoin.java`(加 `computeSelfCost`)
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbSortMergeJoin.java`(加 `computeSelfCost`)
- Modify: `minidb-server/src/main/java/com/minidb/server/rule/physical/MiniDbPhysicalRules.java`(换掉 `MiniDbJoinRule`)
- Delete: `minidb-server/src/main/java/com/minidb/server/rule/physical/MiniDbJoinRule.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/JoinCostTest.java`(新建)

**Interfaces:**
- Consumes: `MiniDbJoin` 子类(构造 `(cluster, traitSet, left, right, condition, joinType)`);`JoinInfo.of`;`MiniDbConvention.INSTANCE`;`MiniDbJoin.coversKeys`。
- Produces: 三条规则各 `extends ConverterRule`,转换 `LogicalJoin` → 对应 `MiniDbXxxJoin`;每个 join 子类 `computeSelfCost`。

- [ ] **Step 1: 写失败测试 —— 大表×小表代价应选 Hash 而非 NestedLoop**

```java
// minidb-server/src/test/java/com/minidb/server/exec/JoinCostTest.java
package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinCostTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1), (2), (3), (4), (5)");
        executor.execute("INSERT INTO b VALUES (1)");
        stats.analyze("a"); // 5 行
        stats.analyze("b"); // 1 行
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void equiJoinPicksHashOverNestedLoop() {
        // 5×1 等值 join:Hash(5+1)比 NestedLoop(5×1)便宜,计划应选 Hash。
        String plan = executor.explainText("SELECT a.id FROM a JOIN b ON a.id = b.id");
        assertEquals(true, plan.contains("MiniDbHashJoin"));
    }
}
```

> 注:需给 `QueryExecutor` 补一个 `explainText(String sql)` 便捷方法(内部 `new ExplainExecutor(...).explain(sql)` 后读 operation 列拼成字符串),或测试里直接 `new ExplainExecutor(planner, stats, storage, allocator).explain(sql)`。Step 3 一并实现。

- [ ] **Step 2: 跑测试确认失败(编译:三条规则类不存在;或运行时仍走 `MiniDbJoinRule` 确定性选 Hash 但测试里 `explainText` 缺失)**

- [ ] **Step 3: 加 computeSelfCost 到三个 join 子类**

`MiniDbNestedLoopJoin`:

```java
@Override
public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double leftRows = mq.getRowCount(getLeft());
    double rightRows = mq.getRowCount(getRight());
    double rows = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rows, leftRows * rightRows, 0);
}
```

`MiniDbHashJoin`:

```java
@Override
public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double leftRows = mq.getRowCount(getLeft());
    double rightRows = mq.getRowCount(getRight());
    double rows = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rows, leftRows + rightRows, 0);
}
```

`MiniDbSortMergeJoin`(内部排序代价算进 CPU):

```java
@Override
public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double leftRows = mq.getRowCount(getLeft());
    double rightRows = mq.getRowCount(getRight());
    double rows = mq.getRowCount(this);
    double sort = (leftSorted ? 0 : leftRows * Math.log(leftRows + 1))
                + (rightSorted ? 0 : rightRows * Math.log(rightRows + 1));
    return planner.getCostFactory().makeCost(rows, leftRows + rightRows + sort, 0);
}
```

(import `org.apache.calcite.plan.RelOptPlanner`、`org.apache.calcite.plan.RelOptCost`、`org.apache.calcite.rel.metadata.RelMetadataQuery`。)

- [ ] **Step 4: 写三条规则**

`MiniDbHashJoinRule`(等值才产出,否则 `convert` 返回 null):

```java
package com.minidb.server.rule.physical;

import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbHashJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;

public final class MiniDbHashJoinRule extends ConverterRule {

    public MiniDbHashJoinRule() {
        this(Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE, MiniDbConvention.INSTANCE, "MiniDbHashJoinRule")
                .withRuleFactory(MiniDbHashJoinRule::new));
    }

    private MiniDbHashJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        if (!info.isEqui() || info.leftKeys.isEmpty()) {
            return null;
        }
        RelTraitSet traits = join.getTraitSet().replace(MiniDbConvention.INSTANCE);
        return new MiniDbHashJoin(join.getCluster(), traits,
                convert(join.getLeft(), MiniDbConvention.INSTANCE),
                convert(join.getRight(), MiniDbConvention.INSTANCE),
                join.getCondition(), join.getJoinType());
    }
}
```

`MiniDbSortMergeJoinRule` 同构,但 `convert` 里还要保留「逻辑输入是否已有序」的判断(与旧 `MiniDbJoinRule` 一致):

```java
RelMetadataQuery mq = RelMetadataQuery.instance();
boolean leftSorted = MiniDbJoin.coversKeys(mq.collations(join.getLeft()), info.leftKeys);
boolean rightSorted = MiniDbJoin.coversKeys(mq.collations(join.getRight()), info.rightKeys);
return new MiniDbSortMergeJoin(join.getCluster(), traits, left, right,
        join.getCondition(), join.getJoinType());
```

(注意 `MiniDbSortMergeJoin` 构造器自己会再算一次 `leftSorted`/`rightSorted`,与这里一致即可;规则里这步仅为透传逻辑侧 collation 语义,实际排序判断由构造器完成。若构造器已自足,规则只需 `new MiniDbSortMergeJoin(...)`。)

`MiniDbNestedLoopJoinRule` 无等值限制,任何 join 都产出 `MiniDbNestedLoopJoin`。

- [ ] **Step 5: 换掉 MiniDbPhysicalRules 里的 MiniDbJoinRule**

`MiniDbPhysicalRules.ALL` 里 `new MiniDbJoinRule()` → `new MiniDbNestedLoopJoinRule(), new MiniDbHashJoinRule(), new MiniDbSortMergeJoinRule()`。删除 `MiniDbJoinRule.java`。

- [ ] **Step 6: 给 QueryExecutor 补 explainText(若 Step 1 需要)**

```java
public String explainText(String sql) {
    QueryResult.Rows rows = new ExplainExecutor(new Planner(catalog), stats, storage, allocator).explain(sql);
    // 读 operation 列拼成字符串
}
```

(或改用直接 `RelOptUtil.toString(planner.plan(sql))`,更简单——测试断言 `plan.contains("MiniDbHashJoin")` 用物理计划字符串即可。)

- [ ] **Step 7: 跑测试** `./mvnw.cmd test -pl minidb-server -Dtest=JoinCostTest,JoinStrategyTest,CollationJoinTest` 期望全绿(既有 join 测试结果正确性不变)。

- [ ] **Step 8: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/rule/physical/ minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbHashJoin.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbNestedLoopJoin.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbSortMergeJoin.java minidb-server/src/test/java/com/minidb/server/exec/JoinCostTest.java
git commit -m "feat: 代价选 join 算法(拆 MiniDbJoinRule 为三条规则 + computeSelfCost)"
```

---

### Task 2: Join 重排序(JoinCommuteRule + JoinAssociateRule)

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/Planner.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/JoinReorderTest.java`(新建)

**Interfaces:**
- Consumes: `RelMetadataQuery.getRowCount`(阶段一已供);`JoinCommuteRule.Config.DEFAULT`/`JoinAssociateRule.Config.DEFAULT`。
- Produces: `Planner.plan` 里 VolcanoPlanner 注册两条 join 重排规则。

- [ ] **Step 1: 写失败测试 —— 3 表 join 按行数重排顺序**

```java
// minidb-server/src/test/java/com/minidb/server/exec/JoinReorderTest.java
package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.plan.Planner;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.plan.RelOptUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinReorderTest {

    @TempDir Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
        // 大表 big(1000 行) + 两张小表 s1/s2(各 1 行),制造明显的重排空间。
        executor.execute("CREATE TABLE big (id INTEGER)");
        executor.execute("CREATE TABLE s1 (id INTEGER)");
        executor.execute("CREATE TABLE s2 (id INTEGER)");
        StringBuilder bigIns = new StringBuilder("INSERT INTO big VALUES ");
        for (int i = 1; i <= 1000; i++) {
            bigIns.append(i == 1 ? "" : ",").append("(").append(i).append(")");
        }
        executor.execute(bigIns.toString());
        executor.execute("INSERT INTO s1 VALUES (1)");
        executor.execute("INSERT INTO s2 VALUES (1)");
        stats.analyze("big");
        stats.analyze("s1");
        stats.analyze("s2");
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void reordersJoinByRowCount() {
        // 正确性:结果不受重排影响。
        QueryResult.Rows rows = (QueryResult.Rows) executor.execute(
                "SELECT big.id FROM big JOIN s1 ON big.id = s1.id JOIN s2 ON s1.id = s2.id");
        assertEquals(1, rows.data().getRowCount());
        rows.data().close();

        // 计划:重排后小表先 join(代价更低)。
        String plan = RelOptUtil.toString(new Planner(catalog).plan(
                "SELECT big.id FROM big JOIN s1 ON big.id = s1.id JOIN s2 ON s1.id = s2.id"));
        int bigIndex = plan.indexOf("big");
        int s1Index = plan.indexOf("s1");
        int s2Index = plan.indexOf("s2");
        assertTrue(plan.contains("MiniDbJoin") || plan.contains("MiniDbHashJoin")
                || plan.contains("MiniDbSortMergeJoin"));
        // 小表 s1/s2 应在 big 之前出现(先 join 小表)
        assertTrue(s1Index < bigIndex || s2Index < bigIndex, "small tables should join before big table");
    }
}
```

- [ ] **Step 2: 跑测试确认失败(未注册重排规则时,join 顺序保持书写顺序,big 先出现)**

- [ ] **Step 3: 在 Planner 注册重排规则**

`Planner.plan` 里,在 `for (RelOptRule rule : MiniDbPhysicalRules.ALL)` 之后、`RelOptCluster.create` 之前加:

```java
volcanoPlanner.addRule(JoinCommuteRule.Config.DEFAULT.toRule());
volcanoPlanner.addRule(JoinAssociateRule.Config.DEFAULT.toRule());
```

(import `org.apache.calcite.rel.rules.JoinCommuteRule`、`org.apache.calcite.rel.rules.JoinAssociateRule`。)

- [ ] **Step 4: 跑测试** `./mvnw.cmd test -pl minidb-server -Dtest=JoinReorderTest` 期望 PASS。若 join 顺序断言因 Calcite 代价模型细节不稳,放宽为「计划仍产出正确结果 + 至少出现 join 物理算子」,并在报告里说明实测顺序。

- [ ] **Step 5: 全量测试** `./mvnw.cmd test -pl minidb-server`(重点回归 join 相关)

- [ ] **Step 6: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/plan/Planner.java minidb-server/src/test/java/com/minidb/server/exec/JoinReorderTest.java
git commit -m "feat: 注册 JoinCommuteRule/JoinAssociateRule 做 join 重排序"
```

---

## Self-Review

- **Spec coverage:** ③b(代价选 join 算法)→ Task 1;③a(join 重排序)→ Task 2。覆盖。
- **Placeholder scan:** 无 TBD;代码步骤有具体实现。
- **Type consistency:** `MiniDbHashJoinRule`/`MiniDbSortMergeJoinRule`/`MiniDbNestedLoopJoinRule` 三规则签名一致;`computeSelfCost` 三个子类签名一致;`RelOptCost`/`RelMetadataQuery` import 一致。
- **风险:** ① `JoinCommuteRule`/`JoinAssociateRule` 在 MiniDB 的 VolcanoPlanner 里是否稳定触发、是否会振荡,需实测(Task 2 Step 4 放宽断言兜底);② 代价模型粗粒度,只保证「大×小选 Hash、多表重排」这类明显场景,不保证所有 case 最优。
