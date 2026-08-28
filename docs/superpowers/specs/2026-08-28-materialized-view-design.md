# 物化视图设计 spec

## 概述

在 MiniDB 中实现物化视图（Materialized View）。物化视图是查询结果的物理副本，存储为普通表，支持 DML 增量刷新和查询自动重写。

### 目标

- `CREATE MATERIALIZED VIEW mv AS <query>` — 创建物化视图，执行查询并持久化结果
- `DROP MATERIALIZED VIEW mv` — 删除物化视图及其存储
- `REFRESH MATERIALIZED VIEW mv` — 手动刷新（stale → 全量，非 stale → no-op）
- DML 增量刷新：基表 INSERT/UPDATE/DELETE 后自动增量维护 MV
- 查询自动重写：用户查询被 Calcite 自动重写为对 MV 的扫描

### 非目标

- 多表 JOIN 物化视图（增量刷新过于复杂，暂不支持）
- DISTINCT / 子查询 / 窗口函数 / UNION / 递归 CTE 的物化视图
- 增量刷新日志（delta log）— 每次 DML 立即增量刷新，无延迟

---

## 支持的查询类型

### 单表 SPJ（SELECT-PROJECT-JOIN——无 JOIN 即单表）

```sql
CREATE MATERIALIZED VIEW mv AS SELECT col1, col2 FROM t WHERE col3 > 0
```

增量刷新：INSERT 时对新行执行 WHERE 过滤，结果追加到 MV；DELETE 时从 MV 删除匹配行；UPDATE = DELETE + INSERT。

### 单表聚合（GROUP BY + SUM/COUNT/AVG/MIN/MAX）

```sql
CREATE MATERIALIZED VIEW mv AS
  SELECT col1, SUM(amount), COUNT(*), AVG(price), MIN(dt), MAX(dt)
  FROM t GROUP BY col1
```

增量刷新：对新行/旧行按 GROUP BY 分组计算聚合值，合并到 MV 现有行（SUM/COUNT 加减，AVG 拆为 SUM+COUNT 除法，MIN/MAX 比较）。

### AVG 的内部表示

AVG 不直接存储，拆为 `SUM(expr)` 和 `COUNT(expr)` 两列。查询重写时由 Calcite 自动组合为 `SUM/COUNT`。

### 拒绝的场景

以下查询类型在 `CREATE MATERIALIZED VIEW` 时直接拒绝：

- 多表 JOIN
- DISTINCT
- 子查询（WHERE 中的子查询、FROM 子查询）
- 窗口函数（OVER）
- UNION / INTERSECT / EXCEPT
- 递归 CTE（WITH RECURSIVE）
- HAVING
- LIMIT / OFFSET（SPJ 路径）
- 非 SUM/COUNT/AVG/MIN/MAX 的聚合函数

---

## 架构

### 模块划分

| 模块 | 文件 | 职责 | 类型 |
|------|------|------|------|
| 数据模型 | `catalog/MVDefinition.java` | 物化视图定义（查询 SQL + 依赖表 + 结构信息） | 新增 |
| 数据模型 | `catalog/MVStructure.java` | 增量刷新所需的结构信息（SPJ/聚合） | 新增 |
| 核心管理 | `exec/MVManager.java` | 物化视图生命周期管理（创建/删除/刷新/依赖索引） | 新增 |
| 增量刷新 | `exec/IncrementalRefreshEngine.java` | 增量刷新核心逻辑（SPJ/聚合） | 新增 |
| Catalog | `catalog/MiniDbCatalog.java` | 反向索引 `table → List<MV>`、MV 存储 | 修改 |
| Catalog | `catalog/CatalogSnapshot.java` | 新增 `materializedViews` 字段 | 修改 |
| 存储 | `storage/common/TableSchema.java` | 新增 `mvDefinition` 字段 | 修改 |
| 存储 | `storage/JsonCatalogStore.java` | 序列化 MV 定义 | 修改 |
| DDL | `exec/QueryExecutor.java` | 处理 `SqlCreateMaterializedView`/`SqlDropMaterializedView`/`REFRESH` | 修改 |
| DML | `plan/physical/MiniDbModify.java` | 收集 delta 数据，写入 TxHandle | 修改 |
| 事务 | `transaction/TxHandle.java` | 新增 `pendingMVRefresh` 队列 | 修改 |
| 网络 | `netty/SessionHandler.java` | commit 后触发增量刷新 | 修改 |
| 规划 | `plan/Planner.java` | MVStructure 提取 + MV 查询验证 | 修改 |
| Calcite | `calcite/MiniDbRootCalciteSchema.java` | 注册 `MaterializedViewTable` 供查询重写 | 修改 |
| 信息模式 | `exec/InformationSchema.java` | 新增 `materialized_views` 系统表 | 修改 |
| 信息模式 | `catalog/InformationSchemaCatalog.java` | 新增物化视图表元数据 | 修改 |

### 物化视图生命周期

```
CREATE MATERIALIZED VIEW mv AS <query>
  → ① Planner.plan(query) 得到 RelNode 树
  → ② 从 RelNode 提取 MVStructure（SPJ/聚合），验证只含单表
  → ③ 从 RelNode 提取依赖表列表（MiniDbScan → schema.table）
  → ④ 在 StorageManager 创建一张普通 ArrowTable（tableType=MATERIALIZED_VIEW）
  → ⑤ 执行查询，全量填充结果到该表
  → ⑥ 写入 TableSchema（含 mvDefinition + dependencies）到 catalog
  → ⑦ 建立反向索引：基表 → 依赖它的 MV 集合

基表 DML (INSERT/UPDATE/DELETE)
  → ① MiniDbModify 执行写入
  → ② 收集变更行数据（delta VectorSchemaRoot）
  → ③ 如果在事务中：追加到 TxHandle.pendingMVRefresh
  → ④ 如果 auto-commit：立即同步调用 IncrementalRefreshEngine.refresh()
  → ⑤ commit 路径：TransactionManager.commit() 后，遍历 pendingMVRefresh 执行刷新

REFRESH MATERIALIZED VIEW mv
  → ① 检查 staleGroups 是否非空（整体 stale 或部分组 stale）
  → ② stale → 全量刷新（TRUNCATE + 重算全量）
  → ③ 非 stale → no-op（已通过 DML 增量刷新保持最新）

DROP MATERIALIZED VIEW mv
  → ① 删除存储表
  → ② 删除 catalog 条目
  → ③ 清理反向索引
```

---

## 数据模型

### MVDefinition

```java
public record MVDefinition(
    String schemaName,
    String name,
    String querySql,                    // 规范化后的定义 SQL
    List<ColumnMeta> columns,           // 输出列
    List<TableRef> dependencies,        // 依赖的基表列表
    MVStructure structure               // 增量刷新所需的结构信息
) {}

public record TableRef(String schemaName, String tableName) {}
```

### MVStructure

```java
public sealed interface MVStructure {

    /** SPJ：SELECT col... FROM single_table WHERE ... */
    record Spj(
        String querySql,                // 可重执行的查询 SQL
        List<String> outputColumns      // 输出列名（与 MV 列对应）
    ) implements MVStructure {}

    /** 单表聚合：GROUP BY + SUM/COUNT/AVG/MIN/MAX */
    record Aggregate(
        String querySql,                // 可重执行的查询 SQL
        List<String> outputColumns,     // 输出列名
        List<String> groupByColumns,    // GROUP BY 列
        List<AggFunc> aggFuncs          // 聚合函数列表
    ) implements MVStructure {}

    record AggFunc(String outputColumn, AggType type, String inputColumn) {}
    enum AggType { SUM, COUNT, AVG, MIN, MAX }
}
```

### TableSchema 变更

```java
public record TableSchema(
    String schemaName, String name,
    List<ColumnMeta> columns,
    List<String> primaryKey, List<List<String>> uniqueKeys,
    List<ForeignKey> foreignKeys,
    StorageFormat storageFormat,
    TableType tableType,           // 新增 MATERIALIZED_VIEW
    List<IndexDef> indexes,
    MVDefinition mvDefinition      // 新增：非物化视图时为 null
) {}
```

### Catalog 反向索引

```java
// MiniDbCatalog 中新增
private final Map<TableRef, Set<String>> mvDependencyIndex = new ConcurrentHashMap<>();

// 查询受影响的 MV
public Set<String> getDependentMVs(String schemaName, String tableName);

// 注册/注销依赖
public void addMVDependency(TableRef baseTable, String mvFullName);
public void removeMVDependencies(String mvFullName);
```

### CatalogSnapshot 扩展

```java
public record CatalogSnapshot(
    List<String> schemas,
    List<TableSchema> tables,
    List<ViewDefinition> views,
    List<MVDefinition> materializedViews,  // 新增
    Map<String, TableStats> stats
) {}
```

---

## 增量刷新引擎

### IncrementalRefreshEngine

```java
public class IncrementalRefreshEngine {
    /**
     * 对基表 DML 后的增量行做刷新。
     * @param mv       物化视图定义
     * @param delta    变更的行数据（整个基表的行，不只是变更列）
     * @param op       INSERT / DELETE / UPDATE
     * @return 是否成功（false 表示部分组退化为 stale，需全量刷新）
     */
    public boolean refresh(MVDefinition mv, VectorSchemaRoot delta, DmlOperation op);
}
```

### SPJ 路径

```
INSERT:
  ① 把 delta 行注册为 ExecContext 的瞬态表（替换基表引用）
  ② 重新 plan + execute mv.querySql
  ③ 结果追加到 MV 存储表

DELETE:
  ① 同上，结果从 MV 存储表中删除匹配行（按所有输出列匹配）

UPDATE:
  ① 旧行走 DELETE 路径，新行走 INSERT 路径
```

### 聚合路径

```
INSERT:
  ① 对 delta 行按 GROUP BY 列分组，计算各组 SUM/COUNT/MIN/MAX
  ② 对每个组，在 MV 中查找现有行（按 GROUP BY 列匹配）
  ③ 存在：SUM += delta.SUM, COUNT += delta.COUNT
  ④ MIN = min(old.MIN, delta.MIN), MAX = max(old.MAX, delta.MAX)
  ⑤ 不存在：直接插入新行

DELETE:
  ① 对 delta 行按 GROUP BY 列分组，计算各组 SUM/COUNT/MIN/MAX
  ② 对每个组，在 MV 中查找现有行
  ③ SUM -= delta.SUM, COUNT -= delta.COUNT
  ④ MIN/MAX：如果被删行包含当前极值 → 标记该组 stale
  ⑤ COUNT == 0 → 删除该行

UPDATE:
  ① 旧行走 DELETE 路径，新行走 INSERT 路径
```

### MIN/MAX 极值退避

当 DELETE 删除了某组的 MIN 或 MAX 值所在行时：
- 该组 GROUP BY key 写入 `staleGroups` 集合
- 该组在 MV 中保留旧值（不更新 MIN/MAX 列）
- 下次 `REFRESH MATERIALIZED VIEW` 时，全量重算 stale 组
- 如果组内只剩 0 或 1 行，直接推算极值（无需退避）

### AVG 处理

AVG 在存储时拆为两列：`SUM(expr)` 和 `COUNT(expr)`。查询重写时由 Calcite 自动组合为 `SUM(expr)/COUNT(expr)`。

---

## DML 集成与事务

### 事务内增量刷新

```
事务内 DML (INSERT/UPDATE/DELETE)
  → MiniDbModify 写入 tx-private 存储
  → 收集 delta 数据（变更行的 VectorSchemaRoot）
  → 将 delta 追加到 TxHandle.pendingMVRefresh
  → 不立即刷新 MV

commit()
  → TransactionManager.commit(txId) — 合并 tx-private 到主存储
  → 遍历 TxHandle.pendingMVRefresh
  → 对每个条目调用 IncrementalRefreshEngine.refresh()
  → 增量刷新在主存储上进行（事务已提交，数据可见）
  → 如果增量刷新导致部分组 stale → 记录到 MVManager 内存状态
  → 不回滚事务（数据已提交）

rollback()
  → 释放 TxHandle.pendingMVRefresh 中的所有 VectorSchemaRoot
  → 不做任何刷新
```

### 非事务路径（auto-commit）

```
DML 执行完毕 → 数据已落盘 → 立即同步增量刷新 MV
```

### TxHandle 新增字段

```java
// TxHandle 中新增
private final List<MVDirtyEntry> pendingMVRefresh = new ArrayList<>();

public record MVDirtyEntry(
    String mvSchemaName,
    String mvName,
    VectorSchemaRoot delta,
    DmlOperation operation
) {}
```

### delta 数据收集

在 `MiniDbModify` 中：
- **INSERT**：写入的 batch 即为 delta
- **DELETE**：`materializeInput()` 物化的 matched 行即为 delta
- **UPDATE**：旧行 = matched 行（DELETE 路径），新行 = 构造后的 updated 行（INSERT 路径），需要两份 delta

delta 的 `VectorSchemaRoot` 在 DML 执行期间分配，commit 后由刷新引擎负责释放，rollback 后由 TxHandle close 释放。

---

## REFRESH MATERIALIZED VIEW

### 语法

```sql
REFRESH MATERIALIZED VIEW [schema.]mv_name
```

### 执行流程

```
① 解析 mv_name，获取 MVDefinition 和 TableSchema
② 检查 MV 状态：
   - staleGroups 非空 → 全量刷新
   - 整体 stale 标记 → 全量刷新
   - 否则 → no-op（MV 已最新）
③ 全量刷新：
   a. 重新 plan + execute mv.querySql → 全量结果
   b. TRUNCATE MV 存储表
   c. INSERT 全量结果到 MV 存储表
   d. 清空 staleGroups 和 stale 标记
   e. 更新统计信息
```

### 实现位置

在 `QueryExecutor.tryHandleCommand()` 中作为前缀命令拦截（类似 `ANALYZE`/`EXPLAIN`）。

---

## 查询重写（Calcite 集成）

### 注册物化视图

在 `MiniDbCalciteSchema.getTableMap()` 中，对每个物化视图：
1. 用 `MaterializedViewTable` 包装（传入定义查询的 RelNode 供 `SubstitutionVisitor` 匹配）
2. 物化视图的存储表作为普通 `MiniDbCalciteTable` 暴露，`MiniDbScan` 正常扫描

### 重写机制

Calcite 在 VolcanoPlanner 规划期间，`SubstitutionVisitor` 自动检查用户查询是否可被已注册的物化视图替代。物化视图表通常行数更少（预聚合），`getStatistic()` 返回真实行数即可让 CBO 自动选择。

### 关键点

- 物化视图的 `RelProtoDataType` 从 `MVDefinition.columns` 构建
- 定义查询的 RelNode 在 `CREATE MATERIALIZED VIEW` 时规划并保存，供 `MaterializedViewTable` 构造使用
- 聚合物化视图的 SUM/COUNT 拆分后，查询重写由 Calcite 自动处理列映射

---

## 持久化与恢复

### catalog.json

`CatalogSnapshot` 新增 `materializedViews` 字段，`JsonCatalogStore` 序列化时自动包含。

### 启动恢复

```
StorageManager.loadAll()
  → ① 读取 catalog.json → CatalogSnapshot
  → ② catalog.restore(snapshot) — 恢复 schemas/tables/views/MV定义
  → ③ 遍历 data/ 目录恢复 .arrow 文件
  → ④ 重建反向索引：对每个 MVDefinition，遍历 dependencies 建立 mvDependencyIndex
  → ⑤ MV 存储表本身就是普通 ArrowTable，恢复路径和普通表完全一致
  → ⑥ 所有 MV 初始标记为 fresh（staleGroups 是内存状态，重启后丢失）
```

### 重启后 staleness

- `staleGroups` 是运行时内存状态，重启后丢失
- 保守标记为 fresh：重启前已提交的 DML 必然已触发增量刷新（commit 后同步完成），MV 在重启后理论正确
- 用户如需确保数据一致，可手动 `REFRESH MATERIALIZED VIEW`

---

## 边界条件与约束

### 与其他 DDL 的交互

| DDL | 行为 |
|-----|------|
| `DROP TABLE t` | 拒绝，如果存在依赖的 MV（报错提示先 DROP 依赖的 MV） |
| `ALTER TABLE t DROP COLUMN c` | 拒绝，如果 c 被任何 MV 引用 |
| `ALTER TABLE t ADD COLUMN` | 允许（MV 不引用新列，无影响） |
| `ALTER TABLE t RENAME COLUMN` | 拒绝，如果被任何 MV 引用 |
| `ALTER TABLE t RENAME TO` | 拒绝，如果存在依赖的 MV（MV 定义中的表名失效） |
| `TRUNCATE TABLE t` | 触发依赖 MV 的 DELETE 路径（delta 为全表行，清空 MV 或归零聚合） |
| `DROP SCHEMA s` | 拒绝，如果 schema 下有 MV |
| `DROP VIEW v` | 允许（MV 不能依赖视图，见下方约束） |

### 创建约束

- MV 定义查询只允许引用基表，不允许引用视图或其他 MV
- MV 定义查询不能包含 `ORDER BY`（SPJ 路径，聚合路径允许）
- MV 名称不能与现有表或视图同名

### 并发

- REFRESH 期间 MV 表不可读：利用现有存储的写入隔离（LSM 的 MemTable 机制天然支持）
- 多个 DML 并发修改同一基表：各自在事务内收集 delta，串行提交

---

## 测试策略

### 单元测试

| 测试类 | 覆盖 |
|--------|------|
| `MVStructureTest` | `extractStructure` 对各种查询形状的正确提取，不支持的查询类型正确拒绝 |
| `IncrementalRefreshTest` | SPJ 增量刷新（INSERT/DELETE/UPDATE）、聚合增量刷新（SUM/COUNT/AVG/MIN/MAX）、MIN/MAX 极值退避 |
| `MVManagerTest` | 创建/删除/刷新生命周期、依赖索引正确性 |

### 集成测试

| 测试类 | 覆盖 |
|--------|------|
| `MaterializedViewTest` | 端到端：CREATE → DML 增量刷新 → 查询命中 MV → REFRESH → DROP |
| `MVTransactionTest` | 事务内 DML → commit 后 MV 正确、rollback 后 MV 不变 |
| `MVQueryRewriteTest` | 用户查询自动重写为 MV 扫描，EXPLAIN 验证计划 |
| `MVCatalogTest` | 持久化重启后 MV 恢复，catalog.json 正确 |
| `MVDDLTest` | DDL 边界：DROP TABLE 被 MV 依赖时拒绝、ALTER TABLE 拒绝等 |

---

## 实现顺序

1. **数据模型**：`MVDefinition`、`MVStructure`、`TableSchema` 扩展
2. **Catalog 扩展**：反向索引、`CatalogSnapshot` 扩展
3. **MVManager**：创建/删除/刷新生命周期、MVStructure 提取
4. **IncrementalRefreshEngine**：SPJ 路径 + 聚合路径
5. **DDL 集成**：`QueryExecutor.handleDdl` 处理物化视图节点
6. **DML 集成**：`MiniDbModify` delta 收集 + `TxHandle` 扩展 + `SessionHandler` commit 触发
7. **查询重写**：Calcite `MaterializedViewTable` 注册
8. **REFRESH** 命令
9. **information_schema** 集成
10. **持久化**：`JsonCatalogStore` 序列化 + 启动恢复
11. **测试**