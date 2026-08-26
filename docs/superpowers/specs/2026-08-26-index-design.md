# 索引(二级索引)设计

日期：2026-08-26

## 概述

为 MiniDB 引入二级索引:CREATE INDEX / DROP INDEX DDL、catalog 元数据持久化、DML 自动维护、非主键列查询加速、UNIQUE 索引约束校验、CBO 唯一键统计、EXPLAIN 显示索引使用。

**核心模型:索引 = 内部普通表**。索引数据是一张 `(索引列..., 数据主键列...)` 的表(主键 = 全部列),复用现有 LSM 存储的全部能力(持久化、compact、恢复、后台合并),查询时「索引表前缀范围扫 → 主键 getByKey 回表 → 残留条件过滤」。

## 前置约束(建索引时校验,违反即报错)

1. **被索引表必须有主键且为 LSM 存储**(`tableType != SIMPLE`):索引命中后靠 `getByKey` 回表,SimpleTable 无点查能力;无主键/`type=simple` 的表建索引直接拒绝。
2. **索引列类型限 SMALLINT/INTEGER/BIGINT/VARCHAR**:LSM 零填充 key 编码(符号位翻转定长 / 长度前缀 UTF-8)对这几类保序且往返可靠;DOUBLE/DECIMAL 的 `toString` 字典序不保数值序(`"10.0" < "1.5"`),DATE/TIMESTAMP decode 回读 String 与 LocalDate 往返不一致,BOOLEAN 边缘,均 v1 拒绝。
3. 复合索引支持:`CREATE INDEX ON t (a, b)`——索引表主键 = (a, b, 数据主键),前缀匹配天然可用。
4. NULL 索引值:LSM writePart 把 PK 列的 null 替换为空串(既有行为),索引表主键含 null 时落成 `""`;`col IS NULL` 查找搜 `""` 得超集,由 residual 条件过滤兜底(与 `col = ''` 区分靠 residual)。

## 数据模型

`minidb-common` 新增 record:

```java
public record IndexDef(String name, boolean unique, List<String> columns) {
    public IndexDef { columns = columns == null ? List.of() : List.copyOf(columns); }
}
```

`TableSchema` 新字段 `List<IndexDef> indexes`(record 追加字段,canonical 构造器 `null → List.of()`;Jackson 序列化自动兼容旧 catalog.json,旧文件无此字段读成 null 再归一)。`TableSchema` 增加便捷构造重载与 `withIndexes(...)` 副本辅助,避免改散落各处的全部构造调用点。

**索引表布局**:`data/<schema>/<table>/.indexes/<indexName>/`——索引表目录在数据表目录内部,天然获得:DROP TABLE 目录删除自动带走、RENAME TABLE 目录移动自动跟随、recoverCompaction 的表级 .bak/.tmp 恢复无需改动。

索引表不注册进 MiniDbCatalog、不进 information_schema、Calcite 不可见(用户 SQL 无法查询它)。其 TableSchema 为合成对象(列 = 索引列 + 数据主键列,类型照抄,主键 = 全部列),只存在于 IndexManager 的句柄映射中。合成 schema 会把主键列标成 NOT NULL(TableSchema canonical 构造器强制),但索引列的值运行时可为 null——LSM writePart 把 null 落成空串,
**NOT NULL 只是元数据,存储层不校验约束**,无碍。

## 模块与类

### minidb-parser

仿 `SqlAlterTable` 模式(SqlDdl 子类,`QueryExecutor.handleDdl` 的 `instanceof SqlDdl` 能命中):

- `SqlCreateIndex extends SqlDdl`:`(pos, unique, indexName(SqlIdentifier), table(SqlIdentifier), columnList(SqlNodeList))`
- `SqlDropIndex extends SqlDdl`:`(pos, ifExists, indexName, table)`

`parserImpls.ftl` 增两条:

- `SqlCreate SqlCreateIndex(Span s, boolean replace)`:供 `SqlCreate` 分支的 `[UNIQUE] INDEX name ON CompoundIdentifier (列表)`
- `SqlDrop SqlDropIndex(Span s)`:供 `SqlDrop` 分支的 `INDEX [IF EXISTS] name ON CompoundIdentifier`

### catalog

- `IndexDef` 随 `TableSchema` 走 `CatalogSnapshot`/`JsonCatalogStore` 现有序列化,零新代码。
- `MiniDbCatalog` 不加新方法(索引元数据随表整体增删改:`createTable`/`alterTable`/`dropTable` 已覆盖;renameTable 需在构造新 TableSchema 时保留 `indexes` 字段——现有 renameTable 手工重建 record,补一个字段)。

### storage

新增 **`IndexManager`**(StorageManager 内部类或平级类,持有 `BufferAllocator`/`PartFormat`/`MiniDbConfig`):

- `Map<String, Map<String, TableHandle>>` —— key = `schema.table`(小写), value = indexName → LSMTable 句柄
- `TableHandle getIndex(schemaName, tableName, indexName)`
- `TableHandle createIndex(schemaName, tableName, IndexDef)`:合成索引表 schema → `new LSMTable(...)`(目录 = `tableDir/.indexes/<name>`)
- `void dropIndex(schemaName, tableName, indexName)`:close 句柄 + 删目录
- `void populateFromTable(schemaName, tableName, IndexDef, TableHandle indexTable)`:扫数据表 → 按批提取 (索引列, 主键) 子集 → `writePart(INSERT)`
- `void onInsert(schemaName, tableName, VectorSchemaRoot batch)`:逐索引写 (索引列, 主键) 子批
- `void onDelete(schemaName, tableName, VectorSchemaRoot batch)`:逐索引 tombstone
- `void onUpdate(schemaName, tableName, VectorSchemaRoot oldBatch, VectorSchemaRoot newBatch)`:逐索引 tombstone 旧 + insert 新
- `void clearIndexes(schemaName, tableName)`(TRUNCATE 联动)
- `void dropIndexesForTable(...)`(DROP TABLE 联动,目录随数据目录删除,主要清句柄)

`StorageManager`:
- 构造 `IndexManager`,`loadAll()` 里对带 `indexes` 的表按 `.indexes` 目录重建句柄(不重建数据)
- `createTable`/`alterTable`/`renameTable`/`dropTable`/`truncateTable`/`dropSchema` 全路径联动 IndexManager
- `ExecContext.getTable` 之外新增 `ExecContext.getIndex(schemaName, tableName, indexName)`(透传 StorageManager)

### exec

`ConstraintChecker.validateInsert` 扩展:对 `schema.indexes()` 中每个 `unique` 索引,用**索引表前缀点查**校验批内与存量重复(含 null 键跳过,沿用「唯一约束允许多 null」;批内多行同键用 seen 集防重)。不走全表扫。

`MiniDbScan`:
- 新字段 `String usedIndex`(explainTerms 输出 `index=name`,EXPLAIN/EXPLAIN ANALYZE 自动可见;插桩路径无需改动)。**索引选择在 MiniDbScan 构造时完成**(构造器里有 `pushedFilter` + 经 RelOptTable 可取的 TableSchema):这样 EXPLAIN(不执行)也能显示 `index=`;`copy()` 重建时由构造器重算,无需存字段。
- 执行顺序:主键点查(现有)→ 主键范围裁剪(现有)→ **索引点查/范围**(新增)→ 全表扫描
- 索引路径:解析 `pushedFilter` AND 级联子句,遍历 `schema.indexes()`,找「全部列等值/IN 绑定,或(单列索引)范围绑定」的最优索引(绑定列数最多)→ `indexTable.scan(rangeLo, rangeHi)` 前缀范围扫 → 逐行取主键 → `getByKey` 回数据表 → 残留条件过滤 + 列投影(复用 PointLookup 的 `rowsToRoot`/`applyFilter`/`applyProject`)。**IN 点集 = 对每个候选值一次前缀扫,结果集合并**。
- 回表 miss(null)不影响正确性:残留条件兜底保证结果与全扫一致

### calcite(CBO)

`MiniDbCalciteTable.getStatistic().keys()`:把 `unique == true` 的索引列并入唯一键(与 PK/UNIQUE 同通道 → `RelMdUniqueKeys` → join 基数估计更准)。非唯一索引不改基数。

## DDL 流程

### CREATE INDEX

1. 解析出 `SqlCreateIndex` → `QueryExecutor.handleDdl` 分发新 handler
2. 校验:表存在 / 有主键 / 非 SIMPLE / 列存在 / 列类型合法 / 索引名在表内唯一 / 列未重复
3. 合成索引表 schema + `IndexManager.createIndex` 建 LSM 句柄
4. `populateFromTable` 扫数据表灌入(按 4096 行分批 writePart)
5. `catalog.alterTable(schemaName, tableName, newSchemaWithIndexes)` 落元数据(触发 catalog.json 写盘)
6. 灌入失败:删除半成品 `.indexes/<name>` 目录 + 句柄,rethrow(元数据未更新,无残留)

### DROP INDEX

1. 解析出 `SqlDropIndex` → handler;`IF EXISTS` + 不存在 → no-op,否则报错
2. `IndexManager.dropIndex`(close + 删目录)
3. `catalog.alterTable` 去掉该索引元数据

## DML 维护

`MiniDbModify` 挂钩点(数据表为 LSM 时才有索引):

- **INSERT**(`appendRows`):`validateInsert`(含 UNIQUE 索引防重)→ 写数据 → `IndexManager.onInsert`(逐索引写 (索引列, 主键) 子批)。批按 4096 行切分写。
- **UPDATE**(`lsmModify`):输入批含旧行值、构造批含新行值 → 写数据后 `onUpdate`(tombstone 旧 (旧索引值, 旧主键) + insert 新条目;主键被更新也一致)。
- **DELETE**(`lsmModify`):写 tombstone 后 `onDelete`(逐行 tombstone (索引值, 主键))。
- **TRUNCATE**:`StorageManager.truncateTable` → 数据 `clearParts` + `IndexManager.clearIndexes`。
- **DROP TABLE**:目录级删除自动带走 `.indexes`;清句柄即可。
- **RENAME TABLE**:目录移动自动带走 `.indexes`;catalog.renameTable 保留 indexes 字段;句柄按新 key 重建。

一致性:无事务(autoCommit 恒 true),先写数据后写索引,中途失败留不一致——文档注明,与现有「写数据直接落盘」精神一致。

## 查询加速细节

`WHERE` 条件形态与索引选择:

- 等值/IN:索引全部列绑定 → 索引表前缀点扫 `scan([v1..], [v1..])`(多列逐列前缀);IN 是逐值前缀扫求并集
- 单列索引范围:`col > x AND col < y` → `scan([x..], [y..])`(仅 SMALLINT/INTEGER/BIGINT/VARCHAR)
- 索引选择:主键点查优先(单 getByKey 最廉价);多索引选绑定列数最多者;绑定列含范围时只取单列索引
- residual:未绑定条件合成 AND 保留,在回表结果批上逐行求值(复用 `applyFilter`)
- 列投影:命中索引的列裁剪照常工作(回表批是全列,`applyProject` 裁剪)

## EXPLAIN

`MiniDbScan.explainTerms` 增 `index=<name>`;`ExplainExecutor`/`Instrumenter` 零改动即显示。EXPLAIN ANALYZE 的插桩行与普通 scan 相同(索引查找发生在 `execute()` 内部,不算独立节点)。

## 测试

- `minidb-parser`/server 测试:CREATE/DROP INDEX 语法正反例、表限定 `DROP INDEX idx ON t`、IF EXISTS
- DDL 生命周期:建索引灌数据正确;DROP 删元数据+目录;重启后 `loadAll` 恢复句柄与元数据(catalog.json + `.indexes` 目录,无需重建)
- 查询加速:索引命中 vs 全扫**结果一致性**(等值/IN/范围/复合/含 NULL/VARCHAR/残留条件);EXPLAIN 显示 `index=`
- DML 一致性:INSERT/UPDATE/DELETE 后索引与数据一致(查询结果与无索引基线一致;索引表内容 == 期望)
- UNIQUE:重复插入拒绝;多 NULL 允许;批内重复拒绝
- 错误路径:无主键表 / type=simple / 坏列类型(DOUBLE 等)/ 索引重名 / 缺表缺列 / 索引列重复

## 非目标(v1 明确不做)

- DATE/TIMESTAMP/DOUBLE/DECIMAL/BOOLEAN 索引列
- 覆盖索引(索引只存主键,查必回表)
- information_schema / JDBC `getIndexInfo` 索引可见性
- 索引驱动 join 重排 / 访问路径 CBO 选择(索引使用固定发生在 MiniDbScan 内)
- 非唯一索引的基数统计进 ANALYZE
- 外键列自动建索引
- SimpleTable(无主键)表建索引
