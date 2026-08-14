# 2026-08-14 — 元数据独立持久化 + information_schema 系统表

## 目标

1. **独立元数据持久化**:把 `TableSchema`(schema/表/列名 + 类型 + precision/scale)持久化到独立文件,解决「空表(从未 INSERT)没有 `.arrow` 数据文件、重启即丢」的问题。
2. **引入 `information_schema` 系统表**:可 `SELECT` 的只读元数据视图,暴露 `schemata`/`tables`/`columns`,与持久化 catalog 同源(都读内存 `MiniDbCatalog`)。

存储格式**先用 JSON**,但把存储抽象成接口(`CatalogStore`),后续可加 Avro 实现而不动调用方。

## 现状

- `MiniDbCatalog` 是纯内存 `ConcurrentHashMap`,已带 `addListener`/`notifyChange` 钩子(DDL 变更时触发)——这是「变更即持久化」的现成挂点。
- `StorageManager.loadAll()` 从 `data/<schema>/<table>.arrow` 反推 `TableSchema`(字段名→列名、Arrow 类型 + `"minidb.type"` 元数据→`ColumnType`、`Decimal` 的 precision/scale)。**空表无 `.arrow` 文件 → 元数据随数据一起丢**。
- Calcite schema 树:`MiniDbRootCalciteSchema` 挂在 `minidb` 下,`getSubSchemaMap()` 返回各用户 schema;`information_schema` 可作为特殊子 schema 接入。
- 执行侧 `MiniDbScan` 目前只从 `StorageManager`(ArrowTable)或递归 CTE 瞬态表读数据;`information_schema.*` 需要一条「从内存 catalog 物化成 Arrow 批次」的路径。

## 实现

### 1. 可插拔元数据存储(`storage/` 包)

```java
// 格式无关的 catalog 快照(纯领域类型,不掺序列化细节)
public record CatalogSnapshot(List<String> schemaNames, List<TableSchema> tables) {}

// 存储接口:JSON 现在,Avro 以后
public interface CatalogStore extends AutoCloseable {
    CatalogSnapshot load() throws IOException;
    void save(CatalogSnapshot snapshot) throws IOException;
}
```

- `JsonCatalogStore implements CatalogStore` → 读写 `data/catalog.json`。
- 未来 `AvroCatalogStore` → `data/catalog.avro`,接口与调用方不动。
- 位置:三个类都放 `storage/`(`CatalogSnapshot`/`CatalogStore`/`JsonCatalogStore`),与 `StorageManager` 内聚。

**JSON 结构**(Jackson `jackson-databind`,原生 record + enum 序列化;若 minidb-server 未直接依赖 Jackson 就在 pom 显式声明):

```json
{
  "schemas": ["public", "other"],
  "tables": [
    {"schema": "public", "name": "t", "columns": [
      {"name": "id", "type": "INTEGER"},
      {"name": "price", "type": "DECIMAL", "precision": 10, "scale": 2}
    ]}
  ]
}
```

- `TableSchema`/`ColumnMeta` 是 record、`ColumnType` 是 enum,Jackson 直接序列化;`precision`/`scale` 序列化为整数,未设时为 `-1`(与 `ColumnMeta` 哨兵一致,读回无损)。
- `CatalogSnapshot` 用扁平 `List<TableSchema>`(每个 `TableSchema` 自带 `schemaName`)+ 独立 `schemaNames`(覆盖「有 schema 无表」的空 schema)。

### 2. `MiniDbCatalog` 增补

- `CatalogSnapshot snapshot()`:导出当前状态(所有表 + `schemaNames()`)。
- `void restore(CatalogSnapshot)`:批量恢复,**不触发 `notifyChange`**(避免加载时把文件写回)。`public` 已在构造器里,`putIfAbsent` 自然跳过。

### 3. `StorageManager` 集成

- **构造/启动时挂 listener**:`catalog.addListener(this::persistCatalog)`,`persistCatalog()` 调 `catalogStore.save(catalog.snapshot())`。DDL(`createSchema/dropSchema/createTable/dropTable`)同步写盘——`CREATE TABLE` 后空表立即落盘,崩溃也不丢。
- **`loadAll()` 分两步**:
  1. `restoreCatalog()`:`catalog.json` 存在 → `catalogStore.load()` → `catalog.restore(snapshot)`;不存在 → 回退到「从 `.arrow` 反推」的旧逻辑(兼容旧数据目录,复用之前补类型写的 `toColumnMeta` 推断)。
  2. 遍历 `.arrow` 文件加载数据到 `ArrowTable`(该步不变;表 schema 若已由 catalog.json 恢复则不再重建,否则仍按旧逻辑反推)。
- **原子写**:`JsonCatalogStore.save` 用「临时文件 + `Files.move(..., ATOMIC_MOVE)`」+ `synchronized`,避免并发 DDL / 崩溃写坏 catalog.json。
- **保留旧路径**:`close()` 仍 `flushDirty()` 写数据文件;catalog.json 已由 listener 同步写,无需再 flush。

### 4. `information_schema` 系统表

暴露 3 张只读表,列名用标准大写(与 `MetadataExecutor` 的 JDBC 元数据一致)。所有列 NULLABLE。

**`schemata`**(标准完整列集,除 `SCHEMA_NAME` 外恒 NULL):
`CATALOG_NAME` / `SCHEMA_NAME` / `SCHEMA_OWNER` / `DEFAULT_CHARACTER_SET_CATALOG` / `DEFAULT_CHARACTER_SET_SCHEMA` / `DEFAULT_CHARACTER_SET_NAME` / `SQL_PATH`(全 VARCHAR)。

**`tables`**:
`TABLE_CATALOG`(VARCHAR,NULL)/ `TABLE_SCHEMA` / `TABLE_NAME` / `TABLE_TYPE`(VARCHAR,恒 `'BASE TABLE'`;视图落地后报 `'VIEW'`)。

**`columns`**:
`TABLE_CATALOG`(NULL)/ `TABLE_SCHEMA` / `TABLE_NAME` / `COLUMN_NAME`(VARCHAR)/ `ORDINAL_POSITION`(INTEGER,1-based)/ `DATA_TYPE`(VARCHAR,`ArrowTypes.toSqlTypeName`)/ `NUMERIC_PRECISION`(INTEGER,仅 DECIMAL/NUMERIC 填)/ `NUMERIC_SCALE`(INTEGER,仅 DECIMAL/NUMERIC 填)。

**Calcite 暴露**:新增 `calcite/MiniDbInformationSchemaCalciteSchema extends AbstractSchema`,其 `getTableMap()` 返回上述 3 张表(用 `MiniDbCalciteTable` 包固定 `TableSchema`)。`MiniDbRootCalciteSchema.getSubSchemaMap()` 追加 `"information_schema"` → 该系统 schema。**保留名 `information_schema`**:`QueryExecutor.handleCreateSchema` 拒绝创建名为 `information_schema` 的 schema(`MiniDbCatalog` 保持通用、不耦合该保留名)。

**执行**:新增 `exec/InformationSchema`(持 `MiniDbCatalog` + `BufferAllocator`),提供 `VectorSchemaRoot materialize(String tableName)`——按固定 schema 建向量,从内存 catalog 物化出行(每一行数据走 `RowVectors.writeObject` 之类写向量)。`MiniDbScan.execute` 在解析出 `schema`/`table` 后加一个分支:schema 为 `information_schema` → `new InformationSchema(ctx.storage().catalog(), ctx.allocator()).materialize(table)`,返回单批迭代器;否则走现有 storage/瞬态表路径。

### 5. 视图(VIEW)的向前兼容

`TABLE_TYPE` 是 VARCHAR,天然可装 `'VIEW'`。本次**不**给 `TableSchema` 加 kind 字段(YAGNI,视图未做),但 `InformationSchema.materialize("tables")` 写成「按表 kind 收集行」的形状——现在只有 base table 一种,视图落地时加一个 kind 分支即报 `'VIEW'`。届时再引入 kind(或独立 `ViewSchema`)。

## 波及文件

- `storage/`:`CatalogSnapshot`、`CatalogStore`、`JsonCatalogStore`(新增);`StorageManager`(loadAll/restoreCatalog + listener + 构造)。
- `catalog/`:`MiniDbCatalog`(snapshot/restore)。
- `calcite/`:`MiniDbInformationSchemaCalciteSchema`(新增);`MiniDbRootCalciteSchema`(getSubSchemaMap 加 information_schema)。
- `exec/`:`InformationSchema`(新增)。
- `plan/physical/`:`MiniDbScan`(information_schema 物化分支)。
- `minidb-server/pom.xml`:显式 Jackson 依赖(若未直接声明)。

## 测试

- **`JsonCatalogStore` round-trip**:save → load → snapshot 等价(含 DECIMAL 的 precision/scale、空 schema、表名大小写)。
- **空表重启存活**:`CREATE TABLE`(不 INSERT)→ 关闭 → 重载 → 表存在且列/类型/precision/scale 正确(这是本次核心回归)。
- **DDL 变更即落盘**:`CREATE TABLE` 后 `data/catalog.json` 立即更新;`DROP TABLE` 后同样。
- **旧目录回退**:无 `catalog.json` 的数据目录 → 旧「从 .arrow 反推」行为不变(现有 `StorageManagerTest` 保持绿)。
- **information_schema 查询**:`SELECT * FROM information_schema.tables` / `.columns` / `.schemata` 返回正确行;`.columns` 对 DECIMAL 列报 `NUMERIC_PRECISION/SCALE`、其余 NULL;`schemata` 报 `SCHEMA_NAME` 其余 NULL。
- **只读**:`INSERT INTO information_schema.tables ...` 失败。
- **保留名**:`CREATE SCHEMA information_schema` 失败。

## 坑(实施时注意,最终汇总进 CLAUDE.md)

1. `restore` 必须**不触发** `notifyChange`,否则 `loadAll` 会把刚读到的 catalog.json 原样写回,且加载期间 listener 可能未就绪/重复写。用 `restore` 批量不通知。
2. `MiniDbScan` 的 information_schema 分支要在**解析出 schema 之后**加,注意限定名段数(坑 17):`information_schema.tables` 经 Calcite 是 `["minidb","information_schema","tables"]`(3 段),schema=倒数第二段。
3. `schemata` 等大量 NULL 列:Arrow VARCHAR 向量默认全 null,物化时只对 `SCHEMA_NAME` 等有值列写值,其余列保持 null 并 `setValueCount(rows)`。
4. Jackson 序列化 record/enum 需较新 jackson-databind(2.12+);Calcite 1.42 传递的 Jackson 版本可能够用,但显式声明更稳。`ColumnType` 序列化为 name 字符串,读回按 name 匹配。
5. 原子写:并发 DDL 可能同时 `notifyChange` → `save`,`save` 需 `synchronized` + 临时文件 rename,避免写坏或交叉写。
6. 旧目录首次升级:没有 catalog.json 时回退推断,**不会**自动生成 catalog.json(直到首次 DDL 变更才写);可接受,不必在 loadAll 时强制写回。

## 不做

- 不做 `table_constraints` 等约束表(玩具库无约束)。
- information_schema 只读,不接受 INSERT/UPDATE/DELETE。
- 不迁现有 `.stats`(仍 Java 序列化);如需统一,另开任务。
- 不引入 kind 字段 / `ViewSchema`(视图未做)。
- 不做 Avro 实现(仅留 `CatalogStore` 接口)。
