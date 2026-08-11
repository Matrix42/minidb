# Schema 支持 — Public Schema + CREATE/DROP SCHEMA + 限定名解析

日期: 2026-08-12
状态: 设计定稿

## 背景

MiniDB 的表目前没有 schema 概念，所有表以扁平方式存储于 `MiniDbCatalog`。JDBC 的 `ResultSetMetaData.getSchemaName()` 返回空字符串，不符合 JDBC 规范预期。

## 目标

1. 所有表默认 schema 为 `"public"`
2. `MiniDbResultSetMetaData.getSchemaName()` 返回正确的 schema 名称
3. 支持 `CREATE SCHEMA` / `DROP SCHEMA` DDL
4. 支持 `schema.table` 限定名解析
5. 支持 `USE SCHEMA schema_name` 切换当前 schema
6. 零协议改动（`minidb-protocol` 不动）
7. 对现有物理算子零侵入

## 设计方案

### Calcite Schema 树结构

```
root
  └── "minidb" (MiniDbCalciteSchema — schema 容器)
       ├── "public" (MiniDbCalciteSchema — 默认 schema)
       │   ├── "users" (MiniDbCalciteTable)
       │   └── ...
       └── "other" (MiniDbCalciteSchema — 用户创建)
           └── ...
```

- `MiniDbCalciteSchema` 改为: 每个 schema 一个实例，`getSubSchemaMap()` 不再需要（schema 由上层容器暴露），`getTableMap()` 返回该 schema 下的表
- 需要一个 wrapper schema（`MiniDbRootSchema` 或类似）承载所有 schema 子节点

### 查询名解析

- `SELECT * FROM users` → 搜索默认路径 `root.minidb.<currentSchema>.users`
- `SELECT * FROM other.products` → 解析 `root.minidb.other.products`
- `CalciteContext` 跟踪当前 schema，构造 `CalciteCatalogReader` 时搜索路径为 `List.of(SCHEMA_NAME, currentSchema)`

### USE SCHEMA 语法

- 字符串前缀拦截（类似 EXPLAIN/ANALYZE 的处理方式）
- 语法: `USE SCHEMA schema_name`
- 调用 `calcite.setCurrentSchema(name)` 设置当前 schema

### 数据流

```
TableSchema(schemaName="public", name="users")
  ↓
ArrowTable: 构造 Arrow Schema 时附加 metadata {"schema" → "public"}
  ↓
Arrow IPC 序列化 → 网络 → 客户端反序列化
  ↓
MiniDbResultSetMetaData: 从 root.getSchema().getCustomMetadata() 读取 "schema"
  ↓
getSchemaName() 返回 "public"
```

## 改动点

### 1. MiniDbCatalog 改造

- 内部存储: `Map<String, Map<String, TableSchema>>` (schemaName → tableName → TableSchema)
- 构造时自动创建 `"public"` schema
- 新方法: `createSchema(name)`, `dropSchema(name)`, `schemaNames()`
- 现有 `getTable(name)` / `hasTable(name)` / `tableNames()` 兼容: 委托到 `"public"` schema

### 2. TableSchema 增加 schemaName 字段

```java
public record TableSchema(String schemaName, String name, List<ColumnMeta> columns) {
    // 构造时 schemaName 在前，name 在后
}
```

所有现有构造调用处改为 `new TableSchema("public", tableName, columns)`。

### 3. QueryExecutor 扩展

- `USE SCHEMA` 前缀拦截 → 切换当前 schema
- `SqlCreateSchema` → `catalog.createSchema(name)`
- `SqlDropSchema` → `catalog.dropSchema(name)`，级联删除该 schema 下所有表
- `handleCreate(SqlCreateTable)` → 从 `SqlIdentifier` 提取 schema + table name:
  - 简单名 `users` → 当前 schema
  - 限定名 `public.users` → 指定 schema
- `handleDrop(SqlDropTable)` → 同理

### 4. CalciteContext 支持当前 schema

- 增加 `currentSchema` 字段（默认 `"public"`）
- `setCurrentSchema(name)` 方法
- `buildCatalogReader` 搜索路径改为 `List.of(SCHEMA_NAME, currentSchema)`

### 5. ArrowTable 传递 schema 元数据

```java
this.arrowSchema = new Schema(fields, Map.of("schema", schema.schemaName()));
```

### 6. MiniDbResultSetMetaData 读取元数据

```java
@Override
public String getSchemaName(int column) {
    Map<String, String> meta = root.getSchema().getCustomMetadata();
    return meta != null ? meta.getOrDefault("schema", "") : "";
}
```

### 7. StorageManager 适配

- `createTable(TableSchema)` → schema 感知
- `dropSchema(name)` → 级联删除该 schema 下所有表
- `loadAll()` → 从 Arrow schema metadata 恢复 schemaName

## 不变部分

- `Message.ArrowBatch` 不改
- `minidb-protocol` 模块不动
- 物理算子（MiniDbScan/Filter/Project/Sort/Values/Modify）不动
- 客户端不依赖任何服务端类

## 涉及文件

| 文件 | 改动 |
|------|------|
| `minidb-server/.../catalog/TableSchema.java` | 增加 `schemaName` 字段 |
| `minidb-server/.../catalog/MiniDbCatalog.java` | schema 感知存储 |
| `minidb-server/.../catalog/MiniDbCalciteSchema.java` | 每个 schema 一个实例 |
| `minidb-server/.../calcite/CalciteContext.java` | 当前 schema + 搜索路径 |
| `minidb-server/.../storage/StorageManager.java` | schema 感知 + `dropSchema` |
| `minidb-server/.../storage/ArrowTable.java` | 构造 Schema 时加 metadata |
| `minidb-server/.../exec/QueryExecutor.java` | 处理 schema DDL + USE SCHEMA |
| `minidb-jdbc/.../MiniDbResultSetMetaData.java` | 实现 `getSchemaName()` |
| (其他调 `TableSchema` 构造器的地方) | 加 `"public"` 参数 |

## 测试

- 单元测试: `MiniDbCatalog` schema 创建/删除/默认 public
- 单元测试: `QueryExecutor` 处理 `CREATE SCHEMA` / `DROP SCHEMA` / `USE SCHEMA`
- 集成测试: `schema.table` 限定名查询
- 集成测试: `MiniDbResultSetMetaData.getSchemaName()` 返回正确 schema