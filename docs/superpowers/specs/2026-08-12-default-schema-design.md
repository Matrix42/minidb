# 默认 Schema 支持 — Public Schema

日期: 2026-08-12
状态: 设计定稿

## 背景

MiniDB 的表目前没有 schema 概念，所有表以扁平方式存储于 `MiniDbCatalog`。JDBC 的 `ResultSetMetaData.getSchemaName()` 返回空字符串，不符合 JDBC 规范预期。

## 目标

1. 所有表默认 schema 为 `"public"`
2. `MiniDbResultSetMetaData.getSchemaName()` 返回正确的 schema 名称
3. 零协议改动（`minidb-protocol` 不动）
4. 对现有算子零侵入

## 设计方案

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

### 改动点

#### 1. TableSchema 增加 schemaName 字段

```java
public record TableSchema(String schemaName, String name, List<ColumnMeta> columns) {
    // 构造时 schemaName 在前，name 在后
}
```

所有现有构造调用处改为 `new TableSchema("public", tableName, columns)`。

#### 2. ArrowTable 传递 schema 元数据

```java
this.arrowSchema = new Schema(fields, Map.of("schema", schema.schemaName()));
```

#### 3. MiniDbResultSetMetaData 读取元数据

```java
@Override
public String getSchemaName(int column) {
    Map<String, String> meta = root.getSchema().getCustomMetadata();
    return meta != null ? meta.getOrDefault("schema", "") : "";
}
```

### 不变部分

- `Message.ArrowBatch` 不改
- `minidb-protocol` 模块不动
- 物理算子（MiniDbScan/Filter/Project/Sort/Values/Modify）不动
- 客户端不依赖任何服务端类

## 涉及文件

| 文件 | 改动 |
|------|------|
| `minidb-server/.../catalog/TableSchema.java` | 增加 `schemaName` 字段 |
| `minidb-server/.../storage/ArrowTable.java` | 构造 Arrow Schema 时附加 metadata |
| `minidb-jdbc/.../MiniDbResultSetMetaData.java` | 实现 `getSchemaName()` |
| (其他调 `TableSchema` 构造器的文件) | 增加 `"public"` 参数 |

## 测试

- 单元测试：验证 `TableSchema` 构造时 schemaName 正确
- 集成测试：验证 `MiniDbResultSetMetaData.getSchemaName()` 返回 `"public"`