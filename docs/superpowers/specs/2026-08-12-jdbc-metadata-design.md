# JDBC DatabaseMetaData 元数据查询设计

日期: 2026-08-12
状态: 已确认,待实现
范围: 实现 `MiniDbDatabaseMetaData` 的 `getSchemas` / `getTables` / `getColumns`

## 目标

JDBC 客户端的 `DatabaseMetaData.getSchemas()` / `getTables(...)` / `getColumns(...)` 当前抛 `SQLFeatureNotSupportedException`。本设计让它们返回符合 JDBC 规范列结构、反映服务端 `MiniDbCatalog` 真实状态的 `ResultSet`,使 DBeaver/Squirrel 等 JDBC 工具能列出 schema/表/列。

## 关键决策(已与用户确认)

1. **实现路径**: 在 `minidb-protocol` 新增专用元数据请求消息(不复用 `ExecuteRequest`+伪SQL)。
2. **协议消息形态**: 三条独立请求消息(`SchemasRequest`/`TablesRequest`/`ColumnsRequest`),而非单一统一请求。
3. **catalog 语义**: MiniDB 无 catalog 概念(`getCatalog()=null`,只有 schema)。`getSchemas` 的 `TABLE_CATALOG` 列与 `getTables`/`getColumns` 的 `TABLE_CAT` 列恒为 `null`;入参 `catalog` 忽略。
4. **NULLABLE**: MiniDB 列全可空(CREATE TABLE 无 NOT NULL 语法),`NULLABLE` 列恒为 `1`(`columnNullable`),`IS_NULLABLE` 恒为 `"YES"`。
5. **列集完整性**: `getColumns` 按 JDBC 规范返回全部 24 列(无语义的列填合理默认);`getTables` 返回全部 10 列;`getSchemas` 返回 2 列。
6. **types 参数**: MiniDB 只有基表。`TABLE_TYPE` 列恒输出 `"TABLE"`;`types` 入参 `null`/空→返回所有,含 `"TABLE"`(大小写不敏感)→返回所有,含其他→空集。
7. **测试**: 真服务端到端集成测试(照 `PersistenceTest` 模式:`MiniDbServer.start(0, tempDir)`,JDBC 走真网络)。

## 架构与数据流

三层改动,自底向上:

### 1. minidb-protocol(最小增量)

新增 3 个 `MessageType` 常量:
- `SCHEMAS_REQUEST = 0x12`
- `TABLES_REQUEST = 0x13`
- `COLUMNS_REQUEST = 0x14`

新增 3 个 `Message` record:
- `SchemasRequest(long requestId, String schemaPattern)`
- `TablesRequest(long requestId, String schemaPattern, String tableNamePattern, String[] types)`
- `ColumnsRequest(long requestId, String schemaPattern, String tableNamePattern, String columnNamePattern)`

`MessageEncoder` / `MessageDecoder` 各加 3 个分支。字符串字段用与 `ExecuteRequest` 一致的编码(UTF-8 字节 + 长度前缀)。`String pattern` 的 `null` 编码:长度前缀写 `-1`(解码时 `-1` 还原 `null`,区分于空串 `""` 写 `0`)。`TablesRequest.types`:`null` 写 count=`-1`;空数组写 count=`0`;非空写 count=`n` + 逐个字符串。

**响应不新增任何消息**:元数据结果集就是 Arrow 行,复用现有 `Message.ArrowBatch`(服务端 `sendRows` 路径)。错误兜底复用 `Message.ExecuteResponse.error`。

### 2. minidb-server(物化逻辑)

新增 `exec/MetadataExecutor`(参考 `ExplainExecutor` 外挂方式,零侵入算子/规划器),持 `(catalog, allocator)`:

- `VectorSchemaRoot schemas(String schemaPattern)` — 遍历 `catalog.schemaNames()`,按 `schemaPattern` 过滤,物化 2 列。
- `VectorSchemaRoot tables(String schemaPattern, String tableNamePattern, String[] types)` — 遍历所有 schema 的所有表,按 schema 名/表名过滤,按 `types` 过滤,物化 10 列。
- `VectorSchemaRoot columns(String schemaPattern, String tableNamePattern, String columnNamePattern)` — 遍历表×列,按 schema/表/列名过滤,物化 24 列。

物化模式参考 `MiniDbValues.execute()`:`FieldVector.createVector(allocator)` + `setInitialCapacity(n)` + `allocateNew()` + `setSafe(row, val)` + `setValueCount(n)` + `VectorSchemaRoot.of(...)`,但类型仅限元数据列所需(VARCHAR/INT/SMALLINT 等)。

`SessionHandler.channelRead0` 加 3 个分支,各调 `MetadataExecutor` 得 `VectorSchemaRoot`,走现有 `sendRows(ctx, requestId, root)` 方法,随后 `root.close()`(照 `Rows` 路径)。

`MetadataExecutor` 仅依赖 catalog(元数据全在 catalog),不依赖 storage/stats。

### 3. minidb-jdbc(客户端翻译)

`MiniDbClient` 加 3 个方法:`schemas(...)` / `tables(...)` / `columns(...)`,各发对应请求、按 `requestId` 路由到 `CompletableFuture`、复用 `readArrow` 解码 `ArrowBatch` 成 `VectorSchemaRoot` 返回。复用 `execute` 的 timeout/断连/`SQLException` 包装逻辑(抽公共私有方法 `sendRequest`/`awaitResponse`)。

`ResponseCollector` 无需改(`ArrowBatch` 路由已通用,按 `requestId`;`ExecuteResponse.error` 已分流抛异常)。

`MiniDbDatabaseMetaData` 三个方法各建一个 `MiniDbStatement`、调 `client` 对应方法、包 `MiniDbResultSet`。`getSchemas()` 无参重载调 `getSchemas(null, null)`。

### 数据流

```
getMetaData().getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)
  → MiniDbClient.columns(...)
  → Message.ColumnsRequest(requestId, schemaPattern, tableNamePattern, columnNamePattern)
  → wire → SessionHandler
  → MetadataExecutor.columns(schemaPattern, tableNamePattern, columnNamePattern)
  → catalog.getTable(schema, table).columns()  // 遍历所有 schema/table
  → VectorSchemaRoot(24 列, 按 schema/table/ordinal 排序)
  → sendRows → Message.ArrowBatch
  → wire → client.readArrow → MiniDbResultSet
```

## JDBC 列结构与默认值

所有元数据列用 `FieldType.nullable(...)`(与现有 `ArrowTypes.field` 一致)。

### getSchemas(2 列)

| 列 | Arrow 类型 | 值 |
|---|---|---|
| `TABLE_SCHEM` | VARCHAR | schema 名(catalog 内部存储即小写) |
| `TABLE_CATALOG` | VARCHAR | `null` |

注意:`getSchemas` 规范列名是 `TABLE_SCHEM` + `TABLE_CATALOG`;`getTables`/`getColumns` 才是 `TABLE_CAT`。MiniDB 无 catalog 概念,两者值均恒 `null`。

### getTables(10 列,JDBC 规范)

| 列 | 值 |
|---|---|
| `TABLE_CAT` | `null` |
| `TABLE_SCHEM` | schema 名 |
| `TABLE_NAME` | 表名 |
| `TABLE_TYPE` | 恒 `"TABLE"` |
| `REMARKS` | `null` |
| `TYPE_CAT` | `null` |
| `TYPE_SCHEM` | `null` |
| `TYPE_NAME` | `null` |
| `SELF_REFERENCING_COL_NAME` | `null` |
| `REF_GENERATION` | `null` |

### getColumns(24 列,JDBC 规范)

| 列 | Arrow 类型 | 值 |
|---|---|---|
| `TABLE_CAT` | VARCHAR | `null` |
| `TABLE_SCHEM` | VARCHAR | schema 名 |
| `TABLE_NAME` | VARCHAR | 表名 |
| `COLUMN_NAME` | VARCHAR | 列名 |
| `DATA_TYPE` | INT | `java.sql.Types.*`(见类型映射) |
| `TYPE_NAME` | VARCHAR | `INTEGER`/`BIGINT`/`DOUBLE`/`VARCHAR`/`BOOLEAN`/`DATE`/`TIMESTAMP` |
| `COLUMN_SIZE` | INT | `0`(MiniDB 无精度/长度概念,VARCHAR 也无长度限制) |
| `BUFFER_LENGTH` | INT | `0` |
| `DECIMAL_DIGITS` | INT | `0`(无 scale) |
| `NUM_PREC_RADIX` | INT | `10`(整数类型)/ `null`(其他,规范允许) |
| `NULLABLE` | INT | `1`(`columnNullable`,决策已定) |
| `REMARKS` | VARCHAR | `null` |
| `COLUMN_DEF` | VARCHAR | `null`(无 DEFAULT 语法) |
| `SQL_DATA_TYPE` | INT | `null` |
| `SQL_DATETIME_SUB` | INT | `null` |
| `CHAR_OCTET_LENGTH` | INT | `null` |
| `ORDINAL_POSITION` | INT | 从 1 起的列序号 |
| `IS_NULLABLE` | VARCHAR | `"YES"`(对齐 NULLABLE=1) |
| `SCOPE_CATALOG` | VARCHAR | `null` |
| `SCOPE_SCHEMA` | VARCHAR | `null` |
| `SCOPE_TABLE` | VARCHAR | `null` |
| `SOURCE_DATA_TYPE` | SMALLINT | `null` |
| `IS_AUTOINCREMENT` | VARCHAR | `"NO"` |
| `IS_GENERATEDCOLUMN` | VARCHAR | `"NO"` |

### 类型映射(对齐 MiniDbResultSetMetaData,避免客户端元数据/列查询不一致)

| ColumnType | DATA_TYPE (java.sql.Types) | TYPE_NAME |
|---|---|---|
| INTEGER | INTEGER (4) | `INTEGER` |
| BIGINT | BIGINT (-5) | `BIGINT` |
| DOUBLE | DOUBLE (8) | `DOUBLE` |
| VARCHAR | VARCHAR (12) | `VARCHAR` |
| BOOLEAN | BOOLEAN (16) | `BOOLEAN` |
| DATE | DATE (91) | `DATE` |
| TIMESTAMP | TIMESTAMP (93) | `TIMESTAMP` |

## 过滤语义与服务端逻辑

### 过滤(MetadataExecutor)

- `schemaPattern` / `tableNamePattern` / `columnNamePattern`: `null`→不过滤;`""`→只匹配空名(无匹配);含 `_`/`%`→SQL LIKE 语义(`_`=单字符,`%`=任意序列);其余→全等匹配。用 `java.util.regex` 把 LIKE pattern 转正则。
- `types`(getTables): `null`/空数组→不过滤;含 `"TABLE"`(大小写不敏感)→返回所有;含其他类型→空集。
- `catalog` 入参:忽略(决策已定)。

### 排序

结果按 `schema`、`table`、`ORDINAL_POSITION` 排序(JDBC 规范建议,工具依赖)。schema 遍历用 `catalog.schemaNames()`(已是列表,排序保证稳定顺序);表遍历用 `catalog.tableNames(schema)`;列用 `TableSchema.columns()` 原序。

### LIKE→正则转换

把 SQL LIKE pattern 转为正则:转义正则元字符,`_`→`.`,`%`→`.*`,大小写不敏感(`VARCHAR` schema/表/列名在 MiniDB 内部全小写,pattern 也小写化匹配)。`null` pattern 直接跳过该字段过滤。

## 错误处理与资源管理

### 客户端

`client.schemas()/tables()/columns()` 复用 `execute` 的 timeout/断连/`SQLException` 包装。抽公共私有方法 `sendRequest(Message req)` + `awaitResponse(requestId)`,与现有 `execute` 共用 `pending` map 和 `ResponseCollector`。收到 `ExecuteResponse.error`→抛 `SQLException`;收到 `ArrowBatch`→`readArrow` 解码成 `VectorSchemaRoot` 返回。

### 服务端

`MetadataExecutor` 不抛受检异常。`SessionHandler` 已有 try-catch 兜底:异常→`Message.ExecuteResponse.error(requestId, message)`。客户端 `ResponseCollector` 已按消息类型分流(`ExecuteResponse` 抛异常,`ArrowBatch` 解码),无需改。

### 资源管理

`MetadataExecutor` 物化的 `VectorSchemaRoot` 由 `SessionHandler` 在 `sendRows` 之后调用 `root.close()`(照现有 `QueryResult.Rows` 路径:`sendRows(ctx, req.requestId(), rows.data()); rows.data().close();`——非 finally,pre-existing,所有 Rows 路径都这样)。客户端 `readArrow` 已 copy 出独立 root,原始 reader 关闭。`MiniDbResultSet.close()` 关 root。

## 测试

真服务端到端集成测试,新增 `minidb-jdbc/src/test/java/com/minidb/jdbc/DatabaseMetaDataTest.java`,照 `PersistenceTest` 模式:

```
MiniDbServer server = new MiniDbServer();
server.start(0, tempDir);
url = "jdbc:minidb://127.0.0.1:" + server.port();
try (Connection c = DriverManager.getConnection(url);
     Statement s = c.createStatement()) {
    s.execute("CREATE SCHEMA other");
    s.execute("CREATE TABLE public.users (id INTEGER, name VARCHAR)");
    s.execute("CREATE TABLE other.t (a BIGINT, b BOOLEAN, c DATE)");

    DatabaseMetaData md = c.getMetaData();
    // getSchemas: 断言含 public, other
    // getTables(null, null, null, null): 断言含 users(public), t(other)
    // getTables(null, "public", null, null): 只 users
    // getTables(null, null, "%", null): 通配
    // getTables(null, null, null, new String[]{"VIEW"}): 空
    // getColumns(null, null, null, null): 断言 users.id/users.name/t.a/t.b/t.c 含 TYPE_NAME/ORDINAL_POSITION
    // getColumns(null, "public", "users", null): 只 users 两列
    // getColumns(null, null, null, "%name%"): 只 name 列
}
```

断言关系/子集而非精确行集顺序依赖(但 getSchemas/getTables/getColumns 规范要求排序,故可断言有序或断言包含)。

## 不在本范围

- `getCatalogs()` / `getTableTypes()` 仍抛 `SQLFeatureNotSupportedException`(用户未要求;`getTableTypes` 可后续单独加,仅需一行 `"TABLE"`)。
- `getPrimaryKeys` / `getIndexInfo` / `getImportedKeys` 等仍不支持。
- `NUM_PREC_RADIX` 对非整数类型的 `null` 值(JDBC 工具一般容忍)。
- 协议不新增分页(元数据结果集小,一次性物化,符合现有"结果集客户端一次性物化"定位)。
