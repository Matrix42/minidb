# MiniDB 设计文档

日期：2026-08-09

基于 Apache Calcite 的微型数据库服务端：自研 JDBC 驱动 + 自定义网络协议（Netty），数据传输与存储格式为 Apache Arrow，支持建表、插入、查询（Scan / Filter / Project / Sort / Limit）。

## 1. 需求范围

**支持**

- SQL：CREATE TABLE、DROP TABLE、INSERT INTO ... VALUES（多行）、INSERT INTO ... SELECT、SELECT
- SELECT 能力：列投影、表达式、WHERE 过滤、ORDER BY、LIMIT
- 列类型：INTEGER、BIGINT、DOUBLE、VARCHAR、BOOLEAN、DATE、TIMESTAMP
- 内存存储 + Arrow IPC 文件持久化

**明确不支持**

- 事务、Savepoint、游标、连接池、结果集分页（结果一次取完）
- INSERT ... SELECT 以外的复杂 DML（UPDATE / DELETE）
- 服务端预编译（PreparedStatement 为简单实现）
- 基于代价的查询优化

## 2. 总体架构

```
+----------------------+          +-----------------------------+
| 应用 (java.sql.*)    |          | MiniDB Server               |
|                      |          |                             |
| minidb-jdbc          |  TCP /   | Netty pipeline              |
| JDBC 驱动 +          |  Arrow   |   帧编解码 + 会话处理       |
| Netty 同步客户端     |<-------->| SQL 服务                    |
|                      |  帧      |   Calcite parse / validate  |
+----------------------+          |   VolcanoPlanner + 规则     |
                                  | 向量化执行器                |
                                  |   Scan/Filter/Project/Sort  |
                                  | Catalog + Storage           |
                                  |   Arrow 批次内存表          |
                                  |   Arrow IPC 文件持久化      |
                                  +-----------------------------+
```

### Maven 模块

| 模块 | 职责 | 依赖 |
|---|---|---|
| `minidb-protocol` | 协议消息 POJO + Netty 帧/消息编解码器 | Netty、Arrow |
| `minidb-server` | Catalog、存储、Calcite 规划、执行器、Netty 服务端 | protocol、Calcite、Arrow、Netty |
| `minidb-jdbc` | `java.sql.*` 驱动实现 + Netty 同步客户端 | protocol |

### 会话模型

- 每个 TCP 连接 = 一个会话，会话内 SQL 串行执行
- 会话绑定单线程执行，避免并发写表，符合 JDBC 同步语义
- 无认证

## 3. Wire Protocol

### 连接流程

TCP 建立后客户端发 `Handshake`（协议魔数 + 版本号），服务端回 `HandshakeAck`，之后进入消息循环。

### 帧格式

用 Netty `LengthFieldBasedFrameDecoder` 处理粘包：

```
magic 2B | msgType 1B | payload length 4B | payload 变长
```

### 消息类型

客户端 → 服务端：

| 消息 | 载荷 |
|---|---|
| `Handshake` | 魔数 + 协议版本 |
| `ExecuteRequest` | requestId(8B) + SQL 字符串(UTF-8 长度前缀) |
| `CloseRequest` | 无载荷，关闭会话 |

服务端 → 客户端：

| 消息 | 载荷 |
|---|---|
| `HandshakeAck` | 协议版本 |
| `ExecuteResponse` | requestId + status(OK/ERROR) + 错误信息 |
| `ArrowBatch` | requestId + lastBatch 标志 + Arrow IPC 消息字节 |
| `UpdateCount` | requestId + 受影响行数 |

### 查询结果传输

- SELECT 第一帧：Arrow IPC schema message（结果列名 + 类型）
- 后续帧：Arrow record batch，一帧一批次
- `lastBatch=true` 表示结果结束
- DDL / DML 走 `UpdateCount`，不发 Arrow 数据
- 错误发 `ExecuteResponse(status=ERROR)`，不产生数据帧

### 设计要点

一条 SQL 对应一组帧，requestId 防止串包。Arrow 批次原样透传，不二次封装，驱动端直接复用 Arrow IPC 读取器。

## 4. Calcite 集成与 Volcano 规划器

### SQL 处理流水线

```
SQL 字符串
  -> SqlParser.parseStmt      解析，出错带行列位置
  -> SqlValidator             类型检查，依赖 Catalog schema
  -> SqlToRelConverter        转成 RelNode 逻辑树
  -> VolcanoPlanner           应用 ConverterRule，转物理算子
  -> 执行器树                 物理算子即可执行，直接调用
```

### 关键组件

- **Schema / Table**：实现 Calcite `Schema` + `Table` 接口；`MiniDbTable` 向 Calcite 暴露 RelDataType 与统计信息；DDL 后刷新 schema
- **Convention**：自定义 `Convention.MINIDB`，所有物理算子归属该 convention
- **ConverterRule**（每类逻辑算子一条）：
  - `LogicalTableScan -> MiniDbScan`
  - `LogicalFilter -> MiniDbFilter`
  - `LogicalProject -> MiniDbProject`
  - `LogicalSort -> MiniDbSort`
  - `LogicalValues -> MiniDbValues`（INSERT 的 VALUES 常量行）
  - `LogicalTableModify -> MiniDbModify`（INSERT/DML 入口）
- **物理算子即执行器**：每个 `MiniDb*` RelNode 自己实现 `execute()`，返回批次迭代器；规划器输出的根节点直接执行，无二次翻译层
- **表达式求值**：`RexInterpreter` 在 Arrow 向量上逐批求值 RexNode 表达式树（列向量进、列向量出）。支持：比较（= < > <= >= <>）、逻辑（AND/OR/NOT）、算术（+ - * /）、字面量
- **INSERT 处理**：`LogicalTableModify` 子树为 `MiniDbValues`（VALUES）或查询子树（INSERT...SELECT），执行时把子树输出追加到目标表

## 5. 存储层与持久化

### 内存布局

每张表 = 一个 `ArrowTable`：

- 元数据：表名 + `VectorSchemaRoot` 形式的 schema
- 数据：`List<VectorSchemaRoot>`，每个元素一个 Arrow 批次，追加写入不合并
- 批次上限 4096 行，超出切分
- INSERT 写入新批次追加；SELECT 扫描遍历批次

### Catalog

`MiniDbCatalog` 维护 `Map<String, ArrowTable>`，提供建表 / 删表 / 查表，同时作为 Calcite `SchemaPlus` 后端。读写锁保护，防止会话间读写交错。

### 类型映射（SQL -> Arrow）

| SQL 类型 | Arrow 向量 |
|---|---|
| INTEGER | IntVector(32) |
| BIGINT | BigIntVector |
| DOUBLE | Float8Vector |
| VARCHAR | VarCharVector |
| BOOLEAN | BitVector |
| DATE | DateDayVector |
| TIMESTAMP | TimeStampMilliVector |

NULL 用 Arrow validity bitmap 表达。

### 持久化

- 数据目录可配置，默认 `./data`
- 每张表一个文件：`data/<table>.arrow`，Arrow IPC FileWriter 写出（schema + 所有批次）
- 保存时机：DDL（建表 / 删表）立即落盘；INSERT 脏标记，服务端正常关闭时统一 flush（避免每次插入重写整表）
- 启动时扫描 `data/`，ArrowFileReader 加载所有表
- 崩溃时未 flush 的插入会丢，文档注明（本项目可接受）
- DROP TABLE：内存移除 + 删除文件

## 6. JDBC 驱动

### 驱动注册

- 实现 `java.sql.Driver`，URL 格式：`jdbc:minidb://host:port`
- `META-INF/services/java.sql.Driver` SPI 注册

### 连接与生命周期

- `MiniDbConnection` 持有 Netty 同步客户端通道，构造时完成握手
- `close()` 发 `CloseRequest` 并关闭通道
- autoCommit 恒为 true；`getMetaData()` 返回基本 `DatabaseMetaData`

### Statement 执行

- `execute(sql)` 发 `ExecuteRequest` 阻塞等响应
- SELECT：收 schema + 批次帧，包成 `ResultSet`
- DML/DDL：收 `UpdateCount`，`getUpdateCount()` 返回行数
- PreparedStatement：客户端实现——`setObject` 把参数转成 SQL 字面量（字符串做引号转义）替换 `?` 占位符后发完整 SQL，不做服务端预编译

### ResultSet 从 Arrow 读取

- 收齐所有批次后逐批游标遍历（当前批次 + 行号，`next()` 跨批次推进）
- `getInt/getString/getDouble/getBoolean/getDate/getTimestamp/getObject` 从 Arrow 向量取值，`wasNull()` 查 validity bit
- 列元数据来自 schema 帧

### 客户端网络层

- Netty 通道 + `CompletableFuture` 把异步响应转同步等待
- 帧编解码器与 protocol 模块共用
- 按 requestId 匹配响应，默认 30s 超时抛 `SQLException`

## 7. 错误处理

服务端任何异常都不让连接挂死，一律转错误响应发回：

- SQL 解析/校验错误：Calcite 异常带行列位置，转 `SQLException` 并保留位置信息
- 运行时错误（表不存在、类型不匹配、除零等）：执行器抛异常，服务端捕获 -> `ExecuteResponse(status=ERROR)`
- 网络错误：驱动端超时抛 `SQLException`；服务端对端断开时清理会话
- Arrow 缓冲区：统一 BufferAllocator 层级（server root + 每连接 child），会话关闭时强制释放并检查泄漏

## 8. 测试策略

- 单元测试：RexInterpreter 表达式求值、SQL/Arrow 类型映射、协议编解码器（粘包/拆包）
- 集成测试（核心）：真实服务端（随机端口）+ 自研 JDBC 驱动走完整链路——建表、插入、查询、WHERE、ORDER BY、LIMIT、INSERT...SELECT、DROP，断言结果
- 持久化测试：写入 -> 重启服务端 -> 数据还在
- 错误路径：坏 SQL、查询不存在的表、连接关闭后执行

### 验收场景

```java
Connection c = DriverManager.getConnection("jdbc:minidb://localhost:8899");
Statement s = c.createStatement();
s.execute("CREATE TABLE t (id INT, name VARCHAR, ts TIMESTAMP)");
s.executeUpdate(
    "INSERT INTO t VALUES (1, 'a', TIMESTAMP '2026-01-01 00:00:00')");
ResultSet rs = s.executeQuery(
    "SELECT id, name FROM t WHERE id > 0 ORDER BY id LIMIT 10");
```

## 9. 技术选型

- Java 17（环境已有 azul-17）
- Apache Calcite（core）：解析、校验、Volcano 规划
- Apache Arrow（Java）：内存列式 + IPC 读写
- Netty：服务端 pipeline + 客户端同步封装
- Maven 多模块，用 Maven Wrapper（环境未装 Maven，wrapper 自动下载）
