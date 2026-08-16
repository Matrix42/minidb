# 结果集服务端分页设计

日期: 2026-08-16
状态: 已确认,待实现
范围: 实现结果集服务端分页(游标式、拉取式),JDBC `Statement.setFetchSize(n)` 落地

## 目标

当前结果集从服务端到客户端是**一次性全量物化**:`QueryExecutor.executeQuery` 把 `BatchIterator` 全量拉完合并成一个 `VectorSchemaRoot`,序列化成**单条** `ArrowBatch`(且 `lastBatch` 恒为 `true`);客户端 `MiniDbResultSet` 持有这整棵 root,`next()` 纯内存游标。大结果集会同时打爆服务端与客户端内存。

本设计引入**服务端游标(拉取式分页)**:服务端不再立即物化,而是保留 `BatchIterator`,按客户端请求逐页返回定长行。客户端 `next()` 读到本页底时向服务端拉下一页,使客户端内存始终只占一页,防止 OOM。

## 关键决策(已与用户确认)

1. **模型**: 服务端游标(拉取式)。客户端 `setFetchSize(n)` 后,`ResultSet.next()` 读到页底时发 `FetchRequest` 拉下一页;服务端持有游标。**不是**推送式(服务端边执行边推)也**不是**仅切块(全量物化后切)。
2. **粒度**: 固定行数分页。`setFetchSize(n)` → 每页恰好 ≤n 行,服务端在批边界间切片重块(真 JDBC fetchSize 语义)。分页主要省**客户端 + 网络**内存;服务端内存不因分页显著下降——eager 算子(Sort/Aggregate/Join)首次 `next()` 已物化整棵 root,lazy 算子(Scan/Filter/Project)也因迭代器在 `close()` 前累积所有产出批而不省(见坑 5/25 的批所有权模型)。
3. **默认分页**: `fetchSize=0`(JDBC 默认)也分页,客户端把它解析为固定默认页大小 `DEFAULT_FETCH_SIZE = 4096`(对齐 `ArrowTable.MAX_BATCH_ROWS`)。小结果集(≤4096 行)仍单条 `ArrowBatch` 返回(`lastBatch=true`),一次往返拿全、零性能损失;大结果集逐页流式,防客户端 OOM。
4. **游标 id**: 复用 `ExecuteRequest` 的 `requestId`(客户端生成的 per-connection 单调递增 id,天然唯一)。
5. **首页随 execute 返回**: `ExecuteRequest` 立即回第一页(而非先只建游标再拉),保持「单往返拿到首行 + 列元数据」。
6. **入口**: `Statement.setFetchSize(n)`(含 PreparedStatement,继承自动生效)。`ResultSet.setFetchSize` 暂保持 no-op。

## 架构与数据流

三层改动,自底向上:

### 1. minidb-protocol(最小增量)

- `Message.ExecuteRequest` 增加 `int fetchSize` 字段:`(requestId, sql)` → `(requestId, sql, fetchSize)`。协议里**不再有「全量」语义**:fetchSize 恒为具体页大小(客户端把 `setFetchSize(0)` 解析成默认页,不下发 0)。
- 新增两个 `MessageType` byte 常量:
  - `FETCH_REQUEST`
  - `CLOSE_CURSOR_REQUEST`
- 新增两个 `Message` record:
  - `FetchRequest(long requestId, long cursorId, int maxRows)` — 从游标拉一页(≤maxRows 行)。
  - `CloseCursorRequest(long cursorId)` — 客户端提前释放游标,无响应(发完即忘)。
- `MessageEncoder` / `MessageDecoder` 各加对应分支(`ExecuteRequest` 多编码一个 int,新增两个消息)。
- **响应不新增任何消息**:分页结果复用现有 `Message.ArrowBatch`,其已有 `lastBatch` 字段即「游标最后一页」标志。错误兜底复用 `Message.ExecuteResponse.error`。

### 2. minidb-server(游标化执行)

**QueryExecutor**:
- 新增 `executeCursor(sql, currentSchema)`:DQL SELECT 返回新变体 `QueryResult.Cursor(CursorHandle)`(**不消费**迭代器),DDL/DML/UseSchema 路径与 `execute` 相同。SessionHandler(生产)走此入口。
- 新增 `record CursorHandle(BatchIterator it, ExecContext ctx, Schema schema)`:复用现有 `planner.plan` + `MiniDbRel.execute(ctx)` 但**不拉取**;`ctx` 随游标存活(迭代器 lazy 拉取时要用,含 allocator 与瞬态表状态);`schema` 从 `plan.getRowType()` 派生,供空结果页构建。`CursorHandle.materialize()` 全量拉取合并成一个 root(供测试路径用)。
- 现有 `execute(sql, currentSchema)` **保留**:委托 `executeCursor`,若得 `Cursor` 则 `handle.materialize()` 成 `QueryResult.Rows` 返回。这样 30 个测试文件里约 80 处 `((QueryResult.Rows) execute(...)).data()` **零改动**。`execute` 本质是「运行并物化」的便利入口(测试/EXPLAIN 消费者),`executeCursor` 是「运行并流式」的生产入口。
- `QueryResult`(sealed interface)增加 `Cursor` 变体(嵌套 record,无需显式 `permits`)。
- `QueryResult.Rows` **保留**,给 EXPLAIN(`ExplainExecutor`)与 `execute` 物化路径用。
- 旧私有 `materialize`/`emptyRoot` 删除,逻辑迁入 `CursorHandle.materialize()` + `schemaFromRowType(RelDataType)`。

**SessionHandler**(per-channel 单线程,Netty 事件循环保证串行,无需锁):
- 新增 `Map<Long, Cursor> cursors`;`Cursor = (Paginator paginator)`(Paginator 内部持 iterator/ctx/schema/allocator)。
- `handleExecute`:遇 `QueryResult.Cursor` → 建 `Paginator`、**立即拉第一页**并回 `ArrowBatch(requestId, lastBatch=paginator.isDone(), data)`;未耗尽则 `cursors.put(cursorId=requestId, cursor)`。遇 `Rows`(EXPLAIN)仍走现 `sendRows`。
- 处理 `FetchRequest`:按 `cursorId` 取游标 → `paginator.nextPage(maxRows)` → 回 `ArrowBatch`;`lastBatch` 则关游标并从 `cursors` 移除。
- 处理 `CloseCursorRequest`:移除并关闭游标。
- `channelInactive` 兜底:关闭所有残留游标。

**Paginator**(新类,`exec/`):跨批切片出定长页,是本次唯一的新执行逻辑。

### 3. minidb-jdbc(客户端翻页)

- `MiniDbClient`:
  - `execute(String sql, int fetchSize)` 对 SELECT 返回 `ClientResult.Cursor(cursorId, fetchSize, firstPage, lastBatch)`(DML 仍返回 `ClientResult.Update`,不变)。
  - 新增 `fetch(long cursorId, int maxRows)`(阻塞拉下一页,返回 `(root, lastBatch)`)、`closeCursor(long cursorId)`(发 `CloseCursorRequest`)。
  - `ResponseCollector` 的 `ArrowBatch` 路由已按 `requestId` 走 `pending`,游标页与全量共用,无需改。
- `MiniDbStatement`:加 `fetchSize` 字段 + `DEFAULT_FETCH_SIZE = 4096`;`setFetchSize(int)` / `getFetchSize()` 落地;`execute`/`executeQuery` 传 `client.execute(sql, effectiveFetchSize())`(0 → 4096)。PreparedStatement 继承自动生效。
- `MiniDbResultSet`:永远游标化。`root` 由 `final` 改可变(翻页时替换);`getMetaData()` 缓存首份 schema(各页 schema 相同);`next()` 本页耗尽且 `!lastBatch` → `client.fetch(cursorId, fetchSize)` 换页、游标归零;`close()` 关本地 root + 未耗尽则发 `closeCursor(cursorId)`。

### 数据流

```
Statement.executeQuery(sql) [fetchSize 已设或默认 4096]
  → MiniDbClient.execute(sql, fetchSize)
  → Message.ExecuteRequest(requestId, sql, fetchSize)
  → wire → SessionHandler
  → QueryExecutor.openCursor(sql, currentSchema)
      → planner.plan → MiniDbRel.execute(ctx) → BatchIterator(未消费)
      → CursorHandle(it, ctx, schema)
  → Paginator.nextPage(fetchSize) → 第一页
  → Message.ArrowBatch(requestId, lastBatch=isDone, data)
  → wire → client → ClientResult.Cursor(firstPage, lastBatch)
  → MiniDbResultSet

MiniDbResultSet.next() 读到底:
  → client.fetch(cursorId, fetchSize)
  → Message.FetchRequest(newRequestId, cursorId, fetchSize)
  → SessionHandler → Paginator.nextPage(fetchSize) → 下一页
  → Message.ArrowBatch(newRequestId, lastBatch, data)
  → client → 换页

最后一页(lastBatch=true):服务端关游标;客户端 next() 返回 false。
提前 close / 断连:client.closeCursor(cursorId) / channelInactive → 服务端关游标。
```

## 分页语义与 Paginator 契约

**Paginator** 字段:`BatchIterator it`、`Schema schema`、`BufferAllocator allocator`、`VectorSchemaRoot current`(当前输入批)、`int offset`、`boolean done`、`boolean emitted`、`VectorSchemaRoot out`(复用单棵输出 root)。

- `nextPage(int maxRows)`:每次分配**新**输出 root(按 `schema`),从 `current` 批(及后续批)用 `RowCopier` 逐行拷入,拷满 `maxRows` 或迭代器耗尽为止;**每耗尽一个输入批就 `close()` 它**(释放表 owned / 算子 owned 批,见坑 5/25);返回该页(已 `setValueCount`)。若已 `done` 且 `emitted` 已置位 → 返回 `null`(无更多页)。
- 空结果/首 fetch:迭代器立即耗尽且 `emitted==false` → 返回 0 行但**带 schema** 的 `out`(复用现有 `emptyRoot` 逻辑),保证客户端拿到列元数据;`emitted` 置位,避免重复返回空页。
- `isDone()`:返回 `done`(底层迭代器是否耗尽),供 SessionHandler 设 `lastBatch`。
- `close()`:关闭 `current`(若有残留)+ `it.close()`。

**批次所有权**:`nextPage` 每页分配新 root、**序列化后由调用方关闭**(SessionHandler 序列化到 byte[] 后 `page.close()`),不跨页复用(避免 VarChar offset 缓冲跨页累积);输入批逐批随消费关闭。分页只关「尚未消费完」的批与迭代器,与现有 eager 算子「merge 后 close」的所有权语义对齐。

**内存**:分页省的是客户端与网络内存(客户端只持 1 页);服务端内存不因分页下降——eager 算子已物化整棵 root,lazy 算子(Scan/Filter/Project)也因迭代器在 `close()` 前累积所有产出批而不省(见坑 5/25 的批所有权模型)。游标本身只持有尚未消费的迭代器与当前批引用。

## 资源管理与清理兜底

游标在三处释放,防服务端迭代器/allocator 泄漏:

1. **耗尽**:最后一页 `lastBatch=true`,服务端关游标。
2. **客户端提前 close**:`MiniDbResultSet.close()` 发 `CloseCursorRequest`(仅当游标未耗尽)。
3. **连接断开**:`SessionHandler.channelInactive` 关闭该 channel 所有残留游标。

客户端 `fetch` 用 `CompletableFuture.get` 阻塞于 JDBC 调用线程,Netty 事件循环线程仍处理回包,无死锁(与现有 `execute` 同模式)。服务端游标只在事件循环线程访问,无并发问题(与 ExecContext「per-query 单线程」一致)。

## 测试

- **协议 codec**:`FetchRequest` / `CloseCursorRequest` / `ExecuteRequest.fetchSize` round-trip(`CodecTest`)。
- **Paginator 单测**:跨批切片、空结果带 schema、恰好整数倍页、eager 单大批切片、`isDone`/`nextPage` 边界。
- **服务端集成**(照 `PersistenceTest` 模式,真网络):`setFetchSize(2)` 查 5 行表 → 3 页(2/2/1);默认 `fetchSize=0` 用 4096 页大小;`setFetchSize(0)` 查 >4096 行表逐页流式、客户端不 OOM;元数据正确;提前 `close` 释放游标;默认小结果集仍单条 `ArrowBatch`。
- **客户端**:`MiniDbResultSet.next()` 跨页迭代、`close()` 发 `CloseCursorRequest`、`getMetaData()` 跨页稳定。
- **现有测试影响**:<4096 行的小结果集仍单条 `ArrowBatch` 返回,行为不变;>4096 行的结果集测试会改为多页,需核对;服务端断言 `QueryResult.Rows` 的 SELECT 测试改断言 `QueryResult.Cursor`。

## 不在本范围

- `ResultSet.setFetchSize` / `Statement.setMaxRows` 仍 no-op(本次只做 Statement.setFetchSize)。
- 事务/可更新游标(无事务,游标只读、forward-only)。
- 服务端持久化游标(游标随连接生命周期,断连即释放)。
- eager 算子(Sort/Aggregate/Join)的服务端内存优化(需算子级增量化,属独立大功能)。
