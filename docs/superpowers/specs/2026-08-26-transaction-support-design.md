# 事务支持设计文档

日期：2026-08-26 | 状态：已完成

## 1. 概述

为 MiniDB 增加多级事务支持(READ_UNCOMMITTED / READ_COMMITTED / REPEATABLE_READ / SERIALIZABLE)，基于 Snapshot Isolation 引擎，不同的隔离级别是"快照时机"和"冲突检测"的差异。

### 核心策略

- **隔离级别在服务端启动时配置**（`--isolation-level`），全局统一，不在协议层按请求传递
- **DDL 不参与事务**——每条 DDL 自动提交（MySQL 语义）
- **First-committer-wins**：后提交的冲突事务 abort，由应用层重试
- **LSMTable**：利用现有 WAL 基础设施，加 txId 字段 + tx-private MemTable
- **SimpleTable**：临时目录 `.tx/<txId>/` 存放未提交写入，commit 时原子移动，rollback 时删除
- **全局事务日志**（`data/txlog.log`）作为 COMMIT 的权威来源，保证 crash recovery 正确性

### 隔离级别语义

| 级别 | 快照时机 | 写冲突检测 |
|------|---------|-----------|
| READ_UNCOMMITTED | 无快照，读最新(含未提交) | 无 |
| READ_COMMITTED | 每条语句开始时取快照 | 无 |
| REPEATABLE_READ | 事务开始时取快照 | 无 |
| SERIALIZABLE | 事务开始时取快照 | 有(SSI) |

## 2. 协议层改动（minidb-protocol）

### 新增消息类型

```java
// Message.java 新增
record BeginRequest(long requestId) implements Message {}
record CommitRequest(long requestId) implements Message {}
record RollbackRequest(long requestId) implements Message {}
record SetAutoCommitRequest(long requestId, boolean autoCommit) implements Message {}
record CommitResponse(long requestId, boolean ok, String error) implements Message {}
```

### 新增 MessageType 常量

```java
// MessageType.java 新增
BEGIN_REQUEST      = 0x17
COMMIT_REQUEST     = 0x18
ROLLBACK_REQUEST   = 0x19
SET_AUTOCOMMIT     = 0x1A
COMMIT_RESPONSE    = 0x24
```

### 编解码

`MessageEncoder` / `MessageDecoder` 按现有模式加 5 个 case，不涉及 Arrow 编解码。

### 不改动

`ExecuteRequest` 保持原样——事务模式下 SessionHandler 在服务端维护 tx 上下文，无需随每条 SQL 传递 txId。

## 3. 全局事务日志（TxLog）

### 格式

```
[checksum:4][length:4][txId:8][status:1]   // status: 0=COMMIT, 1=ABORT
```

文件路径：`data/txlog.log`。只追加，不删除。每条记录 CRC32 校验。

### 关键方法

```java
public class TxLog implements AutoCloseable {
    void append(long txId, byte status);      // 写一条 + fsync
    Set<Long> recoverCommitted();             // 恢复时读取所有 COMMITTED 的 txId
    void truncate();                          // 清空日志
}
```

### 截断时机

- 服务端正常关闭时
- 运行时：`activeTxCount == 0` 时自动截断

### Commit 流程中的角色

```
1. TxLog.append(txId, COMMIT) → fsync        ← 决定性的：此后事务已提交
2. LSMTable: merge tx-private → shared MemTable
3. SimpleTable: move txDir/part → tableDir/
4. TxManager: txStatuses.put(txId, COMMITTED)
5. 清理 accessSets
6. 若 activeTxCount == 0, TxLog.truncate()
```

## 4. TransactionManager（新组件）

### 职责

- 全局事务 ID 分配（`AtomicLong nextTxId`，从 1 开始）
- 事务状态管理（`ConcurrentHashMap<Long, TxStatus>`）
- 快照边界计算（根据隔离级别）
- Serializable 冲突检测（SSI）
- Commit/Rollback 编排

### 核心数据结构

```java
public class TransactionManager {
    private final AtomicLong nextTxId = new AtomicLong(1);
    private final ConcurrentHashMap<Long, TxStatus> txStatuses;
    private final TransactionIsolation isolationLevel;
    private final TxLog txLog;

    // Serializable 专用
    // key = "schema.table.column", value = 最后写入的 txId
    private final ConcurrentHashMap<String, Long> lastWriteTx;
    // 每个活跃事务的读写集
    private final ConcurrentHashMap<Long, TxAccessSet> accessSets;
}
```

### 核心方法

```java
TxHandle begin();
    // 分配 txId → 确定 snapshotTxId → 返回 TxHandle(txId, snapshotTxId)

long computeSnapshot(long txId);
    // READ_UNCOMMITTED → -1（不过滤）
    // READ_COMMITTED → 最新 COMMITTED txId（每语句刷新）
    // REPEATABLE_READ / SERIALIZABLE → 最新 COMMITTED txId（事务开始时一次）

CommitResult commit(long txId);
    // 1. Serializable: SSI 冲突检测
    // 2. TxLog.append(txId, COMMIT) + fsync
    // 3. 各表 commitTx(txId)
    // 4. 标记 COMMITTED + 清理

void rollback(long txId);
    // 1. 各表 rollbackTx(txId)
    // 2. 标记 ABORTED + 清理

void recordRead(long txId, String schema, String table, String column);
    // SERIALIZABLE 级别记录读集

void recordWrite(long txId, String schema, String table, String column);
    // SERIALIZABLE 级别记录写集 + 更新 lastWriteTx

void checkSerializableConflict(long txId);
    // 检测读写冲突：本事务读取的列是否有其他事务在 snapshot 之后写入
```

### TxHandle

```java
public class TxHandle {
    private final long txId;
    private volatile long snapshotTxId;   // READ_COMMITTED 下可刷新
    private volatile TxStatus status;

    void refreshSnapshot(long newSnapshot);  // READ_COMMITTED 每语句调用
}
```

### TxAccessSet

```java
static class TxAccessSet {
    final long snapshotTxId;
    final Set<String> readSet = ConcurrentHashMap.newKeySet();   // "schema.table.column"
    final Set<String> writeSet = ConcurrentHashMap.newKeySet();  // "schema.table.column"
}
```

### 活跃事务计数

`activeTxCount`（`AtomicInteger`）用于 TxLog 截断判断：`begin()` 时 +1，`commit()` / `rollback()` 时 -1。当 `activeTxCount == 0` 时 TxLog 可安全截断（所有事务已结束，全局日志无残留引用）。

### 生命周期

在 `MiniDbServer` 启动时创建，注入 `SessionHandler`、`QueryExecutor`、`StorageManager`。

## 5. LSMTable 存储层改动

### 5.1 WAL 格式扩展

WAL 条目增加 `txId` 字段（8 字节，在 kind 之前）：

```
现状: [checksum:4][length:4][kind:1][keyLen:2][key...][valLen:2][values...]
新:   [checksum:4][length:4][txId:8][kind:1][keyLen:2][key...][valLen:2][values...]
```

`encodeEntry` / `decodeEntry` 各加一行 `txId` 读写。旧格式无 `txId` 的条目 recovery 时视为 `txId=0`（已提交）。

### 5.2 tx-private MemTable

```java
// LSMTable 新增字段
private final ConcurrentHashMap<Long, MemTable> txMemTables;
```

写路径：`writePart(batch, op, txId)` → 取/创建 tx-private MemTable → WAL 带 txId 写入 → 私有 MemTable 更新。

Commit：`commitTx(txId)` → 私有 MemTable 的行合并到 shared MemTable（`synchronized(tableLock)`）。

Rollback：`rollbackTx(txId)` → 移除私有 MemTable。WAL 条目保留，recovery 时根据 TxLog 判断跳过。

**tx-private MemTable 写满时**：flush 到 tx-private SSTable（`tableDir/.tx/<txId>/sst-*.sst`），commit 时把 tx-private SSTable 加入 `sstManager` 的 L0，rollback 时删除文件。避免大事务撑爆内存。

### 5.3 Scan：快照过滤

```java
public BatchIterator scan(long snapshotTxId);
```

`snapshotTxId == -1` → READ_UNCOMMITTED，不过滤，直接读当前 shared MemTable+SSTable。

`snapshotTxId >= 0` → MergeIterator 加过滤逻辑：
- 同一 key 若在多个源中出现，按 txId 降序取最新版本
- 若最新版本是 DELETE tombstone 且其 txId ≤ snapshotTxId，该 key 不可见
- 未提交事务(或已 ABORT)的写入不可见

### 5.4 Recovery

```java
void recover() {
    sstManager.loadExisting(...);
    Set<Long> committed = txLog.recoverCommitted();
    List<WAL.Entry> entries = wal.recover();
    for (WAL.Entry entry : entries) {
        // txId==0（旧格式）或 txId 在 committed 集合中 → 重放
        if (entry.txId() == 0 || committed.contains(entry.txId())) {
            memTable.put(entry.key(), entry.value());
        }
        // 否则跳过（未提交事务）
    }
}
```

## 6. SimpleTable 存储层改动

### 6.1 写路径：临时目录

```java
public void writePart(VectorSchemaRoot batch, Operation op, long txId) {
    Path txDir = tableDir.resolve(".tx").resolve(String.valueOf(txId));
    Files.createDirectories(txDir);
    int seq = txPartSeq(txDir);
    format.write(txDir.resolve(String.format("part-%06d.%s", seq, ext)), batch);
}
```

UPDATE/DELETE 的重写：新 part 写入 `txDir`，旧 part 保留在 `tableDir` 中不动。大事务写入直接落盘到 `.tx/` 目录，内存零占用。

### 6.2 Commit

```java
public void commitTx(long txId) {
    Path txDir = tableDir.resolve(".tx").resolve(String.valueOf(txId));
    if (!Files.exists(txDir)) return;
    // 移动临时 part 到正式目录（同目录原子 rename）
    for (Path part : listParts(txDir)) {
        Files.move(part, tableDir.resolve(part.getFileName()), ATOMIC_MOVE);
    }
    Files.deleteIfExists(txDir);
}
```

### 6.3 Rollback

```java
public void rollbackTx(long txId) {
    Path txDir = tableDir.resolve(".tx").resolve(String.valueOf(txId));
    if (Files.exists(txDir)) deleteRecursively(txDir);
}
```

### 6.4 Recovery

```java
public void recoverTxDirs(Set<Long> committedTxIds) {
    Path txRoot = tableDir.resolve(".tx");
    if (!Files.exists(txRoot)) return;
    for (Path txDir : listDirs(txRoot)) {
        long txId = Long.parseLong(txDir.getFileName().toString());
        if (committedTxIds.contains(txId)) {
            movePartsToTableDir(txDir);  // 已提交：移动
        } else {
            deleteRecursively(txDir);    // 未提交：删除
        }
    }
}
```

### 6.5 Scan：排除临时目录

`collectParts` 递归时跳过 `.tx` 前缀目录。

## 7. TableHandle 接口扩展

```java
// 新增方法
default BatchIterator scan(long snapshotTxId) {
    return scan();  // 默认回退，LSMTable 覆写
}

default void writePart(VectorSchemaRoot batch, Operation op, long txId) {
    writePart(batch, op);  // 默认回退非事务路径，事务感知表覆写
}

default void commitTx(long txId) {}   // 默认空操作
default void rollbackTx(long txId) {} // 默认空操作
```

## 8. 执行层改动

### 8.1 ExecContext

新增 `TxHandle tx` 字段（null = 非事务模式）。新增构造器重载。

### 8.2 QueryExecutor

`executeQuery(sql, currentSchema, tx)` — 新增带 tx 参数的重载。事务模式下 DML 执行后不立即返回结果，而是由 SessionHandler 在 commit 时统一返回。

### 8.3 MiniDbModify

`appendRows` / `lsmModify` / `rewriteTable` 判断 `ctx.tx()` 是否为 null：
- 非 null：调用 `writePart(batch, op, txId)` 走事务路径
- null：调用 `writePart(batch, op)` 走即时落盘路径（现状）

### 8.4 MiniDbScan

`execute(ctx)` 判断 `ctx.inTransaction()` 且 `snapshotTxId >= 0`：
- 是：调用 `table.scan(snapshotTxId)` 走快照过滤
- 否：调用 `table.scan()` 走旧路径

## 9. 会话层改动（SessionHandler）

### 新增字段

```java
private final TransactionManager txManager;
private TxHandle tx;               // null = 非事务模式
private boolean autoCommit = true; // 默认自动提交
```

### 新增消息处理

| 消息 | 行为 |
|------|------|
| `BeginRequest` | 若已在事务中抛错；否则 `tx = txManager.begin()` |
| `CommitRequest` | 若不在事务中抛错；否则 `txManager.commit(tx)` → `tx = null` |
| `RollbackRequest` | 若不在事务中抛错；否则 `txManager.rollback(tx)` → `tx = null` |
| `SetAutoCommitRequest` | 更新 `autoCommit`；`false→true` 时若在事务中隐式提交；`true→false` 时隐式 begin |

### handleExecute 改动

事务模式下：
- `READ_COMMITTED`：每语句执行前刷新 `tx.refreshSnapshot()`
- DML 执行后结果不直接返回，commit 时统一返回 `UpdateCount`

### channelInactive 改动

连接断开时，若 `tx != null && tx.status() == ACTIVE`，自动 `txManager.rollback(tx)`。

## 10. JDBC 驱动改动（minidb-jdbc）

### MiniDbConnection

- `setAutoCommit(false)`：发送 `SetAutoCommitRequest(false)`，开启事务
- `setAutoCommit(true)`：发送 `SetAutoCommitRequest(true)`，若在事务中隐式提交
- `commit()`：发送 `CommitRequest`，仅在 `autoCommit=false` 时有效
- `rollback()`：发送 `RollbackRequest`，仅在 `autoCommit=false` 时有效
- `getTransactionIsolation()`：返回记录的隔离级别（服务端配置决定实际级别）
- `setTransactionIsolation(int)`：只记录，不发网络请求

### MiniDbClient

新增 `send(Message)` 方法，支持 Begin/Commit/Rollback/SetAutoCommit 消息的同步发送。

### MiniDbDatabaseMetaData

- `supportsTransactions()` → `true`
- `supportsMultipleTransactions()` → `true`
- `getDefaultTransactionIsolation()` → `TRANSACTION_SERIALIZABLE`
- `supportsTransactionIsolationLevel(int)` → 四个级别均返回 `true`
- `supportsDataDefinitionAndDataManipulationTransactions()` → `false`（DDL 自动提交）

## 11. 配置

```java
// MiniDbConfig 新增
private final TransactionIsolation isolationLevel; // 默认 SERIALIZABLE

// TransactionIsolation 枚举
public enum TransactionIsolation {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE;
}

// 启动参数
--isolation-level serializable    // 默认
--isolation-level read-committed
--isolation-level repeatable-read
--isolation-level read-uncommitted
```

## 12. 组件依赖图

```
MiniDbServer
  → TransactionManager (新)
       → TxLog (新, data/txlog.log)
       → ConcurrentHashMap (txStatuses, accessSets, lastWriteTx)
  → SessionHandler (改)
       → TxHandle (新, per-connection)
  → QueryExecutor (改)
       → ExecContext (改, +TxHandle)
            → MiniDbModify (改, 感知 tx)
            → MiniDbScan (改, 快照过滤)
  → StorageManager (改)
       → LSMTable (改, WAL+txId, tx-private MemTable, snapshot scan)
       → SimpleTable (改, .tx/ 临时目录)

minidb-protocol (改)
  Message.java: +5 消息类型
  MessageType.java: +5 常量
  MessageEncoder/Decoder: +5 case

minidb-jdbc (改)
  MiniDbConnection: commit/rollback/setAutoCommit 不再空操作
  MiniDbClient: +send()
  MiniDbDatabaseMetaData: 事务元数据更新
```

## 13. 异常路径处理

| 场景 | 处理 |
|------|------|
| 事务中执行 DDL | 自动提交当前事务，然后执行 DDL |
| Begin 时已在事务中 | 抛异常 |
| Commit 时不在事务中 | 抛异常 |
| 连接断开时事务未提交 | 自动 rollback |
| Commit 时 Serializable 冲突 | 抛 SerializationException |
| Commit 时 fsync 失败 | 事务回滚 |
| Crash 时全局日志有 COMMIT 但表未 merge | Recovery 时补做 |
| Crash 时全局日志无 COMMIT | Recovery 时丢弃未提交写入 |
| 事务内 LSM MemTable 写满 | flush 到 tx-private SSTable |
| 事务内 SimpleTable 写入大量数据 | 直接落盘到 .tx/ 目录，内存零占用 |

## 14. Crash Recovery 流程

```
StorageManager.loadAll():
  1. TxLog.recoverCommitted() → Set<Long> committedTxIds
  2. 对每个 LSMTable:
     a. 恢复 SSTable 元数据
     b. 重放 WAL: 只重放 txId==0 或 txId ∈ committedTxIds 的条目
  3. 对每个 SimpleTable:
     a. recoverTxDirs(committedTxIds):
        - 已提交 .tx/<txId>/ → 移动 part 到正式目录
        - 未提交 .tx/<txId>/ → 删除
  4. TxLog.truncate()（所有旧事务已处理完）
```