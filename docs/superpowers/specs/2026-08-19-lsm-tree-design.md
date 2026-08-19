# LSM-Tree 存储引擎设计

日期：2026-08-19

## 概述

为 MiniDB 引入 LSM-Tree（Log-Structured Merge-Tree）存储引擎，提供写优化的存储路径。有主键的表使用 LSMTable，无主键的表继续使用 SimpleTable。

## 核心架构

```
StorageManager
  ├── SimpleTable (无主键表，现有逻辑不变)
  └── LSMTable    (有主键表，新增)

LSMTable 内部组件：
  ├── MemTable        — ConcurrentSkipListMap<Key, RowValue>，写缓冲
  ├── WAL             — 顺序写日志，保证 crash 后恢复 MemTable
  ├── SSTableManager  — 管理所有 SSTable 文件，按 level 组织
  ├── Compaction      — 后台 leveled compaction
  └── MergeIterator   — 合并 MemTable + L0 + L1+ 的归并迭代器
```

**表类型由主键决定**：`StorageManager.loadAll()` 时，`TableSchema.primaryKey` 非空 → `LSMTable`，空 → `SimpleTable`。

**文件格式由用户指定**：`WITH('format'='parquet')` 控制 SSTable data block 的编码（`PartFormat`），与表类型无关。

## 表接口

`LSMTable` 和 `SimpleTable` 实现共同接口 `TableHandle`：

```java
public interface TableHandle extends AutoCloseable {
    TableSchema schema();
    BatchIterator scan();
    void writePart(VectorSchemaRoot batch, Operation op); // INSERT/UPDATE/DELETE
    long rowCount();
    int partCount(); // SSTable 文件数
    void compact(long targetSizeBytes);
    void clearParts(); // truncate
    VectorSchemaRoot newBatchRoot();
}
```

`MiniDbScan`/`MiniDbModify` 通过 `ExecContext.getTable()` 拿到 `TableHandle`，不感知具体实现。

## MemTable

- **数据结构**：`ConcurrentSkipListMap<List<Object>, RowValue>`
- **Key**：主键列值，`List<Object>`（与现有 `RowVectors.keyOf` 一致）
- **Value**：`RowValue { byte kind; Object[] values }`（kind: 0=INSERT, 1=UPDATE, 2=DELETE）
- **Flush 阈值**：`estimatedBytes >= flushThreshold`（默认 64MB，可配置）
- **Flush 过程**：冻结当前 MemTable → 创建新 MemTable → 后台线程将旧 MemTable 写为 L0 SSTable

## WAL

- 每个 LSMTable 一个 WAL 文件：`data/<schema>/<table>/wal.log`
- 记录格式：`[checksum:4B][length:4B][entry bytes]`
- Entry 格式：`[kind:1B][keyColCount:2B][key1Len:4B][key1...]...[colCount:2B][col1Len:4B][col1...]`
- 写入顺序：先 `WAL.append(entry)` → 再 `MemTable.put(key, value)`
- 截断：MemTable flush 成功后清空 WAL
- 恢复：启动时检查 WAL 非空 → 回放重建 MemTable
- fsync 可配置（默认关闭，学习型定位）

## SSTable 文件格式

自定义二进制容器格式，data block 内部用 `PartFormat`（Arrow/Parquet）编码。

```
┌──────────────────────────────────────┐
│  Data Block 0                        │  ← PartFormat 编码 (Arrow/Parquet)
│    - 行数 (2B)                       │
│    - 实际字节数 (4B)                  │
│    - PartFormat 编码的 batch 字节    │
├──────────────────────────────────────┤
│  Data Block 1                        │
│  ...                                 │
│  Data Block N                        │
├──────────────────────────────────────┤
│  Index Block                         │
│    - entry 数 (4B)                   │
│    - 每个 entry:                     │
│        block 起始 key 长度 (2B)      │
│        block 起始 key 字节           │
│        block 在文件中的 offset (8B)  │
│        block 大小 (4B)               │
├──────────────────────────────────────┤
│  Bloom Filter                        │
│    - bit 数组长度 (4B)               │
│    - hash 函数数 (1B)                │
│    - bit 数组字节                    │
├──────────────────────────────────────┤
│  Footer (末尾 64B)                   │
│    - magic: "LSMTBL" (6B)            │
│    - level: (1B)                     │
│    - data block 数 (4B)              │
│    - 总行数 (8B)                     │
│    - min key 长度 (2B)               │
│    - min key 字节 (变长, 最多 256B)  │
│    - max key 长度 (2B)               │
│    - max key 字节 (变长, 最多 256B)  │
│    - index block offset (8B)         │
│    - bloom filter offset (8B)        │
│    - 填充到 64B                      │
└──────────────────────────────────────┘
```

SSTable 命名：`sst-L<level>-<seq>.sst`，如 `sst-L0-000001.sst`。

## 写路径

```
INSERT → WAL.append(entry) → MemTable.put(key, INSERT)
UPDATE → WAL.append(entry) → MemTable.put(key, UPDATE)
DELETE → WAL.append(entry) → MemTable.put(key, DELETE)

SSTable 文件永不修改！
```

**MiniDbModify 适配**：对 `LSMTable`，UPDATE/DELETE 走 `writePart(batch, op)` 只写 MemTable，不做全表重写。对 `SimpleTable` 保持现有 `rewriteTable` 逻辑。

## 读路径（MergeIterator）

```
MergeIterator 合并多个数据源:
  sources = [MemTable, L0_SSTable_0, L0_SSTable_1, ..., L1_SSTable_0, ...]

每个 source 是有序迭代器:
  - MemTable → ConcurrentSkipListMap 的 entry iterator
  - SSTable → 按 data block 顺序读，block 内行已排序

next():
  - 所有 source 取当前 key 最小值
  - 同 key 取 source 优先级最高的（MemTable > L0(新) > L0(旧) > L1 > L2 > ...）
  - DELETE tombstone → 跳过，继续下一个
  - 返回该行
```

**点查优化**：`LSMTable.getByKey(key)` 先查 MemTable → 再按 level 查 SSTable（Bloom Filter 快速跳过），返回第一个匹配的 RowValue。用于约束校验（主键/唯一冲突）。

## Compaction

### Leveled Compaction

- L0：最多 4 个 SSTable（key range 可重叠），按 flush 顺序，新的优先
- L1：最多 ~64MB
- L2：最多 ~640MB
- L3+：10x 递增

```
触发条件：
  L0.SSTable 数 >= 4  → compact L0 → L1
  Ln.总大小 >= 阈值   → compact Ln → L(n+1)

Compaction 过程（L0→L1）：
  1. 选 L0 所有 SSTable + L1 中与 L0 key range 重叠的 SSTable
  2. MergeIterator 归并排序，同 key 保留最新，丢弃旧版本和 tombstone
  3. 按目标大小切分输出新 SSTable
  4. 原子交换：写新 SSTable → 删旧 SSTable（.tmp/.bak 模式）
```

### 后台线程

两个独立线程：
- **Flush 线程**：检查 MemTable 是否超阈值，是则冻结并 flush 为 L0 SSTable
- **Compaction 线程**：检查各层是否触发 compact，是则执行

## 配置

```yaml
# data/config.yaml
lsm:
  memtable-size-mb: 64
  l0-file-limit: 4
  level-size-multiplier: 10
  wal-fsync: false
  bloom-bits-per-key: 10
```

## 文件清单

```
data/<schema>/<table>/
  ├── wal.log                 — 当前 WAL
  ├── sst-L0-000001.sst       — L0 SSTable
  ├── sst-L0-000002.sst
  ├── sst-L1-000001.sst       — L1 SSTable
  └── ...
```

## 错误处理与边界情况

- **WAL 损坏**：checksum 不匹配时，跳过该条记录及后续，LOG.warn 告警
- **Flush 失败**：后台 flush 线程捕获异常，LOG.error，保留旧 MemTable（数据在内存中不丢），下次重试
- **Compaction 失败**：后台 compaction 线程捕获异常，LOG.error，旧 SSTable 不动（.tmp 目录清理）
- **读并发**：MemTable 被冻结后仍可读，scan 持有旧 MemTable 引用直到迭代器关闭
- **L1+ 不重叠**：同层（L1+）内 SSTable 的 key range 不重叠（compaction 保证）；L0 内 SSTable 可重叠

## 生命周期

- **LSMTable.close()**：关闭时 flush 当前 MemTable（同步），关闭 WAL 文件，停止后台任务
- **SimpleTable**：close 是 no-op（数据不驻留内存）
- **StorageManager.close()**：遍历所有 `TableHandle` 调 `close()`

- **StorageManager**：`loadAll()` 按主键分发；持有 `LSMBackgroundExecutor`
- **ExecContext**：`getTable()` 返回 `TableHandle` 代替 `SimpleTable`
- **MiniDbScan**：`table.scan()` 调用不变，返回 `BatchIterator`
- **MiniDbModify**：对 `LSMTable` 走 `writePart` 路径；对 `SimpleTable` 保持 `rewriteTable`
- **ConstraintChecker**：用 `LSMTable.getByKey()` 做点查优化
- **StatsManager**：`analyze()` 用 `TableHandle.scan()` 不变