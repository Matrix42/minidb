package com.minidb.storage.common;

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

public interface TableHandle extends AutoCloseable {
    enum Operation { INSERT, UPDATE, DELETE }

    TableSchema schema();
    BatchIterator scan();
    /** 列裁剪扫描:只读指定列。默认回退全量扫描。 */
    default BatchIterator scan(int[] projectedColumns) {
        return scan();
    }

    /**
     * 主键范围扫描:只返回主键在闭区间 [rangeLo, rangeHi] 内的行(元素 null = 该列无界)。
     * 默认回退全量扫描;LSM 表覆写以利用文件 min/max 与块索引 startKey 裁剪。
     */
    default BatchIterator scan(List<Object> rangeLo, List<Object> rangeHi) {
        return scan();
    }

    /**
     * 快照读:只返回 snapshotTxId 之前已提交的行。
     * snapshotTxId == -1 表示 READ_UNCOMMITTED(不过滤)。
     * 默认回退全量扫描;LSMTable 覆写以支持快照隔离。
     */
    default BatchIterator scan(long snapshotTxId) {
        return scan();
    }

    void writePart(VectorSchemaRoot batch, Operation op);

    /**
     * 事务写入:带 txId 的写操作。默认回退非事务路径。
     */
    default void writePart(VectorSchemaRoot batch, Operation op, long txId) {
        writePart(batch, op);
    }

    /**
     * 提交事务:将 tx-private 写入合并到主存储。
     * 默认空操作;事务感知表覆写。
     */
    default void commitTx(long txId) {}

    /**
     * 回滚事务:丢弃 tx-private 写入。
     * 默认空操作;事务感知表覆写。
     */
    default void rollbackTx(long txId) {}
    long rowCount();
    int partCount();
    int compact(long targetSizeBytes);
    void clearParts();
    VectorSchemaRoot newBatchRoot();

    /**
     * 按主键点查(仅 LSM 表支持,走 memtable → Bloom → 单 SSTable)。
     * 返回该键的行值;键不存在、已被删除或表不支持点查时返回 null。
     */
    default RowValue getByKey(List<Object> key) {
        return null;
    }

    @Override
    default void close() throws Exception {}
}