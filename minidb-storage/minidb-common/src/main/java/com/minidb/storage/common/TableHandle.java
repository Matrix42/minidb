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
    void writePart(VectorSchemaRoot batch, Operation op);
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