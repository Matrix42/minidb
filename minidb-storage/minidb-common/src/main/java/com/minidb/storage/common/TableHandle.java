package com.minidb.storage.common;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface TableHandle extends AutoCloseable {
    enum Operation { INSERT, UPDATE, DELETE }

    TableSchema schema();
    BatchIterator scan();
    void writePart(VectorSchemaRoot batch, Operation op);
    long rowCount();
    int partCount();
    int compact(long targetSizeBytes);
    void clearParts();
    VectorSchemaRoot newBatchRoot();

    @Override
    default void close() throws Exception {}
}