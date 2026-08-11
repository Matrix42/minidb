package com.minidb.server.exec;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface BatchIterator extends AutoCloseable {
    boolean hasNext();

    VectorSchemaRoot next();

    @Override
    void close();

    static BatchIterator empty() {
        return new EmptyBatchIterator();
    }
}
