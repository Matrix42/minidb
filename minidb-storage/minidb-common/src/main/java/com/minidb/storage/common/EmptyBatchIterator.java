package com.minidb.storage.common;

import org.apache.arrow.vector.VectorSchemaRoot;

public class EmptyBatchIterator implements BatchIterator {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public VectorSchemaRoot next() {
        throw new IllegalStateException("No more batches available");
    }

    @Override
    public void close() {
        // No resources to close
    }
}
