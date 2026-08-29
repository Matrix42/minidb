package com.minidb.storage.common;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface BatchIterator extends AutoCloseable {
    boolean hasNext();

    VectorSchemaRoot next();

    @Override
    void close();

    static BatchIterator empty() {
        return new EmptyBatchIterator();
    }

    /** 包装一个迭代器,在 hasNext()/next() 中自动检查线程中断(客户端断连→cancel)。 */
    static BatchIterator interruptible(BatchIterator delegate) {
        return new BatchIterator() {
            @Override
            public boolean hasNext() {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("query cancelled");
                }
                return delegate.hasNext();
            }

            @Override
            public VectorSchemaRoot next() {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("query cancelled");
                }
                return delegate.next();
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }
}
