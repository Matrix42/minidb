package com.minidb.server.exec;

import com.minidb.storage.common.BatchIterator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A not-yet-consumed query result: the pull-mode iterator plus the execution
 * context it runs against. The server keeps this alive across fetch requests
 * (cursor paging); materialize() is the eager fallback used by the test-facing
 * {@link QueryExecutor#execute} entry point.
 */
public record CursorHandle(BatchIterator iterator, ExecContext context, Schema schema) {

    /** 释放游标(关闭底层迭代器),不物化数据。 */
    public void close() {
        iterator.close();
    }

    /** Pulls every remaining batch into a single owned root and closes the iterator. */
    public VectorSchemaRoot materialize() {
        VectorSchemaRoot merged = null;
        int dst = 0;
        try {
            while (iterator.hasNext()) {
                VectorSchemaRoot batch = iterator.next();
                if (merged == null) {
                    merged = VectorSchemaRoot.create(batch.getSchema(), context.allocator());
                    merged.allocateNew();
                }
                for (int i = 0; i < batch.getRowCount(); i++) {
                    RowCopier.copyRow(batch, i, merged, dst++);
                }
            }
        } finally {
            iterator.close();
        }
        if (merged == null) {
            return emptyRoot();
        }
        merged.setRowCount(dst);
        return merged;
    }

    private VectorSchemaRoot emptyRoot() {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, context.allocator());
        root.allocateNew();
        root.setRowCount(0);
        return root;
    }
}
