package com.minidb.server.exec;

import com.minidb.storage.common.BatchIterator;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A not-yet-consumed query result: the pull-mode iterator plus the execution
 * context it runs against. The server keeps this alive across fetch requests
 * (cursor paging); materialize() is the eager fallback used by the test-facing
 * {@link QueryExecutor#execute} entry point.
 */
public record CursorHandle(BatchIterator iterator, ExecContext context, Schema schema) {

    /** 释放游标(关闭底层迭代器 + CSE 缓存),不物化数据。 */
    public void close() {
        iterator.close();
        context.close();
    }

    /** Pulls every remaining batch into a single owned root and closes the iterator. */
    public VectorSchemaRoot materialize() {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        int total = 0;
        VectorSchemaRoot merged;
        try {
            while (iterator.hasNext()) {
                VectorSchemaRoot batch = iterator.next();
                batches.add(batch);
                total += batch.getRowCount();
            }
            if (batches.isEmpty()) {
                return emptyRoot();
            }
            merged = VectorSchemaRoot.create(batches.get(0).getSchema(), context.allocator());
            // 预分配 total,批量列拷贝(固定宽走无检查 copyFrom)的前提
            for (FieldVector v : merged.getFieldVectors()) {
                v.setInitialCapacity(total);
                v.allocateNew();
            }
            int dst = 0;
            for (VectorSchemaRoot batch : batches) {
                RowCopier.copyRows(batch, 0, merged, dst, batch.getRowCount());
                dst += batch.getRowCount();
            }
            merged.setRowCount(dst);
            return merged;
        } finally {
            close(); // 释放 iterator + CSE 缓存
        }
    }

    private VectorSchemaRoot emptyRoot() {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, context.allocator());
        root.allocateNew();
        root.setRowCount(0);
        return root;
    }
}
