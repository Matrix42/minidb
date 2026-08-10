package com.minidb.server.storage;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowTable implements AutoCloseable {

    public static final int MAX_BATCH_ROWS = 4096;

    private final TableSchema schema;
    private final BufferAllocator allocator;
    private final Schema arrowSchema;
    private final List<VectorSchemaRoot> batches = new CopyOnWriteArrayList<>();

    public ArrowTable(TableSchema schema, BufferAllocator allocator) {
        this.schema = schema;
        this.allocator = allocator;
        List<Field> fields = new ArrayList<>();
        for (ColumnMeta column : schema.columns()) {
            fields.add(ArrowTypes.field(column));
        }
        this.arrowSchema = new Schema(fields);
    }

    public TableSchema schema() {
        return schema;
    }

    public Schema arrowSchema() {
        return arrowSchema;
    }

    public VectorSchemaRoot newBatchRoot() {
        return VectorSchemaRoot.create(arrowSchema, allocator);
    }

    public void appendBatch(VectorSchemaRoot batch) {
        if (batch.getRowCount() > MAX_BATCH_ROWS) {
            throw new IllegalArgumentException(
                    "batch exceeds MAX_BATCH_ROWS: " + batch.getRowCount());
        }
        batches.add(batch);
    }

    /**
     * Swap the table's batches for {@code newBatches}. The caller keeps the
     * references to the previous batches and is responsible for closing them.
     */
    public void replaceBatches(List<VectorSchemaRoot> newBatches) {
        batches.clear();
        batches.addAll(newBatches);
    }

    public List<VectorSchemaRoot> batches() {
        return List.copyOf(batches);
    }

    public long rowCount() {
        long count = 0;
        for (VectorSchemaRoot batch : batches) {
            count += batch.getRowCount();
        }
        return count;
    }

    @Override
    public void close() {
        for (VectorSchemaRoot batch : batches) {
            batch.close();
        }
        batches.clear();
    }
}
