package com.minidb.server.exec;

import com.minidb.storage.common.BatchIterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Slices a pull-mode batch iterator into fixed-size pages for cursor paging.
 * Each page is a fresh, owned root (the caller serializes then closes it); the
 * input batches are owned by the underlying iterator and released by its
 * close(). nextPage returns null only after at least one page has been emitted.
 */
public final class Paginator implements AutoCloseable {

    private final BatchIterator iterator;
    private final Schema schema;
    private final BufferAllocator allocator;
    private VectorSchemaRoot current;
    private int offset;
    private boolean done;
    private boolean emitted;

    public Paginator(BatchIterator iterator, Schema schema, BufferAllocator allocator) {
        this.iterator = iterator;
        this.schema = schema;
        this.allocator = allocator;
    }

    public VectorSchemaRoot nextPage(int maxRows) {
        if (done && emitted) {
            return null;
        }
        VectorSchemaRoot out = VectorSchemaRoot.create(schema, allocator);
        out.allocateNew();
        int dst = 0;
        while (dst < maxRows) {
            if (current == null || offset >= current.getRowCount()) {
                if (!advance()) {
                    done = true;
                    break;
                }
                continue;
            }
            RowCopier.copyRow(current, offset, out, dst);
            offset++;
            dst++;
        }
        // A page can fill to exactly maxRows on the last row of the final
        // batch, in which case the loop above never observes exhaustion.
        // Re-check here so isDone() is accurate immediately after the final
        // page is emitted.
        if (!done && current != null && offset >= current.getRowCount() && !advance()) {
            done = true;
        }
        // setRowCount sets the root row count AND every vector's valueCount.
        out.setRowCount(dst);
        emitted = true;
        return out;
    }

    public boolean isDone() {
        return done;
    }

    /**
     * Drops the fully-consumed batch and moves to the next one; false when
     * exhausted. The consumed batch is NOT closed here — the iterator owns
     * every batch it yields and releases them all in its own close().
     */
    private boolean advance() {
        current = null;
        if (iterator.hasNext()) {
            current = iterator.next();
            offset = 0;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        // The iterator releases all batches it yielded (including any still
        // held in `current`); Paginator must not close them a second time.
        current = null;
        iterator.close();
    }
}
