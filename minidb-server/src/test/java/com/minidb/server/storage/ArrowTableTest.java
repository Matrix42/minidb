package com.minidb.server.storage;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.TableSchema;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArrowTableTest {

    BufferAllocator allocator;
    ArrowTable table;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        table = new ArrowTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR))), allocator);
    }

    @AfterEach
    void tearDown() {
        table.close();
        allocator.close();
    }

    private VectorSchemaRoot batch(int... ids) {
        VectorSchemaRoot root = table.newBatchRoot();
        IntVector id = (IntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        root.allocateNew();
        for (int i = 0; i < ids.length; i++) {
            id.setSafe(i, ids[i]);
            name.setSafe(i, ("n" + ids[i]).getBytes());
        }
        root.setRowCount(ids.length);
        return root;
    }

    @Test
    void appendAndScanBatches() {
        table.appendBatch(batch(1, 2));
        table.appendBatch(batch(3));
        assertEquals(2, table.batches().size());
        assertEquals(3, table.rowCount());
        IntVector v = (IntVector) table.batches().get(1).getVector("id");
        assertEquals(3, v.get(0));
    }

    @Test
    void emptyTableHasNoBatches() {
        assertEquals(0, table.rowCount());
        assertEquals(0, table.batches().size());
    }

    @Test
    void closeReleasesMemory() {
        table.appendBatch(batch(1));
        table.close();
        // RootAllocator.close() in tearDown throws if buffers leaked
        table = new ArrowTable(new TableSchema("t2", List.of()), allocator);
    }
}
