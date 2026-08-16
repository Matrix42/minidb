package com.minidb.server.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.storage.common.BatchIterator;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PaginatorTest {

    private final RootAllocator allocator = new RootAllocator();

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private static Schema schema() {
        return new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    private VectorSchemaRoot root(int... values) {
        IntVector v = new IntVector("id", allocator);
        v.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            v.setSafe(i, values[i]);
        }
        v.setValueCount(values.length);
        return new VectorSchemaRoot(List.of(v));
    }

    private static BatchIterator iterator(VectorSchemaRoot... batches) {
        return new BatchIterator() {
            int i = 0;
            @Override public boolean hasNext() { return i < batches.length; }
            @Override public VectorSchemaRoot next() { return batches[i++]; }
            @Override public void close() {}
        };
    }

    @Test
    void slicesAcrossBatchBoundaries() {
        Paginator p = new Paginator(iterator(root(1, 2, 3), root(4, 5)), schema(), allocator);
        VectorSchemaRoot page1 = p.nextPage(2);
        assertEquals(2, page1.getRowCount());
        assertEquals(1, ((IntVector) page1.getVector(0)).get(0));
        assertEquals(2, ((IntVector) page1.getVector(0)).get(1));
        assertFalse(p.isDone());
        page1.close();

        VectorSchemaRoot page2 = p.nextPage(2);
        assertEquals(2, page2.getRowCount());
        assertEquals(3, ((IntVector) page2.getVector(0)).get(0));
        assertEquals(4, ((IntVector) page2.getVector(0)).get(1));
        assertFalse(p.isDone());
        page2.close();

        VectorSchemaRoot page3 = p.nextPage(2);
        assertEquals(1, page3.getRowCount());
        assertEquals(5, ((IntVector) page3.getVector(0)).get(0));
        assertTrue(p.isDone());
        page3.close();
        p.close();
    }

    @Test
    void emptyInputReturnsSingleEmptyPage() {
        Paginator p = new Paginator(iterator(), schema(), allocator);
        VectorSchemaRoot page = p.nextPage(10);
        assertNotNull(page);
        assertEquals(0, page.getRowCount());
        assertEquals(1, page.getFieldVectors().size());
        assertTrue(p.isDone());
        page.close();
        assertNull(p.nextPage(10));
        p.close();
    }

    @Test
    void exactMultipleOfPageSize() {
        Paginator p = new Paginator(iterator(root(1, 2, 3, 4)), schema(), allocator);
        VectorSchemaRoot page1 = p.nextPage(2);
        assertEquals(2, page1.getRowCount());
        assertFalse(p.isDone());
        page1.close();
        VectorSchemaRoot page2 = p.nextPage(2);
        assertEquals(2, page2.getRowCount());
        assertTrue(p.isDone());
        page2.close();
        p.close();
    }
}
