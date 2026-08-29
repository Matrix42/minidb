package com.minidb.server.stats;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistogramBuilderTest {

    @Test
    void buildsEquiDepthBucketsAndMcv() {
        try (BufferAllocator alloc = new RootAllocator();
                IntVector v = new IntVector("x", alloc)) {
            v.allocateNew(20);
            // values 1..4 each 5x = 20 rows, distinct 4
            int idx = 0;
            for (int val = 1; val <= 4; val++) {
                for (int i = 0; i < 5; i++) {
                    v.setSafe(idx++, val);
                }
            }
            v.setValueCount(20);
            Histogram h =
                    HistogramBuilder.build(
                            List.of(v), com.minidb.storage.common.ColumnType.INTEGER);
            assertEquals(20, h.totalRows());
            assertEquals(4, h.distinctCount());
            // each value appears 5 times; MCV should capture them
            assertTrue(h.mcv().size() <= 10);
            assertEquals(5, h.mcv().get(0).frequency());
        }
    }

    @Test
    void countsNullsSeparately() {
        try (BufferAllocator alloc = new RootAllocator();
                IntVector v = new IntVector("x", alloc)) {
            v.allocateNew(4);
            v.setSafe(0, 1);
            v.setSafe(1, 2);
            v.setNull(2);
            v.setNull(3);
            v.setValueCount(4);
            Histogram h =
                    HistogramBuilder.build(
                            List.of(v), com.minidb.storage.common.ColumnType.INTEGER);
            assertEquals(2, h.nullCount());
            assertEquals(2, h.totalRows());
            assertEquals(2, h.distinctCount());
        }
    }

    @Test
    void emptyColumnProducesEmptyHistogram() {
        try (BufferAllocator alloc = new RootAllocator();
                IntVector v = new IntVector("x", alloc)) {
            v.allocateNew(0);
            v.setValueCount(0);
            Histogram h =
                    HistogramBuilder.build(
                            List.of(v), com.minidb.storage.common.ColumnType.INTEGER);
            assertEquals(0, h.totalRows());
            assertTrue(h.buckets().isEmpty());
        }
    }

    @Test
    void handlesVarCharColumn() {
        try (BufferAllocator alloc = new RootAllocator();
                VarCharVector v = new VarCharVector("s", alloc)) {
            v.allocateNew(6);
            v.setSafe(0, "a".getBytes());
            v.setSafe(1, "a".getBytes());
            v.setSafe(2, "b".getBytes());
            v.setSafe(3, "b".getBytes());
            v.setSafe(4, "c".getBytes());
            v.setSafe(5, "c".getBytes());
            v.setValueCount(6);
            Histogram h =
                    HistogramBuilder.build(
                            List.of(v), com.minidb.storage.common.ColumnType.VARCHAR);
            assertEquals(6, h.totalRows());
            assertEquals(3, h.distinctCount());
        }
    }
}
