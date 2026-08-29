package com.minidb.server.plan.physical;

import com.minidb.server.exec.RowCopier;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowVectorsTest {
    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator();
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    @Test
    void readWriteRoundTripsNewTypes() {
        List<FieldVector> vecs =
                List.of(
                        ArrowTypes.field(new ColumnMeta("s", ColumnType.SMALLINT))
                                .createVector(allocator),
                        ArrowTypes.field(new ColumnMeta("p", ColumnType.DECIMAL, 10, 2))
                                .createVector(allocator),
                        ArrowTypes.field(new ColumnMeta("t", ColumnType.TIME))
                                .createVector(allocator),
                        ArrowTypes.field(new ColumnMeta("b", ColumnType.VARBINARY))
                                .createVector(allocator));
        for (FieldVector v : vecs) {
            v.setInitialCapacity(1);
            v.allocateNew();
        }
        ((SmallIntVector) vecs.get(0)).setSafe(0, (short) 42);
        ((DecimalVector) vecs.get(1)).setSafe(0, new BigDecimal("1.23"));
        ((TimeMilliVector) vecs.get(2)).setSafe(0, 45296000); // 12:34:56
        ((VarBinaryVector) vecs.get(3)).setSafe(0, new byte[] {1, 2, 3});
        for (FieldVector v : vecs) {
            v.setValueCount(1);
        }
        VectorSchemaRoot root = VectorSchemaRoot.of(vecs.toArray(new FieldVector[0]));
        root.setRowCount(1);
        try {
            assertEquals((short) 42, RowVectors.readObject(root.getVector(0), 0));
            assertEquals(new BigDecimal("1.23"), RowVectors.readObject(root.getVector(1), 0));
            assertEquals(45296000, RowVectors.readObject(root.getVector(2), 0));
            assertArrayEquals(
                    new byte[] {1, 2, 3}, (byte[]) RowVectors.readObject(root.getVector(3), 0));
        } finally {
            root.close();
        }
    }

    @Test
    void rowCopierWritesAndNullsNewTypes() {
        // writeObject round-trips the remaining native vector (Float4), then
        // RowCopier.writeValue must coerce between the new types and null them
        // without tripping the "unsupported vector for null" path.
        DecimalVector dstDecimal =
                (DecimalVector)
                        ArrowTypes.field(new ColumnMeta("d", ColumnType.DECIMAL, 10, 2))
                                .createVector(allocator);
        dstDecimal.setInitialCapacity(2);
        dstDecimal.allocateNew();
        DecimalVector srcDecimal =
                (DecimalVector)
                        ArrowTypes.field(new ColumnMeta("d", ColumnType.DECIMAL, 10, 2))
                                .createVector(allocator);
        srcDecimal.setInitialCapacity(2);
        srcDecimal.allocateNew();
        srcDecimal.setSafe(0, new BigDecimal("1.23"));
        srcDecimal.setNull(1);
        srcDecimal.setValueCount(2);
        try {
            RowCopier.writeValue(dstDecimal, 0, srcDecimal, 0);
            RowCopier.writeValue(dstDecimal, 1, srcDecimal, 1);
        } finally {
            srcDecimal.close();
        }
        dstDecimal.setValueCount(2);
        assertEquals(new BigDecimal("1.23"), dstDecimal.getObject(0));
        assertTrue(dstDecimal.isNull(1), "DECIMAL null must survive writeValue");
        dstDecimal.close();

        // SmallInt -> Int coercion (mixed-type CASE), including a null branch.
        SmallIntVector srcSmall = new SmallIntVector("s", allocator);
        srcSmall.setInitialCapacity(2);
        srcSmall.allocateNew();
        srcSmall.setSafe(0, (short) 7);
        srcSmall.setNull(1);
        srcSmall.setValueCount(2);
        org.apache.arrow.vector.IntVector dstInt =
                new org.apache.arrow.vector.IntVector("i", allocator);
        dstInt.setInitialCapacity(2);
        dstInt.allocateNew();
        try {
            RowCopier.writeValue(dstInt, 0, srcSmall, 0);
            RowCopier.writeValue(dstInt, 1, srcSmall, 1);
        } finally {
            srcSmall.close();
        }
        dstInt.setValueCount(2);
        assertEquals(7, dstInt.get(0));
        assertTrue(dstInt.isNull(1), "null SmallInt coerced into Int must be null");
        dstInt.close();

        // Float4 -> Double coercion (readDouble must accept Float4Vector).
        Float4Vector srcFloat = new Float4Vector("f", allocator);
        srcFloat.setInitialCapacity(1);
        srcFloat.allocateNew();
        srcFloat.setSafe(0, 1.5f);
        srcFloat.setValueCount(1);
        org.apache.arrow.vector.Float8Vector dstDouble =
                new org.apache.arrow.vector.Float8Vector("d", allocator);
        dstDouble.setInitialCapacity(1);
        dstDouble.allocateNew();
        try {
            RowCopier.writeValue(dstDouble, 0, srcFloat, 0);
        } finally {
            srcFloat.close();
        }
        dstDouble.setValueCount(1);
        assertEquals(1.5, dstDouble.get(0), 1e-9);
        dstDouble.close();

        // TimeMilliVector / VarBinaryVector null via setNull.
        TimeMilliVector dstTime = new TimeMilliVector("t", allocator);
        dstTime.setInitialCapacity(1);
        dstTime.allocateNew();
        dstTime.setNull(0);
        dstTime.setValueCount(1);
        assertTrue(dstTime.isNull(0));
        dstTime.close();

        VarBinaryVector dstBin = new VarBinaryVector("b", allocator);
        dstBin.setInitialCapacity(1);
        dstBin.allocateNew();
        dstBin.setNull(0);
        dstBin.setValueCount(1);
        assertTrue(dstBin.isNull(0));
        dstBin.close();
    }
}
