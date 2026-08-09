package com.minidb.server.exec;

import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public final class RowCopier {

    private RowCopier() {
    }

    public static FieldVector copyVector(FieldVector src, org.apache.arrow.memory.BufferAllocator allocator) {
        FieldVector dst = src.getField().createVector(allocator);
        dst.setInitialCapacity(src.getValueCount());
        dst.allocateNew();
        for (int i = 0; i < src.getValueCount(); i++) {
            dst.copyFromSafe(i, i, src);
        }
        dst.setValueCount(src.getValueCount());
        return dst;
    }

    public static void copyRow(FieldVector src, int srcRow, FieldVector dst, int dstRow) {
        dst.copyFromSafe(srcRow, dstRow, src);
    }

    public static void copyRow(VectorSchemaRoot src, int srcRow,
                               VectorSchemaRoot dst, int dstRow) {
        List<FieldVector> srcVectors = src.getFieldVectors();
        List<FieldVector> dstVectors = dst.getFieldVectors();
        for (int i = 0; i < srcVectors.size(); i++) {
            dstVectors.get(i).copyFromSafe(srcRow, dstRow, srcVectors.get(i));
        }
    }
}
