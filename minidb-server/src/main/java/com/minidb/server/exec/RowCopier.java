package com.minidb.server.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

public final class RowCopier {

    private RowCopier() {
    }

    public static FieldVector copyVector(FieldVector src, BufferAllocator allocator) {
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
}
