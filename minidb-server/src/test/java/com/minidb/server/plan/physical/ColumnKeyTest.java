package com.minidb.server.plan.physical;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * ColumnKey.equals 必须先校验 cols 数组长度:长度不同必不相等,否则按本对象长度迭代
 * 会越界或漏比较(哈希值碰撞时触发)。
 */
class ColumnKeyTest {

    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator();
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    private static VectorSchemaRoot root(int... values) {
        IntVector v = new IntVector("c", allocator);
        v.setInitialCapacity(values.length);
        v.allocateNew();
        for (int i = 0; i < values.length; i++) {
            v.setSafe(i, values[i]);
        }
        v.setValueCount(values.length);
        return VectorSchemaRoot.of(v);
    }

    @Test
    void differentKeyArityNeverEqual() {
        try (VectorSchemaRoot single = root(1)) {
            // 构造双列 root:[1] 与 [2]
            IntVector v0 = new IntVector("c0", allocator);
            IntVector v1 = new IntVector("c1", allocator);
            v0.setInitialCapacity(1);
            v1.setInitialCapacity(1);
            v0.allocateNew();
            v1.allocateNew();
            v0.setSafe(0, 1);
            v1.setSafe(0, 2);
            v0.setValueCount(1);
            v1.setValueCount(1);
            VectorSchemaRoot multi = VectorSchemaRoot.of(v0, v1);
            multi.setRowCount(1);
            try {
                // 单列键 [1] 与双列键 [1,2] 的 src/other 互换各测一次,
                // 避免只测单方向时「以短迭代长」侥幸通过。
                ColumnKey oneCol = new ColumnKey(single, 0, new int[]{0});
                ColumnKey twoCols = new ColumnKey(multi, 0, new int[]{0, 1});
                assertFalse(oneCol.equals(twoCols));
                assertFalse(twoCols.equals(oneCol));
            } finally {
                multi.close();
            }
        }
    }
}