package com.minidb.server.exec.functions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Kernels.fill* 的纯 Arrow 单元测试(无 Calcite 规划):验证 op 应用与 null 传播。 */
class FunctionFrameworkTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void fillUnaryInt() {
        IntVector in = new IntVector("in", allocator);
        in.allocateNew(3);
        in.setSafe(0, -5);
        in.setSafe(1, 2);
        in.setNull(2);
        in.setValueCount(3);

        IntVector out = new IntVector("out", allocator);
        out.allocateNew(3);

        Kernels.fillUnaryInt(in, out, Math::abs);
        // 内核契约:fill* 只写值不 setValueCount,由调用方统一 setValueCount。
        out.setValueCount(3);

        assertEquals(3, out.getValueCount());
        assertEquals(5, out.get(0), "op 应作用于非 null 行");
        assertEquals(2, out.get(1));
        assertTrue(out.isNull(2), "null 入参应传播为 null 输出");
        out.close();
        in.close();
    }

    @Test
    void fillCompareInt() {
        IntVector l = new IntVector("l", allocator);
        l.allocateNew(4);
        l.setSafe(0, 1);
        l.setSafe(1, 5);
        l.setNull(2);
        l.setSafe(3, 3);
        l.setValueCount(4);

        IntVector r = new IntVector("r", allocator);
        r.allocateNew(4);
        r.setSafe(0, 1);
        r.setSafe(1, 3);
        r.setSafe(2, 9);
        r.setSafe(3, 5);
        r.setValueCount(4);

        BitVector out = new BitVector("out", allocator);
        out.allocateNew(4);

        ScalarKernels.IntCompare cmp = Integer::compare;

        Kernels.fillCompareInt(l, r, out, cmp, SqlKind.EQUALS);
        out.setValueCount(4);
        assertEquals(1, out.get(0));
        assertEquals(0, out.get(1));
        assertTrue(out.isNull(2), "任一侧 null → 结果 null");
        assertEquals(0, out.get(3));

        Kernels.fillCompareInt(l, r, out, cmp, SqlKind.GREATER_THAN);
        out.setValueCount(4);
        assertEquals(0, out.get(0));
        assertEquals(1, out.get(1));
        assertTrue(out.isNull(2));
        assertEquals(0, out.get(3));

        Kernels.fillCompareInt(l, r, out, cmp, SqlKind.LESS_THAN);
        out.setValueCount(4);
        assertEquals(0, out.get(0));
        assertEquals(0, out.get(1));
        assertTrue(out.isNull(2));
        assertEquals(1, out.get(3));

        out.close();
        l.close();
        r.close();
    }
}
