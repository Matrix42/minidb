package com.minidb.server.exec.functions;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Kernels.fill* 的纯 Arrow 单元测试(无 Calcite 规划):验证 op 应用与 null 传播,及 Function 分发。 */
class FunctionFrameworkTest {

    private BufferAllocator allocator;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        typeFactory = new JavaTypeFactoryImpl();
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

    /** Function.evaluate 应按输入向量类型选内核;输出向量已由 evaluate setValueCount,入参由 evaluate 关闭。 */
    @Test
    void functionDispatchPicksOverload() {
        Function twiceOrInc = new Function("twiceOrInc", List.of(
                new Overload(List.of(IntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillUnaryInt((IntVector) args.get(0), (IntVector) out, v -> v * 2)),
                new Overload(List.of(BigIntVector.class), BigIntVector.class,
                        (args, out) -> Kernels.fillUnaryLong((BigIntVector) args.get(0), (BigIntVector) out, v -> v + 1))));

        IntVector intIn = new IntVector("intIn", allocator);
        intIn.allocateNew(3);
        intIn.setSafe(0, -5);
        intIn.setSafe(1, 2);
        intIn.setNull(2);
        intIn.setValueCount(3);
        ValueVector intOut = twiceOrInc.evaluate(List.of(intIn),
                typeFactory.createSqlType(SqlTypeName.INTEGER), allocator);
        assertEquals(IntVector.class, intOut.getClass(), "应命中 int 重载并分配 IntVector 输出");
        assertEquals(3, intOut.getValueCount(), "evaluate 应统一 setValueCount");
        assertEquals(-10, ((IntVector) intOut).get(0));
        assertEquals(4, ((IntVector) intOut).get(1));
        assertTrue(intOut.isNull(2), "null 入参应传播为 null 输出");
        intOut.close(); // evaluate 已关闭 intIn,不能重复 close

        BigIntVector longIn = new BigIntVector("longIn", allocator);
        longIn.allocateNew(2);
        longIn.setSafe(0, 41L);
        longIn.setSafe(1, 0L);
        longIn.setValueCount(2);
        ValueVector longOut = twiceOrInc.evaluate(List.of(longIn),
                typeFactory.createSqlType(SqlTypeName.BIGINT), allocator);
        assertEquals(BigIntVector.class, longOut.getClass(), "应命中 long 重载并分配 BigIntVector 输出");
        assertEquals(42L, ((BigIntVector) longOut).get(0));
        assertEquals(1L, ((BigIntVector) longOut).get(1));
        longOut.close(); // evaluate 已关闭 longIn
    }

    /** 无匹配重载时应抛 UnsupportedOperationException 且不产出向量。 */
    @Test
    void unknownOverloadThrows() {
        Function twiceOrInc = new Function("twiceOrInc", List.of(
                new Overload(List.of(IntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillUnaryInt((IntVector) args.get(0), (IntVector) out, v -> v * 2)),
                new Overload(List.of(BigIntVector.class), BigIntVector.class,
                        (args, out) -> Kernels.fillUnaryLong((BigIntVector) args.get(0), (BigIntVector) out, v -> v + 1))));

        Float8Vector floatIn = new Float8Vector("floatIn", allocator);
        floatIn.allocateNew(1);
        floatIn.setValueCount(1);
        try {
            UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                    () -> twiceOrInc.evaluate(List.of(floatIn),
                            typeFactory.createSqlType(SqlTypeName.INTEGER), allocator));
            assertTrue(ex.getMessage().contains("no overload of twiceOrInc"),
                    "异常应指明函数名与参数类型");
        } finally {
            // resolve 在 evaluate 的 try/finally 之前抛,入参不会被 evaluate 关闭。
            floatIn.close();
        }
    }
}
