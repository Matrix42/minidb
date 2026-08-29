package com.minidb.server.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RexInterpreterTest {

    BufferAllocator allocator;
    RelDataTypeFactory typeFactory;
    RexBuilder rex;
    RexInterpreter interpreter;
    VectorSchemaRoot input;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        typeFactory = new JavaTypeFactoryImpl();
        rex = new RexBuilder(typeFactory);
        interpreter = new RexInterpreter(allocator);

        Field a = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
        Field b = new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
        input = VectorSchemaRoot.create(new Schema(List.of(a, b)), allocator);
        input.allocateNew();
        IntVector va = (IntVector) input.getVector("a");
        IntVector vb = (IntVector) input.getVector("b");
        va.setSafe(0, 1);
        vb.setSafe(0, 2);
        va.setSafe(1, 5);
        vb.setSafe(1, 3);
        va.setNull(2);
        vb.setSafe(2, 9);
        input.setRowCount(3);
    }

    @AfterEach
    void tearDown() {
        input.close();
        allocator.close();
    }

    private RelDataType intType() {
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
    }

    @Test
    void inputRefCopiesColumn() {
        ValueVector out = interpreter.eval(rex.makeInputRef(intType(), 0), input);
        assertEquals(3, out.getValueCount());
        assertEquals(1, ((IntVector) out).get(0));
        assertTrue(out.isNull(2));
        out.close();
    }

    @Test
    void comparisonGreaterThan() {
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        rex.makeInputRef(intType(), 0),
                        rex.makeInputRef(intType(), 1));
        ValueVector out = interpreter.eval(expr, input);
        BitVector bits = (BitVector) out;
        assertEquals(0, bits.get(0));
        assertEquals(1, bits.get(1));
        assertTrue(bits.isNull(2));
        out.close();
    }

    @Test
    void arithmeticPlusWithLiteral() {
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rex.makeInputRef(intType(), 0),
                        rex.makeExactLiteral(java.math.BigDecimal.ONE, intType()));
        ValueVector out = interpreter.eval(expr, input);
        assertEquals(2, ((IntVector) out).get(0));
        assertEquals(6, ((IntVector) out).get(1));
        assertTrue(out.isNull(2));
        out.close();
    }

    @Test
    void andThreeValuedLogic() {
        RexNode gt =
                rex.makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        rex.makeInputRef(intType(), 0),
                        rex.makeExactLiteral(java.math.BigDecimal.ZERO, intType()));
        RexNode lt =
                rex.makeCall(
                        SqlStdOperatorTable.LESS_THAN,
                        rex.makeInputRef(intType(), 0),
                        rex.makeExactLiteral(java.math.BigDecimal.valueOf(3), intType()));
        RexNode and = rex.makeCall(SqlStdOperatorTable.AND, gt, lt);
        ValueVector out = interpreter.eval(and, input);
        BitVector bits = (BitVector) out;
        assertEquals(1, bits.get(0));
        assertEquals(0, bits.get(1));
        assertTrue(bits.isNull(2));
        out.close();
    }

    @Test
    void doubleArithmetic() {
        RelDataType dbl = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RexNode cast = rex.makeCast(dbl, rex.makeInputRef(intType(), 0));
        RexNode div =
                rex.makeCall(
                        SqlStdOperatorTable.DIVIDE,
                        cast,
                        rex.makeApproxLiteral(java.math.BigDecimal.valueOf(2), dbl));
        ValueVector out = interpreter.eval(div, input);
        Float8Vector d = (Float8Vector) out;
        assertEquals(0.5, d.get(0), 1e-9);
        assertEquals(2.5, d.get(1), 1e-9);
        assertTrue(d.isNull(2));
        out.close();
    }

    @Test
    void longArithmetic() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        Field la = new Field("la", FieldType.nullable(new ArrowType.Int(64, true)), List.of());
        Field lb = new Field("lb", FieldType.nullable(new ArrowType.Int(64, true)), List.of());
        VectorSchemaRoot longInput =
                VectorSchemaRoot.create(new Schema(List.of(la, lb)), allocator);
        longInput.allocateNew();
        BigIntVector vla = (BigIntVector) longInput.getVector("la");
        BigIntVector vlb = (BigIntVector) longInput.getVector("lb");
        vla.setSafe(0, 10L);
        vlb.setSafe(0, 3L);
        vla.setSafe(1, 100L);
        vlb.setSafe(1, 7L);
        vla.setNull(2);
        vlb.setSafe(2, 5L);
        longInput.setRowCount(3);

        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.MINUS,
                        rex.makeInputRef(bigint, 0),
                        rex.makeInputRef(bigint, 1));
        ValueVector out = interpreter.eval(expr, longInput);
        BigIntVector result = (BigIntVector) out;
        assertEquals(7L, result.get(0));
        assertEquals(93L, result.get(1));
        assertTrue(result.isNull(2));
        out.close();
        longInput.close();
    }

    @Test
    void literalMinusColumn() {
        // 字面量在左(整数字面量恒 BigIntVector,坑 #23)、列在右(IntVector),走反向跨型重载。
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.MINUS,
                        rex.makeExactLiteral(java.math.BigDecimal.valueOf(100), intType()),
                        rex.makeInputRef(intType(), 0));
        ValueVector out = interpreter.eval(expr, input);
        assertEquals(99, ((IntVector) out).get(0));
        assertEquals(95, ((IntVector) out).get(1));
        assertTrue(out.isNull(2));
        out.close();
    }

    @Test
    void stringComparison() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        Field sa = new Field("sa", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of());
        Field sb = new Field("sb", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of());
        VectorSchemaRoot strInput = VectorSchemaRoot.create(new Schema(List.of(sa, sb)), allocator);
        strInput.allocateNew();
        VarCharVector vsa = (VarCharVector) strInput.getVector("sa");
        VarCharVector vsb = (VarCharVector) strInput.getVector("sb");
        vsa.setSafe(0, "foo".getBytes(StandardCharsets.UTF_8));
        vsb.setSafe(0, "foo".getBytes(StandardCharsets.UTF_8));
        vsa.setSafe(1, "bar".getBytes(StandardCharsets.UTF_8));
        vsb.setSafe(1, "baz".getBytes(StandardCharsets.UTF_8));
        vsa.setNull(2);
        vsb.setSafe(2, "foo".getBytes(StandardCharsets.UTF_8));
        strInput.setRowCount(3);

        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        rex.makeInputRef(varchar, 0),
                        rex.makeInputRef(varchar, 1));
        ValueVector out = interpreter.eval(expr, strInput);
        BitVector bits = (BitVector) out;
        assertEquals(1, bits.get(0));
        assertEquals(0, bits.get(1));
        assertTrue(bits.isNull(2));
        out.close();
        strInput.close();
    }

    @Test
    void literalDivideColumn() {
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.DIVIDE,
                        rex.makeExactLiteral(java.math.BigDecimal.valueOf(10), intType()),
                        rex.makeInputRef(intType(), 0));
        ValueVector out = interpreter.eval(expr, input);
        assertEquals(10, ((IntVector) out).get(0));
        assertEquals(2, ((IntVector) out).get(1));
        assertTrue(out.isNull(2));
        out.close();
    }

    private RelDataType varcharType() {
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    /** 单列 VarCharVector 输入:["Ab", " x ", "abc", null],供字符串函数测试含 null 的 STRICT 语义。 */
    private VectorSchemaRoot varcharInput() {
        Field s = new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of());
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(s)), allocator);
        root.allocateNew();
        VarCharVector v = (VarCharVector) root.getVector("s");
        v.setSafe(0, "Ab".getBytes(StandardCharsets.UTF_8));
        v.setSafe(1, " x ".getBytes(StandardCharsets.UTF_8));
        v.setSafe(2, "abc".getBytes(StandardCharsets.UTF_8));
        v.setNull(3);
        root.setRowCount(4);
        return root;
    }

    private static String varchar(VarCharVector v, int i) {
        return new String(v.get(i), StandardCharsets.UTF_8);
    }

    @Test
    void stringUpper() {
        VectorSchemaRoot root = varcharInput();
        RexNode expr = rex.makeCall(SqlStdOperatorTable.UPPER, rex.makeInputRef(varcharType(), 0));
        ValueVector out = interpreter.eval(expr, root);
        VarCharVector v = (VarCharVector) out;
        assertEquals("AB", varchar(v, 0));
        assertEquals(" X ", varchar(v, 1));
        assertEquals("ABC", varchar(v, 2));
        assertTrue(v.isNull(3));
        out.close();
        root.close();
    }

    @Test
    void stringLower() {
        VectorSchemaRoot root = varcharInput();
        RexNode expr = rex.makeCall(SqlStdOperatorTable.LOWER, rex.makeInputRef(varcharType(), 0));
        ValueVector out = interpreter.eval(expr, root);
        VarCharVector v = (VarCharVector) out;
        assertEquals("ab", varchar(v, 0));
        assertEquals(" x ", varchar(v, 1));
        assertEquals("abc", varchar(v, 2));
        assertTrue(v.isNull(3));
        out.close();
        root.close();
    }

    @Test
    void stringTrim() {
        VectorSchemaRoot root = varcharInput();
        // TRIM 是 RexInterpreter 的专用 handler:Calcite 把 `TRIM(s)` 解析期重写为
        // `TRIM(Flag, ' ', s)`,Flag 是 SYMBOL 字面量。这里直接构造 3 参形式。
        RexNode expr =
                rex.makeCall(
                        varcharType(),
                        SqlStdOperatorTable.TRIM,
                        List.of(
                                rex.makeFlag(SqlTrimFunction.Flag.BOTH),
                                rex.makeLiteral(" "),
                                rex.makeInputRef(varcharType(), 0)));
        ValueVector out = interpreter.eval(expr, root);
        VarCharVector v = (VarCharVector) out;
        assertEquals("Ab", varchar(v, 0));
        assertEquals("x", varchar(v, 1));
        assertEquals("abc", varchar(v, 2));
        assertTrue(v.isNull(3));
        out.close();
        root.close();
    }

    @Test
    void stringLength() {
        VectorSchemaRoot root = varcharInput();
        RexNode expr =
                rex.makeCall(SqlStdOperatorTable.CHAR_LENGTH, rex.makeInputRef(varcharType(), 0));
        ValueVector out = interpreter.eval(expr, root);
        IntVector v = (IntVector) out;
        assertEquals(2, v.get(0));
        assertEquals(3, v.get(1));
        assertEquals(3, v.get(2));
        assertTrue(v.isNull(3));
        out.close();
        root.close();
    }

    @Test
    void stringConcat() {
        VectorSchemaRoot root = varcharInput();
        // 字面量路径:'a' || 'b' → 'ab'。
        RexNode litExpr =
                rex.makeCall(
                        SqlStdOperatorTable.CONCAT,
                        rex.makeLiteral("a", varcharType()),
                        rex.makeLiteral("b", varcharType()));
        ValueVector litOut = interpreter.eval(litExpr, root);
        assertEquals("ab", varchar((VarCharVector) litOut, 0));
        litOut.close();

        // 列路径:null 行 STRICT 传播。
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.CONCAT,
                        rex.makeInputRef(varcharType(), 0),
                        rex.makeInputRef(varcharType(), 0));
        ValueVector out = interpreter.eval(expr, root);
        VarCharVector v = (VarCharVector) out;
        assertEquals("AbAb", varchar(v, 0));
        assertTrue(v.isNull(3));
        out.close();
        root.close();
    }

    @Test
    void stringSubstring() {
        VectorSchemaRoot root = varcharInput();
        // 第二/三参是整数字面量 → BigIntVector(坑 #23),走 [VarChar,BigInt,BigInt] 重载。
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.SUBSTRING,
                        rex.makeInputRef(varcharType(), 0),
                        rex.makeExactLiteral(java.math.BigDecimal.ONE, intType()),
                        rex.makeExactLiteral(java.math.BigDecimal.valueOf(2), intType()));
        ValueVector out = interpreter.eval(expr, root);
        VarCharVector v = (VarCharVector) out;
        assertEquals("Ab", varchar(v, 0));
        assertEquals(" x", varchar(v, 1));
        assertEquals("ab", varchar(v, 2));
        assertTrue(v.isNull(3));
        out.close();
        root.close();
    }

    private RelDataType doubleType() {
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    }

    /** 单列 Float8Vector 输入:[-2.5, 2.7, null],供数学函数测试含 null 的 STRICT 语义。 */
    private VectorSchemaRoot doubleInput() {
        Field d =
                new Field(
                        "d",
                        FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        List.of());
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(d)), allocator);
        root.allocateNew();
        Float8Vector v = (Float8Vector) root.getVector("d");
        v.setSafe(0, -2.5);
        v.setSafe(1, 2.7);
        v.setNull(2);
        root.setRowCount(3);
        return root;
    }

    @Test
    void absInteger() {
        // 单列 IntVector:[-3, 7, null],ABS 逐行取绝对值,null 行 STRICT 传播。
        Field f = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(f)), allocator);
        root.allocateNew();
        IntVector v = (IntVector) root.getVector("a");
        v.setSafe(0, -3);
        v.setSafe(1, 7);
        v.setNull(2);
        root.setRowCount(3);

        RexNode expr = rex.makeCall(SqlStdOperatorTable.ABS, rex.makeInputRef(intType(), 0));
        ValueVector out = interpreter.eval(expr, root);
        IntVector result = (IntVector) out;
        assertEquals(3, result.get(0));
        assertEquals(7, result.get(1));
        assertTrue(result.isNull(2));
        out.close();
        root.close();
    }

    @Test
    void absIntegerLiteral() {
        // INTEGER 字面量经 literalVector 恒产 BigIntVector(坑 #23),命中 ABS 的 [BigIntVector]
        // 重载;但 call.getType() 仍为 INTEGER,Function.evaluate 分配 IntVector 输出。核内必须按
        // out 实际类型写入(IntVector 分支),否则 (BigIntVector) out 强转失败。
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.ABS,
                        rex.makeExactLiteral(java.math.BigDecimal.valueOf(-3), intType()));
        ValueVector out = interpreter.eval(expr, input);
        assertTrue(out instanceof IntVector, "ABS(<int literal>) 结果类型是 INTEGER,输出应为 IntVector");
        IntVector result = (IntVector) out;
        assertEquals(3, result.getValueCount());
        assertEquals(3, result.get(0));
        assertEquals(3, result.get(1));
        assertEquals(3, result.get(2));
        out.close();

        // null 字面量同样产 BigIntVector(全 null),STRICT 应传播为 IntVector 全 null。
        RexNode nullExpr = rex.makeCall(SqlStdOperatorTable.ABS, rex.makeNullLiteral(intType()));
        ValueVector nullOut = interpreter.eval(nullExpr, input);
        IntVector nullResult = (IntVector) nullOut;
        assertTrue(nullResult.isNull(0));
        assertTrue(nullResult.isNull(1));
        assertTrue(nullResult.isNull(2));
        nullOut.close();
    }

    @Test
    void absDouble() {
        VectorSchemaRoot root = doubleInput();
        RexNode expr = rex.makeCall(SqlStdOperatorTable.ABS, rex.makeInputRef(doubleType(), 0));
        ValueVector out = interpreter.eval(expr, root);
        Float8Vector result = (Float8Vector) out;
        assertEquals(2.5, result.get(0), 1e-9);
        assertEquals(2.7, result.get(1), 1e-9);
        assertTrue(result.isNull(2));
        out.close();
        root.close();
    }

    @Test
    void floorCeil() {
        VectorSchemaRoot root = doubleInput();
        // FLOOR(2.7)→2.0、CEIL(-2.5)→-2.0,Math.floor/ceil 返回 double,fillUnaryDouble 原样写入。
        RexNode floor = rex.makeCall(SqlStdOperatorTable.FLOOR, rex.makeInputRef(doubleType(), 0));
        ValueVector floorOut = interpreter.eval(floor, root);
        Float8Vector f = (Float8Vector) floorOut;
        assertEquals(2.0, f.get(1), 1e-9);
        assertEquals(-3.0, f.get(0), 1e-9);
        assertTrue(f.isNull(2));
        floorOut.close();

        RexNode ceil = rex.makeCall(SqlStdOperatorTable.CEIL, rex.makeInputRef(doubleType(), 0));
        ValueVector ceilOut = interpreter.eval(ceil, root);
        Float8Vector c = (Float8Vector) ceilOut;
        assertEquals(-2.0, c.get(0), 1e-9);
        assertEquals(3.0, c.get(1), 1e-9);
        assertTrue(c.isNull(2));
        ceilOut.close();
        root.close();
    }
}
