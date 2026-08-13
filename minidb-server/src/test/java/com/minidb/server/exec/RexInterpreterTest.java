package com.minidb.server.exec;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

        Field a = new Field("a", FieldType.nullable(
                new ArrowType.Int(32, true)), List.of());
        Field b = new Field("b", FieldType.nullable(
                new ArrowType.Int(32, true)), List.of());
        input = VectorSchemaRoot.create(new Schema(List.of(a, b)), allocator);
        input.allocateNew();
        IntVector va = (IntVector) input.getVector("a");
        IntVector vb = (IntVector) input.getVector("b");
        va.setSafe(0, 1); vb.setSafe(0, 2);
        va.setSafe(1, 5); vb.setSafe(1, 3);
        va.setNull(2);    vb.setSafe(2, 9);
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
        RexNode expr = rex.makeCall(SqlStdOperatorTable.GREATER_THAN,
                rex.makeInputRef(intType(), 0), rex.makeInputRef(intType(), 1));
        ValueVector out = interpreter.eval(expr, input);
        BitVector bits = (BitVector) out;
        assertEquals(0, bits.get(0));
        assertEquals(1, bits.get(1));
        assertTrue(bits.isNull(2));
        out.close();
    }

    @Test
    void arithmeticPlusWithLiteral() {
        RexNode expr = rex.makeCall(SqlStdOperatorTable.PLUS,
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
        RexNode gt = rex.makeCall(SqlStdOperatorTable.GREATER_THAN,
                rex.makeInputRef(intType(), 0),
                rex.makeExactLiteral(java.math.BigDecimal.ZERO, intType()));
        RexNode lt = rex.makeCall(SqlStdOperatorTable.LESS_THAN,
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
        RexNode div = rex.makeCall(SqlStdOperatorTable.DIVIDE,
                cast, rex.makeApproxLiteral(java.math.BigDecimal.valueOf(2), dbl));
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
        VectorSchemaRoot longInput = VectorSchemaRoot.create(new Schema(List.of(la, lb)), allocator);
        longInput.allocateNew();
        BigIntVector vla = (BigIntVector) longInput.getVector("la");
        BigIntVector vlb = (BigIntVector) longInput.getVector("lb");
        vla.setSafe(0, 10L); vlb.setSafe(0, 3L);
        vla.setSafe(1, 100L); vlb.setSafe(1, 7L);
        vla.setNull(2);      vlb.setSafe(2, 5L);
        longInput.setRowCount(3);

        RexNode expr = rex.makeCall(SqlStdOperatorTable.MINUS,
                rex.makeInputRef(bigint, 0), rex.makeInputRef(bigint, 1));
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
        RexNode expr = rex.makeCall(SqlStdOperatorTable.MINUS,
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

        RexNode expr = rex.makeCall(SqlStdOperatorTable.EQUALS,
                rex.makeInputRef(varchar, 0), rex.makeInputRef(varchar, 1));
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
        RexNode expr = rex.makeCall(SqlStdOperatorTable.DIVIDE,
                rex.makeExactLiteral(java.math.BigDecimal.valueOf(10), intType()),
                rex.makeInputRef(intType(), 0));
        ValueVector out = interpreter.eval(expr, input);
        assertEquals(10, ((IntVector) out).get(0));
        assertEquals(2, ((IntVector) out).get(1));
        assertTrue(out.isNull(2));
        out.close();
    }
}
