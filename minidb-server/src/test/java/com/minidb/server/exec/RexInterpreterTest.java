package com.minidb.server.exec;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
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
}
