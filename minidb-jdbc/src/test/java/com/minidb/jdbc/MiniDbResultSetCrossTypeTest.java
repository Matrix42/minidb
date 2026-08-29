package com.minidb.jdbc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * JDBC 跨类型读取:getString/getDouble/getBoolean/getBigDecimal/getByte 是通用 getter,
 * 应在数值/日期列上做类型转换而非抛异常(JDBC 规范)。
 */
class MiniDbResultSetCrossTypeTest {

    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator();
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    /** 单列 root:INT(123) / BIGINT(456L) / FLOAT8 / FLOAT4 / SMALLINT / DATEDAY。 */
    private static VectorSchemaRoot newRoot(String name, Types.MinorType type, Object value) {
        FieldVector vector = allocField(name, type);
        vector.setInitialCapacity(1);
        vector.allocateNew();
        switch (type) {
            case INT -> ((IntVector) vector).setSafe(0, (Integer) value);
            case BIGINT -> ((BigIntVector) vector).setSafe(0, (Long) value);
            case FLOAT8 -> ((Float8Vector) vector).setSafe(0, (Double) value);
            case FLOAT4 -> ((Float4Vector) vector).setSafe(0, (Float) value);
            case SMALLINT -> ((SmallIntVector) vector).setSafe(0, (Short) value);
            case DATEDAY -> ((DateDayVector) vector).setSafe(0, (Integer) value);
            default -> throw new IllegalArgumentException("unsupported: " + type);
        }
        vector.setValueCount(1);
        return VectorSchemaRoot.of(vector);
    }

    private static FieldVector allocField(String name, Types.MinorType type) {
        return switch (type) {
            case INT -> new IntVector(name, allocator);
            case BIGINT -> new BigIntVector(name, allocator);
            case FLOAT8 -> new Float8Vector(name, allocator);
            case FLOAT4 -> new Float4Vector(name, allocator);
            case SMALLINT -> new SmallIntVector(name, allocator);
            case DATEDAY -> new DateDayVector(name, allocator);
            default -> throw new IllegalArgumentException("unsupported: " + type);
        };
    }

    @Test
    void getStringOnNumericColumns() throws Exception {
        try (VectorSchemaRoot root = newRoot("i", Types.MinorType.INT, 123)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals("123", rs.getString(1));
        }
        try (VectorSchemaRoot root = newRoot("l", Types.MinorType.BIGINT, 456L)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals("456", rs.getString(1));
        }
        try (VectorSchemaRoot root = newRoot("d", Types.MinorType.FLOAT8, 12.5)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals("12.5", rs.getString(1));
        }
    }

    @Test
    void getDoubleOnRealAndSmallInt() throws Exception {
        try (VectorSchemaRoot root = newRoot("f", Types.MinorType.FLOAT4, 1.5f)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals(1.5, rs.getDouble(1), 1e-9);
        }
        try (VectorSchemaRoot root = newRoot("s", Types.MinorType.SMALLINT, (short) 42)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals(42.0, rs.getDouble(1), 1e-9);
        }
    }

    @Test
    void getBooleanOnNumericColumns() throws Exception {
        try (VectorSchemaRoot root = newRoot("i", Types.MinorType.INT, 7)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals(true, rs.getBoolean(1));
        }
        try (VectorSchemaRoot root = newRoot("l", Types.MinorType.BIGINT, 0L)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals(false, rs.getBoolean(1));
        }
    }

    @Test
    void getBigDecimalOnIntegralColumns() throws Exception {
        try (VectorSchemaRoot root = newRoot("i", Types.MinorType.INT, 123)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals(0, BigDecimal.valueOf(123).compareTo(rs.getBigDecimal(1)));
        }
        try (VectorSchemaRoot root = newRoot("l", Types.MinorType.BIGINT, 456L)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals(0, BigDecimal.valueOf(456).compareTo(rs.getBigDecimal(1)));
        }
        try (VectorSchemaRoot root = newRoot("s", Types.MinorType.SMALLINT, (short) 9)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals(0, BigDecimal.valueOf(9).compareTo(rs.getBigDecimal(1)));
        }
    }

    @Test
    void getByteOnIntegralColumns() throws Exception {
        try (VectorSchemaRoot root = newRoot("s", Types.MinorType.SMALLINT, (short) 100)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals((byte) 100, rs.getByte(1));
        }
        try (VectorSchemaRoot root = newRoot("i", Types.MinorType.INT, 100)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            assertEquals((byte) 100, rs.getByte(1));
        }
    }

    @Test
    void getByteOverflowIsSignalled() throws Exception {
        try (VectorSchemaRoot root = newRoot("l", Types.MinorType.BIGINT, 300L)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            try {
                rs.getByte(1);
                throw new AssertionError("expected SQLException for out-of-range");
            } catch (SQLException expected) {
                // expected
            }
        }
    }

    @Test
    void getObjectWithIncompatibleClassThrowsSqlException() throws Exception {
        try (VectorSchemaRoot root = newRoot("i", Types.MinorType.INT, 123)) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            rs.next();
            try {
                rs.getObject(1, java.sql.Date.class);
                throw new AssertionError("expected SQLException for incompatible type");
            } catch (SQLException expected) {
                // expected
            }
        }
    }
}
