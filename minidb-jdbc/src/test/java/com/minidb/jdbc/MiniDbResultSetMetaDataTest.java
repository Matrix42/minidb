package com.minidb.jdbc;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 客户端侧验证:ResultSetMetaData 优先读 Field 的 "minidb.type" 元数据(类型名端到端保真),
 * 以及 ResultSet 能从新增的原生 Arrow 向量读回值。用 RootAllocator 直接构造 Arrow 向量,
 * 不走网络(该模块不依赖 minidb-server 类)。
 */
class MiniDbResultSetMetaDataTest {

    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator();
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    private static Field field(String name, ArrowType type, String typeName) {
        return new Field(name,
                new FieldType(true, type, null, Map.of("minidb.type", typeName)),
                List.of());
    }

    private static VectorSchemaRoot newRoot() {
        List<FieldVector> vecs = List.of(
                field("c_smallint", new ArrowType.Int(16, true), "SMALLINT").createVector(allocator),
                field("c_real", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), "REAL").createVector(allocator),
                field("c_decimal", new ArrowType.Decimal(10, 2, 128), "DECIMAL").createVector(allocator),
                field("c_time", new ArrowType.Time(TimeUnit.MILLISECOND, 32), "TIME").createVector(allocator),
                field("c_varbinary", ArrowType.Binary.INSTANCE, "VARBINARY").createVector(allocator));
        for (FieldVector v : vecs) {
            v.setInitialCapacity(1);
            v.allocateNew();
        }
        ((SmallIntVector) vecs.get(0)).setSafe(0, (short) 123);
        ((Float4Vector) vecs.get(1)).setSafe(0, 1.5f);
        ((DecimalVector) vecs.get(2)).setSafe(0, new BigDecimal("1.23"));
        ((TimeMilliVector) vecs.get(3)).setSafe(0, 45296000); // 12:34:56
        ((VarBinaryVector) vecs.get(4)).setSafe(0, new byte[]{1, 2, 3});
        for (FieldVector v : vecs) {
            v.setValueCount(1);
        }
        VectorSchemaRoot root = VectorSchemaRoot.of(vecs.toArray(new FieldVector[0]));
        root.setRowCount(1);
        return root;
    }

    @Test
    void columnTypeNameReadsFieldMetadata() throws Exception {
        try (VectorSchemaRoot root = newRoot()) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals("SMALLINT", md.getColumnTypeName(1));
            assertEquals("REAL", md.getColumnTypeName(2));
            assertEquals("DECIMAL", md.getColumnTypeName(3));
            assertEquals("TIME", md.getColumnTypeName(4));
            assertEquals("VARBINARY", md.getColumnTypeName(5));
        }
    }

    @Test
    void columnTypeMapsNewArrowTypes() throws Exception {
        try (VectorSchemaRoot root = newRoot()) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals(Types.SMALLINT, md.getColumnType(1));
            assertEquals(Types.REAL, md.getColumnType(2));
            assertEquals(Types.DECIMAL, md.getColumnType(3));
            assertEquals(Types.TIME, md.getColumnType(4));
            assertEquals(Types.VARBINARY, md.getColumnType(5));
        }
    }

    @Test
    void resultSetReadsNewArrowVectors() throws Exception {
        try (VectorSchemaRoot root = newRoot()) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            assertEquals(true, rs.next());
            assertEquals((short) 123, rs.getShort(1));
            assertEquals(1.5f, rs.getFloat(2));
            assertEquals(0, new BigDecimal("1.23").compareTo(rs.getBigDecimal(3)));
            // getTime() 回当日时刻;断言本地时区表示(=12:34:56),不依赖具体 epoch 毫秒。
            assertEquals(java.time.LocalTime.of(12, 34, 56), rs.getTime(4).toLocalTime());
            assertArrayEquals(new byte[]{1, 2, 3}, rs.getBytes(5));
        }
    }

    @Test
    void getObjectDispatchesToNewTypes() throws Exception {
        try (VectorSchemaRoot root = newRoot()) {
            MiniDbResultSet rs = new MiniDbResultSet(null, root);
            assertEquals(true, rs.next());
            assertEquals((short) 123, (short) rs.getObject(1));
            assertEquals(1.5f, (float) rs.getObject(2));
            assertEquals(0, new BigDecimal("1.23").compareTo((BigDecimal) rs.getObject(3)));
            assertEquals(java.time.LocalTime.of(12, 34, 56),
                    ((Time) rs.getObject(4)).toLocalTime());
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) rs.getObject(5));
        }
    }

    @Test
    void precisionAndScaleReportDecimalMetadata() throws Exception {
        try (VectorSchemaRoot root = newRoot()) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals(10, md.getPrecision(3)); // c_decimal DECIMAL(10,2)
            assertEquals(2, md.getScale(3));
            // SMALLINT(16-bit) 十进制精度为 5 位(-32768..32767)。
            assertEquals(5, md.getPrecision(1));
            assertEquals(0, md.getScale(1));
        }
    }

    @Test
    void columnClassNamesAreTypeSpecific() throws Exception {
        try (VectorSchemaRoot root = newRoot()) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals(Short.class.getName(), md.getColumnClassName(1));
            assertEquals(Float.class.getName(), md.getColumnClassName(2));
            assertEquals(BigDecimal.class.getName(), md.getColumnClassName(3));
            assertEquals(Time.class.getName(), md.getColumnClassName(4));
            assertEquals(byte[].class.getName(), md.getColumnClassName(5));
        }
    }

    @Test
    void caseSensitivityAndSignedAreTypeSpecific() throws Exception {
        try (VectorSchemaRoot root = newRoot()) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            // 全是数值/时间/二进制列 → 均非大小写敏感
            for (int i = 1; i <= 5; i++) {
                assertEquals(false, md.isCaseSensitive(i), "column " + i + " 应非大小写敏感");
            }
            // SMALLINT/REAL/DECIMAL 都是有符号数值
            assertEquals(true, md.isSigned(1));
            assertEquals(true, md.isSigned(2));
            assertEquals(true, md.isSigned(3));
            assertEquals(false, md.isSigned(4), "TIME 应无符号语义");
            assertEquals(false, md.isSigned(5), "VARBINARY 应无符号语义");
        }
    }

    @Test
    void isNullableReflectsFieldNullability() throws Exception {
        // 非空列:主键/非 NOT NULL 列(Arrow Field nullable=false)
        Field notNull = new Field("pk", new FieldType(false, new ArrowType.Int(32, true), null, null), List.of());
        FieldVector v = notNull.createVector(allocator);
        v.setInitialCapacity(1);
        v.allocateNew();
        v.setValueCount(1);
        try (VectorSchemaRoot root = VectorSchemaRoot.of(v)) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals(java.sql.ResultSetMetaData.columnNoNulls, md.isNullable(1), "NOT NULL 列应报 columnNoNulls");
        }
    }
}
