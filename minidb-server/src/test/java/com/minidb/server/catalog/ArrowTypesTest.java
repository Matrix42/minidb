package com.minidb.server.catalog;
import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.ColumnMeta;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArrowTypesTest {

    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator();
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    @Test
    void sqlTypeNamesMapToColumnType() {
        assertEquals(ColumnType.INTEGER, ArrowTypes.fromSqlTypeName("INTEGER"));
        assertEquals(ColumnType.INTEGER, ArrowTypes.fromSqlTypeName("INT"));
        assertEquals(ColumnType.BIGINT, ArrowTypes.fromSqlTypeName("BIGINT"));
        assertEquals(ColumnType.DOUBLE, ArrowTypes.fromSqlTypeName("DOUBLE"));
        assertEquals(ColumnType.VARCHAR, ArrowTypes.fromSqlTypeName("VARCHAR"));
        assertEquals(ColumnType.BOOLEAN, ArrowTypes.fromSqlTypeName("BOOLEAN"));
        assertEquals(ColumnType.DATE, ArrowTypes.fromSqlTypeName("DATE"));
        assertEquals(ColumnType.TIMESTAMP, ArrowTypes.fromSqlTypeName("TIMESTAMP"));
    }

    @Test
    void unknownTypeNameThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ArrowTypes.fromSqlTypeName("BLOB"));
    }

    @Test
    void arrowTypesMatchDesign() {
        assertEquals(ArrowType.ArrowTypeID.Int,
                ArrowTypes.arrowType(ColumnType.INTEGER, allocator).getTypeID());
        assertEquals(ArrowType.ArrowTypeID.FloatingPoint,
                ArrowTypes.arrowType(ColumnType.DOUBLE, allocator).getTypeID());
        assertEquals(ArrowType.ArrowTypeID.Utf8,
                ArrowTypes.arrowType(ColumnType.VARCHAR, allocator).getTypeID());
        assertEquals(ArrowType.ArrowTypeID.Bool,
                ArrowTypes.arrowType(ColumnType.BOOLEAN, allocator).getTypeID());
        assertEquals(ArrowType.ArrowTypeID.Date,
                ArrowTypes.arrowType(ColumnType.DATE, allocator).getTypeID());
        assertEquals(ArrowType.ArrowTypeID.Timestamp,
                ArrowTypes.arrowType(ColumnType.TIMESTAMP, allocator).getTypeID());
    }

    @Test
    void fieldCarriesNameAndType() {
        Field f = ArrowTypes.field(new ColumnMeta("id", ColumnType.INTEGER));
        assertEquals("id", f.getName());
        assertEquals(ArrowType.ArrowTypeID.Int, f.getType().getTypeID());
    }

    @Test
    void sqlTypeNameRoundTrip() {
        for (ColumnType t : ColumnType.values()) {
            assertEquals(t, ArrowTypes.fromSqlTypeName(ArrowTypes.toSqlTypeName(t)));
        }
    }

    @Test
    void newSqlTypeNamesMapToColumnType() {
        assertEquals(ColumnType.SMALLINT, ArrowTypes.fromSqlTypeName("SMALLINT"));
        assertEquals(ColumnType.REAL, ArrowTypes.fromSqlTypeName("REAL"));
        assertEquals(ColumnType.FLOAT, ArrowTypes.fromSqlTypeName("FLOAT"));
        assertEquals(ColumnType.DOUBLE, ArrowTypes.fromSqlTypeName("DOUBLE PRECISION"));
        assertEquals(ColumnType.CHAR, ArrowTypes.fromSqlTypeName("CHAR"));
        assertEquals(ColumnType.NCHAR, ArrowTypes.fromSqlTypeName("NCHAR"));
        assertEquals(ColumnType.NVARCHAR, ArrowTypes.fromSqlTypeName("NVARCHAR"));
        assertEquals(ColumnType.DECIMAL, ArrowTypes.fromSqlTypeName("DECIMAL"));
        assertEquals(ColumnType.NUMERIC, ArrowTypes.fromSqlTypeName("NUMERIC"));
        assertEquals(ColumnType.TIME, ArrowTypes.fromSqlTypeName("TIME"));
        assertEquals(ColumnType.BINARY, ArrowTypes.fromSqlTypeName("BINARY"));
        assertEquals(ColumnType.VARBINARY, ArrowTypes.fromSqlTypeName("VARBINARY"));
    }

    @Test
    void decimalFieldCarriesPrecisionScaleAndTypeName() {
        Field f = ArrowTypes.field(new ColumnMeta("price", ColumnType.DECIMAL, 10, 2));
        ArrowType.Decimal d = (ArrowType.Decimal) f.getType();
        assertEquals(10, d.getPrecision());
        assertEquals(2, d.getScale());
        assertEquals("DECIMAL", f.getMetadata().get("minidb.type"));
    }
}
