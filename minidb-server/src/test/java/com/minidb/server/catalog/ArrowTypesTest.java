package com.minidb.server.catalog;

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
        for (ColumnType t : new ColumnType[]{ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.DOUBLE,
                ColumnType.VARCHAR, ColumnType.BOOLEAN, ColumnType.DATE, ColumnType.TIMESTAMP}) {
            assertEquals(t, ArrowTypes.fromSqlTypeName(ArrowTypes.toSqlTypeName(t)));
        }
    }
}
