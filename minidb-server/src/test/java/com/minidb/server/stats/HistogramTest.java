package com.minidb.server.stats;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistogramTest {

    private final RexBuilder rex = new RexBuilder(
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

    // Column "x" type INTEGER, index 0. 100 rows, distinct=4 (values 1,2,3,4
    // each 25x). Build by hand to keep the test independent of HistogramBuilder.
    private Histogram hist() {
        List<Histogram.Bucket> buckets = List.of(
                new Histogram.Bucket(1, 1, 25),
                new Histogram.Bucket(2, 2, 25),
                new Histogram.Bucket(3, 3, 25),
                new Histogram.Bucket(4, 4, 25));
        List<Histogram.McValue> mcv = List.of(
                new Histogram.McValue(1, 25), new Histogram.McValue(2, 25),
                new Histogram.McValue(3, 25), new Histogram.McValue(4, 25));
        return new Histogram(buckets, mcv, 4, 0, 100);
    }

    private RexNode eq(int colIndex, int literal) {
        return rex.makeCall(SqlStdOperatorTable.EQUALS,
                rex.makeInputRef(rex.getTypeFactory().createSqlType(SqlTypeName.INTEGER), colIndex),
                rex.makeLiteral(literal, rex.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true));
    }

    private RexNode lt(int colIndex, int literal) {
        return rex.makeCall(SqlStdOperatorTable.LESS_THAN,
                rex.makeInputRef(rex.getTypeFactory().createSqlType(SqlTypeName.INTEGER), colIndex),
                rex.makeLiteral(literal, rex.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true));
    }

    @Test
    void equalityHitMcvUsesMcvFrequency() {
        Histogram h = hist();
        // col = 1 -> mcv freq 25 / 100 = 0.25
        assertEquals(0.25, h.selectivity(eq(0, 1), 100), 1e-9);
    }

    @Test
    void equalityMissFallsBackToOneOverDistinct() {
        Histogram h = hist();
        // col = 99 -> not in mcv -> 1/distinct = 0.25
        assertEquals(0.25, h.selectivity(eq(0, 99), 100), 1e-9);
    }

    @Test
    void rangeLessThanInterpolatesBuckets() {
        Histogram h = hist();
        // col < 3 -> buckets with upper < 3 fully (1,2) = 50 rows -> 0.5
        assertEquals(0.5, h.selectivity(lt(0, 3), 100), 1e-9);
    }

    @Test
    void andCombinesIndependently() {
        Histogram h = hist();
        RexNode and = rex.makeCall(SqlStdOperatorTable.AND, eq(0, 1), eq(0, 2));
        // 0.25 * 0.25 = 0.0625
        assertEquals(0.0625, h.selectivity(and, 100), 1e-9);
    }

    @Test
    void orCombinesInclusionExclusion() {
        Histogram h = hist();
        RexNode or = rex.makeCall(SqlStdOperatorTable.OR, eq(0, 1), eq(0, 2));
        // 0.25 + 0.25 - 0.25*0.25 = 0.4375
        assertEquals(0.4375, h.selectivity(or, 100), 1e-9);
    }

    @Test
    void notComplements() {
        Histogram h = hist();
        RexNode not = rex.makeCall(SqlStdOperatorTable.NOT, eq(0, 1));
        // 1 - 0.25 = 0.75
        assertEquals(0.75, h.selectivity(not, 100), 1e-9);
    }

    @Test
    void unsupportedRexFallsBackToDefault() {
        Histogram h = hist();
        // a RexNode kind we don't model (e.g. a function call) -> default 0.33
        RexNode other = rex.makeCall(SqlStdOperatorTable.PLUS,
                rex.makeInputRef(rex.getTypeFactory().createSqlType(SqlTypeName.INTEGER), 0),
                rex.makeLiteral(1, rex.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true));
        assertEquals(Histogram.DEFAULT_SELECTIVITY, h.selectivity(other, 100), 1e-9);
    }

    @Test
    void emptyHistogramReturnsDefaultForEquality() {
        Histogram empty = Histogram.empty();
        // no stats -> equality falls to default
        assertEquals(Histogram.DEFAULT_SELECTIVITY, empty.selectivity(eq(0, 1), 100), 1e-9);
        assertTrue(empty.totalRows() == 0);
    }

    @Test
    void rangeInterpolationAcrossMultiUnitBucket() {
        // Single multi-unit bucket [0,10] holding 10 rows (one each of 0..9),
        // totalRows=10, distinctCount=10, no MCV. col < 5 should interpolate
        // to half the bucket -> 5 rows -> 0.5. This exercises the boundary
        // interpolation branch (lower < literal < upper), not the "whole bucket
        // below" path, catching the compareTo-vs-numericDelta regression.
        List<Histogram.Bucket> buckets = List.of(
                new Histogram.Bucket(0, 10, 10));
        Histogram h = new Histogram(buckets, List.of(), 10, 0, 10);
        // frac = literal - lower = 5 - 0 = 5; span = 10 - 0 = 10; 5/10 * 10 rows = 5 rows -> 0.5
        assertEquals(0.5, h.selectivity(lt(0, 5), 10), 1e-9);
    }
}
