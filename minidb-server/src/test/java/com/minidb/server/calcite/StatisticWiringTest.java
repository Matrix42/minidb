package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.Histogram;
import com.minidb.server.stats.TableStats;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.TableSchema;

import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class StatisticWiringTest {

    @Test
    void getStatisticReturnsRowCount() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(
                new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t", new TableStats(Map.of(), 42, false));

        MiniDbCalciteTable table = new MiniDbCalciteTable(catalog.getTable("public", "t"), catalog);
        Statistic stat = table.getStatistic();
        assertEquals(42.0, stat.getRowCount());
    }

    @Test
    void getStatisticUnknownWhenStale() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(
                new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t", new TableStats(Map.of(), 42, true));

        MiniDbCalciteTable table = new MiniDbCalciteTable(catalog.getTable("public", "t"), catalog);
        assertNull(table.getStatistic().getRowCount());
    }

    @Test
    void unwrapSelectivityHandlerReturnsEstimate() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(
                new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        Histogram histogram =
                new Histogram(
                        ColumnType.INTEGER,
                        List.of(new Histogram.Bucket("1", "5", 3)),
                        List.of(),
                        5,
                        0,
                        3);
        catalog.setStats("public", "t", new TableStats(Map.of("id", histogram), 3, false));

        MiniDbCalciteTable table = new MiniDbCalciteTable(catalog.getTable("public", "t"), catalog);
        BuiltInMetadata.Selectivity.Handler handler =
                table.unwrap(BuiltInMetadata.Selectivity.Handler.class);
        assertNotNull(handler);

        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode predicate =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        rexBuilder.makeInputRef(intType, 0),
                        rexBuilder.makeExactLiteral(BigDecimal.ONE, intType));

        Double selectivity = handler.getSelectivity(null, null, predicate);
        assertNotNull(selectivity);
    }

    @Test
    void unwrapDistinctRowCountHandlerReturnsDistinctCount() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(
                new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        Histogram histogram =
                new Histogram(
                        ColumnType.INTEGER,
                        List.of(new Histogram.Bucket("1", "5", 3)),
                        List.of(),
                        5,
                        0,
                        3);
        catalog.setStats("public", "t", new TableStats(Map.of("id", histogram), 3, false));

        MiniDbCalciteTable table = new MiniDbCalciteTable(catalog.getTable("public", "t"), catalog);
        BuiltInMetadata.DistinctRowCount.Handler handler =
                table.unwrap(BuiltInMetadata.DistinctRowCount.Handler.class);
        assertNotNull(handler);

        Double distinct = handler.getDistinctRowCount(null, null, ImmutableBitSet.of(0), null);
        assertEquals(5.0, distinct);
    }
}
