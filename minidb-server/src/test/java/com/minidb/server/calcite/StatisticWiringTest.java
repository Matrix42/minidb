package com.minidb.server.calcite;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Statistic;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StatisticWiringTest {

    @Test
    void getStatisticReturnsRowCount() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t", new TableStats(Map.of(), 42, false));

        MiniDbCalciteTable table = new MiniDbCalciteTable(
                catalog.getTable("public", "t"), catalog);
        Statistic stat = table.getStatistic();
        assertEquals(42.0, stat.getRowCount());
    }

    @Test
    void getStatisticUnknownWhenStale() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.setStats("public", "t", new TableStats(Map.of(), 42, true));

        MiniDbCalciteTable table = new MiniDbCalciteTable(
                catalog.getTable("public", "t"), catalog);
        assertNull(table.getStatistic().getRowCount());
    }
}
