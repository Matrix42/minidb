package com.minidb.server.calcite;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.util.List;
import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CalciteContextTest {

    private MiniDbCatalog catalogWithT() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR))));
        return catalog;
    }

    @Test
    void selectPlansToTableScan() {
        CalciteContext ctx = new CalciteContext(catalogWithT());
        RelRoot root = ctx.plan("SELECT id FROM t");
        assertNotNull(root.rel);
        assertEquals(List.of("id"), root.rel.getRowType().getFieldNames());
    }

    @Test
    void filterProjectPlanContainsFilter() {
        CalciteContext ctx = new CalciteContext(catalogWithT());
        RelRoot root = ctx.plan("SELECT name FROM t WHERE id > 3");
        String plan = org.apache.calcite.plan.RelOptUtil.toString(root.rel);
        assertTrue(plan.contains("Filter"));
        assertTrue(plan.contains("Project"));
    }

    @Test
    void unknownTableFailsValidation() {
        CalciteContext ctx = new CalciteContext(catalogWithT());
        assertThrows(Exception.class, () -> ctx.plan("SELECT * FROM missing"));
    }

    @Test
    void badSyntaxThrows() {
        CalciteContext ctx = new CalciteContext(catalogWithT());
        assertThrows(Exception.class, () -> ctx.plan("SELEC id FROM t"));
    }

    @Test
    void newTableVisibleAfterCatalogChange() {
        MiniDbCatalog catalog = catalogWithT();
        CalciteContext ctx = new CalciteContext(catalog);
        ctx.plan("SELECT id FROM t");
        catalog.createTable(new TableSchema("t2", List.of(
                new ColumnMeta("x", ColumnType.BIGINT))));
        RelRoot root = ctx.plan("SELECT x FROM t2");
        assertNotNull(root.rel);
    }
}
