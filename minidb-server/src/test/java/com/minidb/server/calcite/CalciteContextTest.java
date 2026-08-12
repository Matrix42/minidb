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

    @Test
    void qualifiedNameResolvesAcrossSchemas() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(new TableSchema("public", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.createTable(new TableSchema("other", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))));
        CalciteContext ctx = new CalciteContext(catalog);
        RelRoot r1 = ctx.plan("SELECT id FROM t");
        assertNotNull(r1.rel);
        RelRoot r2 = ctx.plan("SELECT id FROM other.t");
        assertNotNull(r2.rel);
        assertEquals(List.of("id"), r2.rel.getRowType().getFieldNames());
    }

    @Test
    void currentSchemaSwitchesUnqualifiedResolution() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(new TableSchema("public", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))));
        catalog.createTable(new TableSchema("other", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("x", ColumnType.VARCHAR))));
        CalciteContext ctx = new CalciteContext(catalog);
        RelRoot r = ctx.plan("SELECT x FROM t", "other");
        assertNotNull(r.rel);
        assertEquals(List.of("x"), r.rel.getRowType().getFieldNames());
    }
}
