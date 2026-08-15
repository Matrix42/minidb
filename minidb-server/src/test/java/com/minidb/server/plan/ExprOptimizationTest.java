package com.minidb.server.plan;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExprOptimizationTest {

    private Planner planner() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR),
                new ColumnMeta("dept", ColumnType.INTEGER))));
        return new Planner(catalog);
    }

    private String plan(String sql) {
        return RelOptUtil.toString(planner().plan(sql));
    }

    @Test
    void constantFolding() {
        // 1 + 2 在计划期折叠为 3,不再出现 +(1, 2)
        String plan = plan("SELECT 1 + 2");
        assertTrue(plan.contains("3"));
        assertFalse(plan.contains("+("), plan);
    }

    @Test
    void constantFoldingInProject() {
        // 投影里的常量表达式也折叠
        String plan = plan("SELECT id, 2 * 3 AS x FROM t");
        assertTrue(plan.contains("6"));
        assertFalse(plan.contains("*("), plan);
    }

    @Test
    void conditionSimplificationTautology() {
        // id = id 简化为 IS NOT NULL(id)
        String plan = plan("SELECT id FROM t WHERE id = id");
        assertTrue(plan.contains("IS NOT NULL"));
        assertFalse(plan.contains("=($0, $0)"), plan);
    }

    @Test
    void conditionSimplificationRedundantAnd() {
        // id > 1 蕴含 id > 0,后者被吸收
        String plan = plan("SELECT id FROM t WHERE id > 1 AND id > 0");
        assertTrue(plan.contains(">($0, 1)"), plan);
        assertFalse(plan.contains(">($0, 0)"), plan);
    }
}
