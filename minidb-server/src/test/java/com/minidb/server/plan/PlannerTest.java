package com.minidb.server.plan;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.TableSchema;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PlannerTest {

    private Planner planner() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(
                new TableSchema(
                        "t",
                        List.of(
                                new ColumnMeta("id", ColumnType.INTEGER),
                                new ColumnMeta("name", ColumnType.VARCHAR))));
        return new Planner(catalog);
    }

    private String planText(String sql) {
        RelNode rel = planner().plan(sql);
        return RelOptUtil.toString(rel);
    }

    @Test
    void scanPlansToMiniDbScan() {
        assertTrue(planText("SELECT * FROM t").contains("MiniDbScan"));
    }

    @Test
    void filterProjectPlansToPhysical() {
        String plan = planText("SELECT name FROM t WHERE id > 1");
        // Filter 已下推到 Scan(谓词下推),MiniDbFilter 不再出现
        assertTrue(plan.contains("MiniDbProject"));
        assertTrue(plan.contains("MiniDbScan"));
    }

    @Test
    void orderLimitPlansToSort() {
        String plan = planText("SELECT id FROM t ORDER BY id DESC LIMIT 5");
        assertTrue(plan.contains("MiniDbSort"));
    }

    @Test
    void insertValuesPlansToModifyOverValues() {
        String plan = planText("INSERT INTO t VALUES (1, 'a')");
        assertTrue(plan.contains("MiniDbModify"));
        assertTrue(plan.contains("MiniDbValues"));
    }

    @Test
    void aggregatePlansToMiniDbAggregate() {
        assertTrue(planText("SELECT COUNT(*) FROM t").contains("MiniDbAggregate"));
    }
}
