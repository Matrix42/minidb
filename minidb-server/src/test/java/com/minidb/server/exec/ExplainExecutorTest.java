package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.Planner;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExplainExecutorTest {

    @TempDir
    Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    Planner planner;
    ExplainExecutor explain;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage);
        planner = new Planner(catalog);
        explain = new ExplainExecutor(planner, stats, storage, allocator);

        storage.createTable(new com.minidb.server.catalog.TableSchema("t",
                java.util.List.of(
                        new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER),
                        new com.minidb.server.catalog.ColumnMeta("name", com.minidb.server.catalog.ColumnType.VARCHAR))));
        insertRows("t", new int[]{1, 2, 3}, new String[]{"a", "b", "c"});
    }

    private void insertRows(String table, int[] ids, String[] names) {
        var arrowTable = storage.getTable("public", table);
        var root = arrowTable.newBatchRoot();
        root.allocateNew();
        var idv = (IntVector) root.getVector(0);
        var nv = (VarCharVector) root.getVector(1);
        for (int i = 0; i < ids.length; i++) {
            idv.setSafe(i, ids[i]);
            nv.setSafe(i, names[i].getBytes());
        }
        idv.setValueCount(ids.length);
        nv.setValueCount(ids.length);
        root.setRowCount(ids.length);
        arrowTable.appendBatch(root);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void explainSelectPlanTreeStructure() {
        QueryResult r = explain.explain("SELECT id, name FROM t WHERE id > 1 ORDER BY id");
        QueryResult.Rows rows = (QueryResult.Rows) r;
        VectorSchemaRoot root = rows.data();
        // Sort, Filter, Scan = 3 operators
        assertEquals(3, root.getRowCount());
        assertEquals(7, root.getFieldVectors().size());

        // operation column contains Sort/Filter/Scan
        VarCharVector op = (VarCharVector) root.getVector("operation");
        java.util.Set<String> ops = new java.util.HashSet<>();
        for (int i = 0; i < root.getRowCount(); i++) {
            ops.add(new String(op.get(i)));
        }
        assertTrue(ops.stream().anyMatch(s -> s.contains("Sort")));
        assertTrue(ops.stream().anyMatch(s -> s.contains("Filter")));
        assertTrue(ops.stream().anyMatch(s -> s.contains("Scan")));

        // id starts at 1, root (Sort) has parent_id NULL
        IntVector id = (IntVector) root.getVector("id");
        IntVector parent = (IntVector) root.getVector("parent_id");
        assertEquals(1, id.get(0));
        assertTrue(parent.isNull(0)); // root parent is NULL

        // Scan rows = 3 (free, table.rowCount)
        BigIntVector rowVec = (BigIntVector) root.getVector("rows");
        int scanIdx = -1;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Scan")) {
                scanIdx = i;
            }
        }
        assertTrue(scanIdx >= 0);
        assertEquals(3L, rowVec.get(scanIdx));
        root.close();
    }

    @Test
    void explainFilterRowsEstimatedWhenStatsPresent() {
        stats.analyze("t");
        QueryResult r = explain.explain("SELECT id FROM t WHERE id > 1");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        BigIntVector rowVec = (BigIntVector) root.getVector("rows");
        VarCharVector remarks = (VarCharVector) root.getVector("remarks");
        long scanRows = -1, filterRows = -1;
        String filterRemarks = null;
        for (int i = 0; i < root.getRowCount(); i++) {
            String s = new String(op.get(i));
            if (s.contains("Scan")) {
                scanRows = rowVec.get(i);
            }
            if (s.contains("Filter")) {
                filterRows = rowVec.get(i);
                filterRemarks = remarks.isNull(i) ? null : new String(remarks.get(i));
            }
        }
        assertTrue(filterRows > 0);
        assertTrue(filterRows < scanRows);
        assertTrue(filterRemarks != null && filterRemarks.contains("estimated"));
        root.close();
    }

    @Test
    void explainFilterDefaultSelectivityWhenNoStats() {
        QueryResult r = explain.explain("SELECT id FROM t WHERE id > 1");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        VarCharVector remarks = (VarCharVector) root.getVector("remarks");
        String filterRemarks = null;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Filter")) {
                filterRemarks = remarks.isNull(i) ? null : new String(remarks.get(i));
            }
        }
        assertTrue(filterRemarks != null);
        assertTrue(filterRemarks.contains("default") || filterRemarks.contains("stale") || filterRemarks.contains("no stats"));
        root.close();
    }

    @Test
    void explainRejectsDml() {
        assertThrows(IllegalArgumentException.class,
                () -> explain.explain("INSERT INTO t VALUES (1, 'a')"));
    }

    @Test
    void explainAnalyzeRunsAndMeasuresAllOperators() {
        QueryResult r = explain.analyze("SELECT id FROM t WHERE id > 1 ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount()); // Sort, Filter, Scan
        VarCharVector op = (VarCharVector) root.getVector("operation");
        BigIntVector rowVec = (BigIntVector) root.getVector("rows");
        IntVector batches = (IntVector) root.getVector("batches");
        Float8Vector elapsed = (Float8Vector) root.getVector("elapsed_ms");
        for (int i = 0; i < root.getRowCount(); i++) {
            assertFalse(rowVec.isNull(i), "rows null at " + i);
            assertFalse(batches.isNull(i), "batches null at " + i);
            assertFalse(elapsed.isNull(i), "elapsed_ms null at " + i);
            assertTrue(elapsed.get(i) >= 0.0);
        }
        // Filter should report the real matched row count (id>1 -> rows 2,3 = 2)
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Filter")) {
                assertEquals(2L, rowVec.get(i));
            }
        }
        root.close();
    }

    @Test
    void explainAnalyzeRejectsDml() {
        assertThrows(IllegalArgumentException.class,
                () -> explain.analyze("DELETE FROM t WHERE id = 1"));
    }

    @Test
    void explainCompoundCrossColumnFilterDoesNotThrow() {
        stats.analyze("t"); // stats on both id (INTEGER) and name (VARCHAR)
        // id > 1 (numeric range) AND name < 'm' (varchar range): picking the id
        // histogram and evaluating name<'m' against it must NOT throw ClassCastException.
        QueryResult r = explain.explain("SELECT id FROM t WHERE id > 1 AND name < 'm'");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        // Should return a valid plan with a Filter row, no exception.
        VarCharVector op = (VarCharVector) root.getVector("operation");
        boolean foundFilter = false;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Filter")) {
                foundFilter = true;
            }
        }
        assertTrue(foundFilter);
        root.close();
    }
}
