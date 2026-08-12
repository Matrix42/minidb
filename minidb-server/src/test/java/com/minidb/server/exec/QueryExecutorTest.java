package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryExecutorTest {

    @TempDir
    Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage, allocator, dataDir);
        storage.setStatsManager(stats);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void createTableInsertSelect() {
        QueryResult ddl = executor.execute(
                "CREATE TABLE t (id INTEGER, name VARCHAR)");
        assertEquals(0L, ((QueryResult.Update) ddl).count());

        QueryResult insert = executor.execute(
                "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertEquals(3L, ((QueryResult.Update) insert).count());

        QueryResult select = executor.execute(
                "SELECT id, name FROM t WHERE id > 1 ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(2, ((IntVector) root.getVector("id")).get(0));
        assertEquals("b",
                new String(((VarCharVector) root.getVector("name")).get(0)));
        root.close();
    }

    @Test
    void limitTrimsRows() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (3), (1), (2)");
        QueryResult select = executor.execute(
                "SELECT id FROM t ORDER BY id LIMIT 2");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        assertEquals(2, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void ideStyleDoubleQuotedQualifiedSqlExecutes() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        // SQL IDEs format queries with newlines and double-quoted identifiers.
        QueryResult select = executor.execute(
                "select \"id\", \"name\"\n"
              + "from \"public\".\"t\"\n"
              + "order by \"id\"");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        assertEquals("b",
                new String(((VarCharVector) root.getVector("name")).get(1)));
        root.close();
    }

    @Test
    void insertSelectCopiesRows() {
        executor.execute("CREATE TABLE src (id INTEGER)");
        executor.execute("INSERT INTO src VALUES (1), (2)");
        executor.execute("CREATE TABLE dst (id INTEGER)");
        QueryResult r = executor.execute("INSERT INTO dst SELECT id FROM src");
        assertEquals(2L, ((QueryResult.Update) r).count());
        assertEquals(2L, storage.getTable("dst").rowCount());
    }

    @Test
    void updateModifiesRows() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        QueryResult update = executor.execute(
                "UPDATE t SET name = 'x' WHERE id = 1");
        assertEquals(1L, ((QueryResult.Update) update).count());

        QueryResult select = executor.execute(
                "SELECT id, name FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(3, root.getRowCount());
        assertEquals("x", new String(((VarCharVector) root.getVector("name")).get(0)));
        assertEquals("b", new String(((VarCharVector) root.getVector("name")).get(1)));
        assertEquals("c", new String(((VarCharVector) root.getVector("name")).get(2)));
        root.close();
    }

    @Test
    void updateIntegerColumnWithLiteral() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        QueryResult update = executor.execute("UPDATE t SET id = 10 WHERE id = 1");
        assertEquals(1L, ((QueryResult.Update) update).count());

        QueryResult select = executor.execute("SELECT id, name FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(2, ((IntVector) root.getVector("id")).get(0));
        assertEquals(10, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void updateNoMatchLeavesTableUnchanged() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        QueryResult update = executor.execute("UPDATE t SET name = 'x' WHERE id = 99");
        assertEquals(0L, ((QueryResult.Update) update).count());
        assertEquals(2L, storage.getTable("t").rowCount());
    }

    @Test
    void deleteRemovesRows() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        QueryResult delete = executor.execute("DELETE FROM t WHERE id = 2");
        assertEquals(1L, ((QueryResult.Update) delete).count());

        QueryResult select = executor.execute("SELECT id FROM t ORDER BY id");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(2, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        assertEquals(3, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void dropTableRemovesIt() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("DROP TABLE t");
        assertThrows(Exception.class,
                () -> executor.execute("SELECT * FROM t"));
    }

    @Test
    void truncateEmptiesTableButKeepsSchema() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        QueryResult truncate = executor.execute("TRUNCATE TABLE t");
        assertEquals(0L, ((QueryResult.Update) truncate).count());
        assertEquals(0L, storage.getTable("t").rowCount());
        assertTrue(catalog.hasTable("t"));

        // SELECT over the now-empty table returns a zero-row result whose
        // schema still describes both columns.
        QueryResult select = executor.execute("SELECT id, name FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(0, root.getRowCount());
        assertEquals(2, root.getFieldVectors().size());
        assertEquals("id", root.getVector("id").getName());
        assertEquals("name", root.getVector("name").getName());
        root.close();
    }

    @Test
    void selectOverEmptyTableReturnsZeroRows() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        QueryResult select = executor.execute("SELECT id, name FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(0, root.getRowCount());
        assertEquals(2, root.getFieldVectors().size());
        root.close();
    }

    @Test
    void selectOverFilterThatMatchesNothingReturnsZeroRows() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        // Filter matches no rows: the pipeline produces no batches, but the
        // result must still carry the projected schema with zero rows.
        QueryResult select = executor.execute("SELECT id FROM t WHERE id > 100");
        VectorSchemaRoot root = ((QueryResult.Rows) select).data();
        assertEquals(0, root.getRowCount());
        assertEquals(1, root.getFieldVectors().size());
        assertEquals("id", root.getVector("id").getName());
        root.close();
    }

    @Test
    void truncateAllowsReinsert() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a')");
        executor.execute("TRUNCATE TABLE t");
        QueryResult insert = executor.execute("INSERT INTO t VALUES (2, 'b')");
        assertEquals(1L, ((QueryResult.Update) insert).count());
        assertEquals(1L, storage.getTable("t").rowCount());
    }

    @Test
    void truncateMissingTableThrows() {
        assertThrows(Exception.class,
                () -> executor.execute("TRUNCATE TABLE nope"));
    }

    @Test
    void badSqlThrowsWithMessage() {
        Exception e = assertThrows(Exception.class,
                () -> executor.execute("SELEC nope"));
        assertTrue(e.getMessage() != null);
    }

    @Test
    void analyzeCommandCollectsStatsForTable() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        executor.execute("ANALYZE t");
        // plain EXPLAIN on a filter should now be "estimated", not "no stats"
        QueryResult r = executor.execute("EXPLAIN SELECT id FROM t WHERE id > 1");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        VarCharVector remarks = (VarCharVector) root.getVector("remarks");
        boolean foundEstimated = false;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Filter") && !remarks.isNull(i)
                    && new String(remarks.get(i)).contains("estimated")) {
                foundEstimated = true;
            }
        }
        assertTrue(foundEstimated);
        root.close();
    }

    @Test
    void statsGoStaleAfterInsert() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2)");
        executor.execute("ANALYZE t");
        executor.execute("INSERT INTO t VALUES (3)");
        QueryResult r = executor.execute("EXPLAIN SELECT id FROM t WHERE id > 1");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        VarCharVector remarks = (VarCharVector) root.getVector("remarks");
        boolean foundStale = false;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Filter") && !remarks.isNull(i)
                    && new String(remarks.get(i)).contains("stale")) {
                foundStale = true;
            }
        }
        assertTrue(foundStale);
        root.close();
    }

    @Test
    void analyzeAllCommandCollectsAllTables() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1)");
        executor.execute("INSERT INTO b VALUES (1)");
        executor.execute("ANALYZE");
        assertNotNull(stats.tableStats("a"));
        assertNotNull(stats.tableStats("b"));
    }

    @Test
    void explainAnalyzeEndToEndMeasuresRows() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3), (4)");
        QueryResult r = executor.execute("EXPLAIN ANALYZE SELECT id FROM t WHERE id > 2");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        BigIntVector rows = (BigIntVector) root.getVector("rows");
        long filterRows = -1;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Filter")) {
                filterRows = rows.get(i);
            }
        }
        assertEquals(2L, filterRows); // id>2 -> 3,4
        root.close();
    }

    @Test
    void explainRejectsDmlEndToEnd() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("EXPLAIN INSERT INTO t VALUES (1)"));
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("EXPLAIN ANALYZE DELETE FROM t"));
    }

    // ---- aggregate ----

    @Test
    void aggregateCountSumAvgMinMax() {
        executor.execute("CREATE TABLE s (id INTEGER)");
        executor.execute("INSERT INTO s VALUES (1), (2), (3)");
        QueryResult r = executor.execute(
                "SELECT COUNT(*) AS c, SUM(id) AS s, AVG(id) AS a, "
              + "MIN(id) AS mn, MAX(id) AS mx FROM s");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(3L, ((BigIntVector) root.getVector("c")).get(0));
        // Calcite derives SUM(INTEGER)->INTEGER and AVG(INTEGER)->INTEGER
        assertEquals(6, ((IntVector) root.getVector("s")).get(0));
        assertEquals(2, ((IntVector) root.getVector("a")).get(0));
        assertEquals(1, ((IntVector) root.getVector("mn")).get(0));
        assertEquals(3, ((IntVector) root.getVector("mx")).get(0));
        root.close();
    }

    @Test
    void aggregateDoubleSumAvg() {
        executor.execute("CREATE TABLE sd (price DOUBLE)");
        executor.execute("INSERT INTO sd VALUES (10.5)");
        executor.execute("INSERT INTO sd VALUES (20.5)");
        executor.execute("INSERT INTO sd VALUES (30.0)");
        QueryResult r = executor.execute(
                "SELECT SUM(price) AS s, AVG(price) AS a FROM sd");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(61.0, ((Float8Vector) root.getVector("s")).get(0), 1e-9);
        assertEquals(20.333333333333332,
                ((Float8Vector) root.getVector("a")).get(0), 1e-9);
        root.close();
    }

    @Test
    void countColumnSkipsNulls() {
        executor.execute("CREATE TABLE n (id INTEGER)");
        executor.execute("INSERT INTO n VALUES (1)");
        executor.execute("INSERT INTO n VALUES (3)");
        executor.execute("UPDATE n SET id = NULL WHERE id = 1");
        QueryResult r = executor.execute(
                "SELECT COUNT(id) AS c, COUNT(*) AS s FROM n");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(1L, ((BigIntVector) root.getVector("c")).get(0));
        assertEquals(2L, ((BigIntVector) root.getVector("s")).get(0));
        root.close();
    }

    @Test
    void groupBySingleColumn() {
        executor.execute("CREATE TABLE t (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3)");
        QueryResult r = executor.execute(
                "SELECT dept, COUNT(*) AS c, SUM(id) AS s FROM t "
              + "GROUP BY dept ORDER BY dept");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals("a", new String(
                ((VarCharVector) root.getVector("dept")).get(0)));
        assertEquals(2L, ((BigIntVector) root.getVector("c")).get(0));
        assertEquals(4, ((IntVector) root.getVector("s")).get(0));
        assertEquals("b", new String(
                ((VarCharVector) root.getVector("dept")).get(1)));
        assertEquals(1L, ((BigIntVector) root.getVector("c")).get(1));
        assertEquals(2, ((IntVector) root.getVector("s")).get(1));
        root.close();
    }

    @Test
    void multiColumnGroupBy() {
        executor.execute("CREATE TABLE t (a INTEGER, b INTEGER, v INTEGER)");
        executor.execute("INSERT INTO t VALUES (1, 1, 10)");
        executor.execute("INSERT INTO t VALUES (1, 2, 20)");
        executor.execute("INSERT INTO t VALUES (1, 1, 30)");
        executor.execute("INSERT INTO t VALUES (2, 1, 40)");
        QueryResult r = executor.execute(
                "SELECT a, b, COUNT(*) AS c FROM t GROUP BY a, b ORDER BY a, b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount());
        assertEquals(2L, ((BigIntVector) root.getVector("c")).get(0)); // (1,1)
        assertEquals(1L, ((BigIntVector) root.getVector("c")).get(1)); // (1,2)
        assertEquals(1L, ((BigIntVector) root.getVector("c")).get(2)); // (2,1)
        root.close();
    }

    @Test
    void groupByNullIsOwnGroup() {
        executor.execute("CREATE TABLE t (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3)");
        executor.execute("UPDATE t SET dept = NULL WHERE id = 2");
        QueryResult r = executor.execute(
                "SELECT dept, COUNT(*) AS c FROM t GROUP BY dept ORDER BY dept");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals("a", new String(
                ((VarCharVector) root.getVector("dept")).get(0)));
        assertEquals(2L, ((BigIntVector) root.getVector("c")).get(0));
        assertTrue(((VarCharVector) root.getVector("dept")).isNull(1));
        assertEquals(1L, ((BigIntVector) root.getVector("c")).get(1));
        root.close();
    }

    @Test
    void globalAggregateOverEmptyTable() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        QueryResult r = executor.execute(
                "SELECT COUNT(*) AS c, SUM(id) AS s, AVG(id) AS a FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(0L, ((BigIntVector) root.getVector("c")).get(0));
        assertTrue(root.getVector("s").isNull(0));
        assertTrue(root.getVector("a").isNull(0));
        root.close();
    }

    @Test
    void groupByOverEmptyTableReturnsZeroRows() {
        executor.execute("CREATE TABLE t (dept VARCHAR)");
        QueryResult r = executor.execute(
                "SELECT dept, COUNT(*) AS c FROM t GROUP BY dept");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(0, root.getRowCount());
        assertEquals(2, root.getFieldVectors().size());
        root.close();
    }

    @Test
    void havingFiltersAggregates() {
        executor.execute("CREATE TABLE t (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3)");
        QueryResult r = executor.execute(
                "SELECT dept, COUNT(*) AS c FROM t "
              + "GROUP BY dept HAVING COUNT(*) > 1");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals("a", new String(
                ((VarCharVector) root.getVector("dept")).get(0)));
        assertEquals(2L, ((BigIntVector) root.getVector("c")).get(0));
        root.close();
    }

    @Test
    void aggregateWithExpressionArgument() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        QueryResult r = executor.execute("SELECT SUM(id * 2) AS s FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(12, ((IntVector) root.getVector("s")).get(0));
        root.close();
    }

    @Test
    void explainShowsAggregateNode() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3)");
        QueryResult r = executor.execute("EXPLAIN SELECT COUNT(*) FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        boolean found = false;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Aggregate")) {
                found = true;
            }
        }
        assertTrue(found);
        root.close();
    }

    @Test
    void explainAnalyzeAggregateMeasuresRows() {
        executor.execute("CREATE TABLE t (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3)");
        QueryResult r = executor.execute(
                "EXPLAIN ANALYZE SELECT dept, COUNT(*) AS c FROM t GROUP BY dept");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        BigIntVector rows = (BigIntVector) root.getVector("rows");
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).equals("Aggregate")) {
                assertEquals(2L, rows.get(i));
            }
        }
        root.close();
    }

    // ---- distinct ----

    @Test
    void countDistinctDeduplicates() {
        executor.execute("CREATE TABLE t (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3), ('a', 1)");
        QueryResult r = executor.execute("SELECT COUNT(DISTINCT id) AS c FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(3L, ((BigIntVector) root.getVector("c")).get(0)); // {1,2,3}
        root.close();
    }

    @Test
    void sumAvgMinMaxDistinct() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (3), (1)");
        QueryResult r = executor.execute(
                "SELECT SUM(DISTINCT id) AS s, AVG(DISTINCT id) AS a, "
              + "MIN(DISTINCT id) AS mn, MAX(DISTINCT id) AS mx FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(6, ((IntVector) root.getVector("s")).get(0)); // 1+2+3
        assertEquals(2, ((IntVector) root.getVector("a")).get(0));
        assertEquals(1, ((IntVector) root.getVector("mn")).get(0));
        assertEquals(3, ((IntVector) root.getVector("mx")).get(0));
        root.close();
    }

    @Test
    void groupByCountDistinct() {
        executor.execute("CREATE TABLE t (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3), ('a', 1)");
        QueryResult r = executor.execute(
                "SELECT dept, COUNT(DISTINCT id) AS c FROM t GROUP BY dept ORDER BY dept");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals(2L, ((BigIntVector) root.getVector("c")).get(0)); // a: {1,3}
        assertEquals(1L, ((BigIntVector) root.getVector("c")).get(1)); // b: {2}
        root.close();
    }

    @Test
    void distinctWithExpressionArgument() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        executor.execute("INSERT INTO t VALUES (1), (2), (1)");
        QueryResult r = executor.execute("SELECT SUM(DISTINCT id * 2) AS s FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(6, ((IntVector) root.getVector("s")).get(0)); // {2,4}
        root.close();
    }

    @Test
    void distinctOverEmptyTable() {
        executor.execute("CREATE TABLE t (id INTEGER)");
        QueryResult r = executor.execute(
                "SELECT COUNT(DISTINCT id) AS c, SUM(DISTINCT id) AS s FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(0L, ((BigIntVector) root.getVector("c")).get(0));
        assertTrue(root.getVector("s").isNull(0));
        root.close();
    }

    @Test
    void selectDistinctRows() {
        executor.execute("CREATE TABLE t (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3), ('a', 1)");
        QueryResult r = executor.execute("SELECT DISTINCT dept FROM t");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals("a", new String(
                ((VarCharVector) root.getVector("dept")).get(0)));
        assertEquals("b", new String(
                ((VarCharVector) root.getVector("dept")).get(1)));
        root.close();

        QueryResult r2 = executor.execute("SELECT DISTINCT dept, id FROM t");
        VectorSchemaRoot root2 = ((QueryResult.Rows) r2).data();
        assertEquals(3, root2.getRowCount()); // (a,1) dup dropped
        root2.close();
    }

    @Test
    void selectDistinctOrdered() {
        executor.execute("CREATE TABLE t (dept VARCHAR)");
        executor.execute("INSERT INTO t VALUES ('b'), ('a'), ('b'), ('a')");
        QueryResult r = executor.execute(
                "SELECT DISTINCT dept FROM t ORDER BY dept");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals("a", new String(
                ((VarCharVector) root.getVector("dept")).get(0)));
        assertEquals("b", new String(
                ((VarCharVector) root.getVector("dept")).get(1)));
        root.close();
    }

    // ---- union ----

    private void createUnionTables() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1), (2), (3)");
        executor.execute("INSERT INTO b VALUES (2), (3), (4)");
    }

    @Test
    void unionAllKeepsDuplicates() {
        createUnionTables();
        QueryResult r = executor.execute(
                "SELECT id FROM a UNION ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(6, root.getRowCount());
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(1, id.get(0));
        assertEquals(2, id.get(1));
        assertEquals(3, id.get(2));
        assertEquals(2, id.get(3));
        assertEquals(3, id.get(4));
        assertEquals(4, id.get(5));
        root.close();
    }

    @Test
    void unionDeduplicates() {
        createUnionTables();
        QueryResult r = executor.execute(
                "SELECT id FROM a UNION SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(4, root.getRowCount());
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(1, id.get(0));
        assertEquals(2, id.get(1));
        assertEquals(3, id.get(2));
        assertEquals(4, id.get(3));
        root.close();
    }

    @Test
    void unionAllWithEmptyInput() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO b VALUES (1), (2)");
        QueryResult r = executor.execute(
                "SELECT id FROM a UNION ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        root.close();
    }

    @Test
    void unionAllOverTwoEmptyTables() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        QueryResult r = executor.execute(
                "SELECT id FROM a UNION ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(0, root.getRowCount());
        assertEquals(1, root.getFieldVectors().size());
        root.close();
    }

    @Test
    void unionOrdered() {
        createUnionTables();
        QueryResult r = executor.execute(
                "SELECT id FROM a UNION SELECT id FROM b ORDER BY id DESC");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(4, root.getRowCount());
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(4, id.get(0));
        assertEquals(3, id.get(1));
        assertEquals(2, id.get(2));
        assertEquals(1, id.get(3));
        root.close();
    }

    @Test
    void unionAllMultiColumn() {
        executor.execute("CREATE TABLE a (dept VARCHAR, id INTEGER)");
        executor.execute("CREATE TABLE b (dept VARCHAR, id INTEGER)");
        executor.execute("INSERT INTO a VALUES ('x', 1), ('y', 2)");
        executor.execute("INSERT INTO b VALUES ('z', 3)");
        QueryResult r = executor.execute(
                "SELECT dept, id FROM a UNION ALL SELECT dept, id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount());
        assertEquals("x", new String(
                ((VarCharVector) root.getVector("dept")).get(0)));
        assertEquals(3, ((IntVector) root.getVector("id")).get(2));
        root.close();
    }

    @Test
    void explainShowsUnionNode() {
        createUnionTables();
        QueryResult r = executor.execute(
                "EXPLAIN SELECT id FROM a UNION ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        boolean found = false;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Union")) {
                found = true;
            }
        }
        assertTrue(found);
        root.close();
    }

    @Test
    void explainAnalyzeUnionMeasuresRows() {
        createUnionTables();
        QueryResult r = executor.execute(
                "EXPLAIN ANALYZE SELECT id FROM a UNION ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        BigIntVector rows = (BigIntVector) root.getVector("rows");
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).equals("Union")) {
                assertEquals(6L, rows.get(i));
            }
        }
        root.close();
    }

    // ---- intersect / except ----

    private void createSetOpTables() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1), (2), (3), (3), (4)");
        executor.execute("INSERT INTO b VALUES (2), (3), (5)");
    }

    @Test
    void intersectDeduplicates() {
        createSetOpTables();
        QueryResult r = executor.execute(
                "SELECT id FROM a INTERSECT SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(2, id.get(0));
        assertEquals(3, id.get(1));
        root.close();
    }

    @Test
    void intersectAllKeepsMinCount() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1), (2), (3), (3), (4)");
        executor.execute("INSERT INTO b VALUES (2), (3), (3), (5)");
        QueryResult r = executor.execute(
                "SELECT id FROM a INTERSECT ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount()); // 2, 3, 3
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(2, id.get(0));
        assertEquals(3, id.get(1));
        assertEquals(3, id.get(2));
        root.close();
    }

    @Test
    void exceptDeduplicates() {
        createSetOpTables();
        QueryResult r = executor.execute(
                "SELECT id FROM a EXCEPT SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount()); // 1, 3, 4 (3 survives once)
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(1, id.get(0));
        assertEquals(3, id.get(1));
        assertEquals(4, id.get(2));
        root.close();
    }

    @Test
    void exceptAllSubtractsCounts() {
        createSetOpTables();
        QueryResult r = executor.execute(
                "SELECT id FROM a EXCEPT ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount()); // 1, 3 (a has 2, b has 1), 4
        IntVector id = (IntVector) root.getVector("id");
        assertEquals(1, id.get(0));
        assertEquals(3, id.get(1));
        assertEquals(4, id.get(2));
        root.close();
    }

    @Test
    void exceptAllEmptyResult() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1), (2)");
        executor.execute("INSERT INTO b VALUES (1), (2), (3)");
        QueryResult r = executor.execute(
                "SELECT id FROM a EXCEPT ALL SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(0, root.getRowCount());
        root.close();
    }

    @Test
    void intersectDisjointEmptyResult() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1)");
        executor.execute("INSERT INTO b VALUES (2)");
        QueryResult r = executor.execute(
                "SELECT id FROM a INTERSECT SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(0, root.getRowCount());
        root.close();
    }

    @Test
    void intersectWithVarchar() {
        executor.execute("CREATE TABLE a (name VARCHAR)");
        executor.execute("CREATE TABLE b (name VARCHAR)");
        executor.execute("INSERT INTO a VALUES ('x'), ('y'), ('z')");
        executor.execute("INSERT INTO b VALUES ('y'), ('z'), ('z')");
        QueryResult r = executor.execute(
                "SELECT name FROM a INTERSECT SELECT name FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals("y", new String(
                ((VarCharVector) root.getVector("name")).get(0)));
        assertEquals("z", new String(
                ((VarCharVector) root.getVector("name")).get(1)));
        root.close();
    }

    @Test
    void explainShowsSetOpNode() {
        createSetOpTables();
        QueryResult r = executor.execute(
                "EXPLAIN SELECT id FROM a INTERSECT SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        boolean found = false;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("SetOp")) {
                found = true;
            }
        }
        assertTrue(found);
        root.close();
    }

    @Test
    void explainAnalyzeSetOpMeasuresRows() {
        createSetOpTables();
        QueryResult r = executor.execute(
                "EXPLAIN ANALYZE SELECT id FROM a INTERSECT SELECT id FROM b");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        BigIntVector rows = (BigIntVector) root.getVector("rows");
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).equals("SetOp")) {
                assertEquals(2L, rows.get(i));
            }
        }
        root.close();
    }

    // ---- join ----

    private void createJoinTables() {
        executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
        executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
        executor.execute("INSERT INTO a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3')");
        executor.execute("INSERT INTO b VALUES (2, 'b2'), (3, 'b3'), (4, 'b4')");
    }

    @Test
    void innerJoinEqui() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id, b.val FROM a JOIN b ON a.id = b.id ORDER BY a.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals(2, ((IntVector) root.getVector("id")).get(0));
        assertEquals("b2", new String(
                ((VarCharVector) root.getVector("val")).get(0)));
        assertEquals(3, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void leftJoinPreservesLeftRows() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id, b.val FROM a LEFT JOIN b ON a.id = b.id "
              + "ORDER BY a.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        assertTrue(root.getVector("val").isNull(0));
        assertEquals("b2", new String(
                ((VarCharVector) root.getVector("val")).get(1)));
        assertEquals("b3", new String(
                ((VarCharVector) root.getVector("val")).get(2)));
        root.close();
    }

    @Test
    void rightJoinPreservesRightRows() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id, b.id AS bid FROM a RIGHT JOIN b ON a.id = b.id "
              + "ORDER BY bid");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(3, root.getRowCount());
        assertTrue(root.getVector("id").isNull(2)); // b.id=4 unmatched
        assertEquals(4, ((IntVector) root.getVector("bid")).get(2));
        root.close();
    }

    @Test
    void fullJoinPreservesBoth() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id, b.id AS bid FROM a FULL JOIN b ON a.id = b.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(4, root.getRowCount()); // 2 matched + 1 left + 1 right
        root.close();
    }

    @Test
    void nonEquiJoin() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id, b.id AS bid FROM a JOIN b ON a.id > b.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount()); // (3,2)
        assertEquals(3, ((IntVector) root.getVector("id")).get(0));
        assertEquals(2, ((IntVector) root.getVector("bid")).get(0));
        root.close();
    }

    @Test
    void multiConditionJoin() {
        executor.execute("CREATE TABLE a (id INTEGER, name VARCHAR)");
        executor.execute("CREATE TABLE b (id INTEGER, val VARCHAR)");
        executor.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y')");
        executor.execute("INSERT INTO b VALUES (1, 'x'), (2, 'z')");
        QueryResult r = executor.execute(
                "SELECT a.id FROM a JOIN b ON a.id = b.id AND a.name = b.val");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount()); // (1, x) only
        assertEquals(1, ((IntVector) root.getVector("id")).get(0));
        root.close();
    }

    @Test
    void joinNullKeysNeverMatch() {
        executor.execute("CREATE TABLE a (id INTEGER)");
        executor.execute("CREATE TABLE b (id INTEGER)");
        executor.execute("INSERT INTO a VALUES (1), (NULL)");
        executor.execute("INSERT INTO b VALUES (1), (NULL)");
        QueryResult r = executor.execute(
                "SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.id = b.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(1, ((IntVector) root.getVector("aid")).get(0));
        assertEquals(1, ((IntVector) root.getVector("bid")).get(0));
        root.close();
    }

    @Test
    void threeWayJoin() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id FROM a JOIN b ON a.id = b.id "
              + "JOIN a c ON b.id = c.id ORDER BY a.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        assertEquals(2, ((IntVector) root.getVector("id")).get(0));
        assertEquals(3, ((IntVector) root.getVector("id")).get(1));
        root.close();
    }

    @Test
    void joinWithWhereFilter() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id, b.val FROM a JOIN b ON a.id = b.id "
              + "WHERE a.id > 2");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(1, root.getRowCount());
        assertEquals(3, ((IntVector) root.getVector("id")).get(0));
        root.close();
    }

    @Test
    void commaJoinIsInnerJoin() {
        createJoinTables();
        QueryResult r = executor.execute(
                "SELECT a.id FROM a, b WHERE a.id = b.id ORDER BY a.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        assertEquals(2, root.getRowCount());
        root.close();
    }

    @Test
    void explainShowsJoinNode() {
        createJoinTables();
        QueryResult r = executor.execute(
                "EXPLAIN SELECT a.id FROM a JOIN b ON a.id = b.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        boolean found = false;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Join")) {
                found = true;
            }
        }
        assertTrue(found);
        root.close();
    }

    @Test
    void explainAnalyzeJoinMeasuresRows() {
        createJoinTables();
        QueryResult r = executor.execute(
                "EXPLAIN ANALYZE SELECT a.id FROM a JOIN b ON a.id = b.id");
        VectorSchemaRoot root = ((QueryResult.Rows) r).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        BigIntVector rows = (BigIntVector) root.getVector("rows");
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).equals("Join")) {
                assertEquals(2L, rows.get(i));
            }
        }
        root.close();
    }
}
