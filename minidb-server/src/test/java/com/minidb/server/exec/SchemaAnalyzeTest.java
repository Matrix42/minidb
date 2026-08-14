package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaAnalyzeTest {

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
        stats = new StatsManager(storage);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void analyzeQualifiedTableThenExplainUsesStats() {
        executor.execute("CREATE SCHEMA s1");
        executor.execute("CREATE TABLE s1.t (id INTEGER)");
        executor.execute("INSERT INTO s1.t VALUES (1), (2), (3), (4), (5)");
        executor.execute("ANALYZE s1.t");

        // 非 public 表的统计应写入 s1.t 并能在 EXPLAIN 里读回(坑 18 补全)。
        assertNotNull(catalog.getStats("s1", "t"));

        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(
                "EXPLAIN SELECT id FROM s1.t WHERE id > 1")).data();
        VarCharVector op = (VarCharVector) root.getVector("operation");
        VarCharVector remarks = (VarCharVector) root.getVector("remarks");
        String filterRemarks = null;
        for (int i = 0; i < root.getRowCount(); i++) {
            if (new String(op.get(i)).contains("Filter")) {
                filterRemarks = remarks.isNull(i) ? null : new String(remarks.get(i));
            }
        }
        assertNotNull(filterRemarks);
        assertTrue(filterRemarks.contains("estimated"),
                "非 public 表的 filter 应命中统计,实际 remarks=" + filterRemarks);
        root.close();
    }

    @Test
    void analyzeBareTableResolvesAgainstCurrentSchema() {
        executor.execute("CREATE SCHEMA s1");
        executor.execute("CREATE TABLE s1.t (id INTEGER)");
        executor.execute("INSERT INTO s1.t VALUES (1), (2), (3)");
        executor.execute("ANALYZE t", "s1"); // 裸名,currentSchema=s1 解析
        assertNotNull(catalog.getStats("s1", "t"));
    }
}
