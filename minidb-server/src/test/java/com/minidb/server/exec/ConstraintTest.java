package com.minidb.server.exec;

import com.minidb.server.calcite.MiniDbCalciteTable;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConstraintTest {

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

    private int[] intCol(String sql, String col) {
        VectorSchemaRoot root = ((QueryResult.Rows) executor.execute(sql)).data();
        IntVector v = (IntVector) root.getVector(col);
        int[] out = new int[v.getValueCount()];
        for (int i = 0; i < out.length; i++) {
            out[i] = v.get(i);
        }
        root.close();
        return out;
    }

    @Test
    void primaryKeyColumnLevel() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO t VALUES (1, 'c')"));
        int[] ids = intCol("SELECT id FROM t ORDER BY id", "id");
        assertEquals(2, ids.length);
        assertEquals(1, ids[0]);
        assertEquals(2, ids[1]);
    }

    @Test
    void primaryKeyTableLevel() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR, PRIMARY KEY (id))");
        executor.execute("INSERT INTO t VALUES (1, 'a')");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO t VALUES (1, 'b')"));
    }

    @Test
    void compositePrimaryKey() {
        executor.execute("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
        executor.execute("INSERT INTO t VALUES (1, 2)");
        executor.execute("INSERT INTO t VALUES (1, 3)");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO t VALUES (1, 2)"));
    }

    @Test
    void uniqueColumnLevel() {
        executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR UNIQUE)");
        executor.execute("INSERT INTO t VALUES (1, 'a')");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO t VALUES (2, 'a')"));
        // 唯一约束允许多个 NULL
        executor.execute("INSERT INTO t VALUES (3, NULL)");
        executor.execute("INSERT INTO t VALUES (4, NULL)");
    }

    @Test
    void notNullViolation() {
        executor.execute("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR)");
        assertThrows(Exception.class, () -> executor.execute("INSERT INTO t VALUES (NULL, 'a')"));
        // name 可空,插 NULL 不报错
        executor.execute("INSERT INTO t VALUES (1, NULL)");
    }

    @Test
    void getStatisticExposesKeys() {
        executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE, age INTEGER)");
        TableSchema schema = catalog.getTable("public", "t");
        Statistic statistic = new MiniDbCalciteTable(schema, catalog).getStatistic();
        List<ImmutableBitSet> keys = statistic.getKeys();
        assertEquals(2, keys.size());
        assertEquals(ImmutableBitSet.of(0), keys.get(0)); // 主键 id
        assertEquals(ImmutableBitSet.of(1), keys.get(1)); // 唯一 name
        assertTrue(statistic.isKey(ImmutableBitSet.of(0, 2))); // 含主键的列集也是唯一键
    }
}
