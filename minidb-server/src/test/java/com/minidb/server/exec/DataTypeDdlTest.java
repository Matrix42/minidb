package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataTypeDdlTest {

    @TempDir Path dataDir;
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
    void createTableParsesDecimalPrecisionScale() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2), qty NUMERIC(8), s SMALLINT)");
        List<ColumnMeta> cols = catalog.getTable("public", "t").columns();
        assertEquals(ColumnType.DECIMAL, cols.get(0).type());
        assertEquals(10, cols.get(0).precision());
        assertEquals(2, cols.get(0).scale());
        // Calcite 把 NUMERIC 折叠为 DECIMAL(SqlTypeName 枚举无 NUMERIC 值),
        // 故 NUMERIC(8) 记为 DECIMAL,precision=8;scale 未指定为 -1(SCALE_UNSET)。
        assertEquals(ColumnType.DECIMAL, cols.get(1).type());
        assertEquals(8, cols.get(1).precision());
        assertEquals(ColumnMeta.SCALE_UNSET, cols.get(1).scale());
        assertEquals(ColumnType.SMALLINT, cols.get(2).type());
    }
}
