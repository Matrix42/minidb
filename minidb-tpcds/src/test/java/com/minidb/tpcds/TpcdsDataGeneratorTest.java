package com.minidb.tpcds;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.TableHandle;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TpcdsDataGeneratorTest {

    @Test
    void generatesAllTables(@TempDir Path dataDir) throws Exception {
        new TpcdsDataGenerator().generate(0.01, dataDir);

        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            storage.loadAll();
            assertTrue(catalog.hasTable("public", "store_sales"));
            assertTrue(catalog.hasTable("public", "customer"));
            assertTrue(catalog.hasTable("public", "date_dim"));
            TableHandle storeSales = storage.getTable("public", "store_sales");
            assertTrue(storeSales.rowCount() > 0, "store_sales 应有数据");
            assertTrue(
                    Files.list(dataDir.resolve("public").resolve("store_sales"))
                            .anyMatch(p -> p.getFileName().toString().endsWith(".parquet")));
            storage.close();
        }
    }
}
