package com.minidb.server.storage;

import com.minidb.server.catalog.CatalogSnapshot;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.TableSchema;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonCatalogStoreTest {

    @Test
    void roundTripsSnapshot(@TempDir Path dir) throws Exception {
        JsonCatalogStore store = new JsonCatalogStore(dir.resolve("catalog.json"));
        CatalogSnapshot original = new CatalogSnapshot(
                List.of("public", "other"),
                List.of(
                        new TableSchema("public", "t", List.of(
                                new ColumnMeta("id", ColumnType.INTEGER),
                                new ColumnMeta("price", ColumnType.DECIMAL, 10, 2))),
                        new TableSchema("other", "u", List.of(
                                new ColumnMeta("name", ColumnType.VARCHAR)))));
        store.save(original);
        CatalogSnapshot loaded = store.load();
        assertEquals(original, loaded);
    }

    @Test
    void loadReturnsEmptyWhenAbsent(@TempDir Path dir) throws Exception {
        JsonCatalogStore store = new JsonCatalogStore(dir.resolve("catalog.json"));
        CatalogSnapshot loaded = store.load();
        assertEquals(List.of(), loaded.schemas());
        assertEquals(List.of(), loaded.tables());
    }
}
