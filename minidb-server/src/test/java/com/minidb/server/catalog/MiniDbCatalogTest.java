package com.minidb.server.catalog;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.TableSchema;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MiniDbCatalogTest {

    private TableSchema table(String name) {
        return new TableSchema(name, List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR)));
    }

    @Test
    void createAndGetTable() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(table("t1"));
        assertTrue(catalog.hasTable("public", "t1"));
        assertEquals("t1", catalog.getTable("public", "t1").name());
        assertEquals(2, catalog.getTable("public", "t1").columns().size());
    }

    @Test
    void tableNamesCaseInsensitive() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(table("t1"));
        assertTrue(catalog.hasTable("public", "T1"));
        assertEquals("t1", catalog.getTable("public", "T1").name());
    }

    @Test
    void createDuplicateThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(table("t1"));
        assertThrows(IllegalArgumentException.class, () -> catalog.createTable(table("t1")));
    }

    @Test
    void dropTableRemovesIt() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(table("t1"));
        catalog.dropTable("public", "t1");
        assertFalse(catalog.hasTable("public", "t1"));
    }

    @Test
    void dropMissingTableThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class, () -> catalog.dropTable("public", "nope"));
    }

    @Test
    void getMissingTableThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class, () -> catalog.getTable("public", "nope"));
    }

    @Test
    void listenersFireOnCreateAndDrop() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        AtomicInteger calls = new AtomicInteger();
        catalog.addListener(calls::incrementAndGet);
        catalog.createTable(table("t1"));
        catalog.dropTable("public", "t1");
        assertEquals(2, calls.get());
    }

    @Test
    void columnLookupByName() {
        TableSchema schema = table("t1");
        assertEquals(ColumnType.VARCHAR, schema.column("name").type());
        assertThrows(IllegalArgumentException.class, () -> schema.column("missing"));
    }

    @Test
    void schemaNameDefaultsToPublicViaConvenienceFactory() {
        TableSchema schema = new TableSchema("t1", List.of(
                new ColumnMeta("id", ColumnType.INTEGER)));
        assertEquals("public", schema.schemaName());
        assertEquals("t1", schema.name());
    }

    @Test
    void explicitSchemaNameStored() {
        TableSchema schema = new TableSchema("other", "t1", List.of(
                new ColumnMeta("id", ColumnType.INTEGER)));
        assertEquals("other", schema.schemaName());
        assertEquals("t1", schema.name());
    }
}
