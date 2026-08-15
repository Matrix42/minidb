package com.minidb.server.catalog;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.TableSchema;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MiniDbCatalogSchemaTest {

    private TableSchema table(String schema, String name) {
        return new TableSchema(schema, name, List.of(
                new ColumnMeta("id", ColumnType.INTEGER)));
    }

    @Test
    void publicSchemaExistsByDefault() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertTrue(catalog.schemaNames().contains("public"));
    }

    @Test
    void createSchemaAppearsInList() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createDuplicateSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        assertThrows(IllegalArgumentException.class,
                () -> catalog.createSchema("other"));
    }

    @Test
    void schemaNamesCaseInsensitive() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("Other");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createTableInNamedSchema() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(table("other", "t"));
        assertTrue(catalog.hasTable("other", "t"));
        assertEquals("other", catalog.getTable("other", "t").schemaName());
        assertEquals(List.of("t"), catalog.tableNames("other"));
    }

    @Test
    void sameTableNameInDifferentSchemasCoexist() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(table("public", "users"));
        catalog.createTable(table("other", "users"));
        assertTrue(catalog.hasTable("public", "users"));
        assertTrue(catalog.hasTable("other", "users"));
    }

    @Test
    void createTableInMissingSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class,
                () -> catalog.createTable(table("ghost", "t")));
    }

    @Test
    void dropSchemaCascadesTables() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(table("other", "t1"));
        catalog.createTable(table("other", "t2"));
        catalog.dropSchema("other");
        assertFalse(catalog.schemaNames().contains("other"));
        assertFalse(catalog.hasTable("other", "t1"));
    }

    @Test
    void dropPublicSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class, () -> catalog.dropSchema("public"));
    }

    @Test
    void dropInformationSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class, () -> catalog.dropSchema("information_schema"));
    }

    @Test
    void dropMissingSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class, () -> catalog.dropSchema("ghost"));
    }

    @Test
    void legacyPublicDelegatesStillWork() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))));
        assertTrue(catalog.hasTable("public", "t"));
        assertEquals("public", catalog.getTable("public", "t").schemaName());
        assertEquals(1, catalog.tableNames("public").size());
        catalog.dropTable("public", "t");
        assertFalse(catalog.hasTable("public", "t"));
    }
}
