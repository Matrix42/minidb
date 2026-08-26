package com.minidb.server.exec;

import com.minidb.parser.ddl.SqlCreateIndex;
import com.minidb.parser.ddl.SqlDropIndex;
import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.catalog.MiniDbCatalog;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexDdlParseTest {

    MiniDbCatalog catalog;
    CalciteContext calcite;

    @BeforeEach
    void setUp() {
        catalog = new MiniDbCatalog();
        calcite = new CalciteContext(catalog);
    }

    @Test
    void createIndex() {
        SqlNode node = calcite.parse("CREATE INDEX idx_a ON t (a)");
        assertTrue(node instanceof SqlCreateIndex, "expected SqlCreateIndex but got " + node.getClass().getSimpleName());
        SqlCreateIndex c = (SqlCreateIndex) node;
        assertFalse(c.unique(), "expected non-unique index");
        assertEquals("idx_a", c.indexName().getSimple());
        assertEquals("t", c.table().getSimple());
        assertEquals(1, c.columnList().size());
        assertEquals("a", c.columnList().get(0).toString());
    }

    @Test
    void createUniqueIndex() {
        SqlNode node = calcite.parse("CREATE UNIQUE INDEX idx_ab ON s.t (a, b)");
        assertTrue(node instanceof SqlCreateIndex);
        SqlCreateIndex c = (SqlCreateIndex) node;
        assertTrue(c.unique());
        assertEquals("idx_ab", c.indexName().getSimple());
        assertEquals(2, c.table().names.size());
        assertEquals("s", c.table().names.get(0));
        assertEquals("t", c.table().names.get(1));
        assertEquals(2, c.columnList().size());
    }

    @Test
    void createIndexMultipleColumns() {
        SqlNode node = calcite.parse("CREATE INDEX idx ON t (a, b, c)");
        assertTrue(node instanceof SqlCreateIndex);
        SqlCreateIndex c = (SqlCreateIndex) node;
        assertEquals(3, c.columnList().size());
    }

    @Test
    void dropIndex() {
        SqlNode node = calcite.parse("DROP INDEX idx ON t");
        assertTrue(node instanceof SqlDropIndex, "expected SqlDropIndex but got " + node.getClass().getSimpleName());
        SqlDropIndex d = (SqlDropIndex) node;
        assertFalse(d.ifExists());
        assertEquals("idx", d.indexName().getSimple());
        assertEquals("t", d.table().getSimple());
    }

    @Test
    void dropIndexIfExists() {
        SqlNode node = calcite.parse("DROP INDEX IF EXISTS idx ON s.t");
        assertTrue(node instanceof SqlDropIndex);
        SqlDropIndex d = (SqlDropIndex) node;
        assertTrue(d.ifExists());
        assertEquals("idx", d.indexName().getSimple());
        assertEquals(2, d.table().names.size());
    }

    @Test
    void dropIndexMissingOn() {
        assertThrows(Exception.class, () -> calcite.parse("DROP INDEX idx"));
    }

    @Test
    void createIndexMissingIndexKeyword() {
        assertThrows(Exception.class, () -> calcite.parse("CREATE INDEX t (a)"));
    }

    @Test
    void createIndexMissingColumnList() {
        assertThrows(Exception.class, () -> calcite.parse("CREATE INDEX idx ON t"));
    }
}