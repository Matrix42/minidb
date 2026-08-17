package com.minidb.server.exec;

import com.minidb.parser.ddl.SqlAlterTable;
import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.catalog.MiniDbCatalog;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlterTableParseTest {

    MiniDbCatalog catalog;
    CalciteContext calcite;

    @BeforeEach
    void setUp() {
        catalog = new MiniDbCatalog();
        calcite = new CalciteContext(catalog);
    }

    private SqlAlterTable alter(String sql) {
        SqlNode node = calcite.parse(sql);
        assertTrue(node instanceof SqlAlterTable,
                "expected SqlAlterTable but got " + node.getClass().getSimpleName());
        return (SqlAlterTable) node;
    }

    @Test
    void addColumn() {
        SqlAlterTable a = alter("ALTER TABLE t ADD c INTEGER");
        assertEquals(SqlAlterTable.AlterKind.ADD_COLUMN, a.kind());
        assertEquals("c", a.column().getSimple());
        assertEquals("INTEGER", a.dataType().getTypeName().getSimple());
    }

    @Test
    void addColumnNotNullDefault() {
        SqlAlterTable a = alter("ALTER TABLE t ADD COLUMN c INTEGER NOT NULL DEFAULT 42");
        assertEquals(SqlAlterTable.AlterKind.ADD_COLUMN, a.kind());
        assertFalse(a.nullable());
        assertNotNull(a.defaultExpr());
    }

    @Test
    void dropColumn() {
        SqlAlterTable a = alter("ALTER TABLE t DROP COLUMN c");
        assertEquals(SqlAlterTable.AlterKind.DROP_COLUMN, a.kind());
        assertEquals("c", a.column().getSimple());
    }

    @Test
    void renameColumn() {
        SqlAlterTable a = alter("ALTER TABLE t RENAME COLUMN c TO c2");
        assertEquals(SqlAlterTable.AlterKind.RENAME_COLUMN, a.kind());
        assertEquals("c2", a.newColumn().getSimple());
    }

    @Test
    void renameTable() {
        SqlAlterTable a = alter("ALTER TABLE t RENAME TO t2");
        assertEquals(SqlAlterTable.AlterKind.RENAME_TABLE, a.kind());
        assertEquals("t2", a.newTable().getSimple());
    }

    @Test
    void alterType() {
        SqlAlterTable a = alter("ALTER TABLE t ALTER COLUMN c SET DATA TYPE BIGINT");
        assertEquals(SqlAlterTable.AlterKind.ALTER_TYPE, a.kind());
        assertEquals("BIGINT", a.dataType().getTypeName().getSimple());
    }

    @Test
    void setNotNull() {
        SqlAlterTable a = alter("ALTER TABLE t ALTER c SET NOT NULL");
        assertEquals(SqlAlterTable.AlterKind.SET_NOT_NULL, a.kind());
        assertFalse(a.nullable());
    }

    @Test
    void dropNotNull() {
        SqlAlterTable a = alter("ALTER TABLE t ALTER c DROP NOT NULL");
        assertEquals(SqlAlterTable.AlterKind.DROP_NOT_NULL, a.kind());
        assertTrue(a.nullable());
    }

    @Test
    void addPrimaryKey() {
        SqlAlterTable a = alter("ALTER TABLE t ADD PRIMARY KEY (c)");
        assertEquals(SqlAlterTable.AlterKind.ADD_CONSTRAINT, a.kind());
        assertEquals(SqlKind.PRIMARY_KEY, a.constraintKind());
    }

    @Test
    void addUniqueConstraintNamed() {
        SqlAlterTable a = alter("ALTER TABLE t ADD CONSTRAINT uq UNIQUE (a, b)");
        assertEquals(SqlAlterTable.AlterKind.ADD_CONSTRAINT, a.kind());
        assertEquals(SqlKind.UNIQUE, a.constraintKind());
        assertEquals("uq", a.constraintName().getSimple());
        assertEquals(2, a.columns().size());
    }

    @Test
    void addForeignKey() {
        SqlAlterTable a = alter("ALTER TABLE t ADD FOREIGN KEY (c) REFERENCES r (rc)");
        assertEquals(SqlAlterTable.AlterKind.ADD_CONSTRAINT, a.kind());
        assertEquals(SqlKind.OTHER, a.constraintKind());
        assertEquals("r", a.refTable().getSimple());
    }

    @Test
    void dropConstraint() {
        SqlAlterTable a = alter("ALTER TABLE t DROP CONSTRAINT pk");
        assertEquals(SqlAlterTable.AlterKind.DROP_CONSTRAINT, a.kind());
        assertEquals("pk", a.constraintName().getSimple());
    }

    @Test
    void dropPrimaryKey() {
        SqlAlterTable a = alter("ALTER TABLE t DROP PRIMARY KEY");
        assertEquals(SqlAlterTable.AlterKind.DROP_CONSTRAINT, a.kind());
        assertEquals(SqlKind.PRIMARY_KEY, a.constraintKind());
    }
}
