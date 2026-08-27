package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataExecutorTest {

    @Test
    void schemasReturnsAllSortedWhenPatternNull() {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createSchema("beta");
            cat.createSchema("alpha");
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.schemas(null)) {
                assertEquals(4, root.getRowCount()); // alpha, beta, information_schema, public
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                assertEquals("alpha", new String(schem.get(0)));
                assertEquals("beta", new String(schem.get(1)));
                assertEquals("information_schema", new String(schem.get(2)));
                assertEquals("public", new String(schem.get(3)));
                assertTrue(root.getVector("TABLE_CATALOG").isNull(0));
            }
        }
    }

    @Test
    void schemasFilterByLikePattern() {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createSchema("prod1");
            cat.createSchema("prod2");
            cat.createSchema("test");
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.schemas("prod%")) {
                assertEquals(2, root.getRowCount());
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                assertEquals("prod1", new String(schem.get(0)));
                assertEquals("prod2", new String(schem.get(1)));
            }
        }
    }

    @Test
    void tablesReturnsAllTablesAcrossSchemas() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createTable(new com.minidb.storage.common.TableSchema("public", "users",
                    java.util.List.of(new com.minidb.storage.common.ColumnMeta("id", com.minidb.storage.common.ColumnType.INTEGER))));
            cat.createSchema("other");
            cat.createTable(new com.minidb.storage.common.TableSchema("other", "t",
                    java.util.List.of(new com.minidb.storage.common.ColumnMeta("a", com.minidb.storage.common.ColumnType.BIGINT))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.tables(null, null, null)) {
                // information_schema(3 张系统表) + other/t + public/users
                assertEquals(5, root.getRowCount());
                VarCharVector name = (VarCharVector) root.getVector("TABLE_NAME");
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                VarCharVector type = (VarCharVector) root.getVector("TABLE_TYPE");
                // 前 3 行系统表,后 2 行用户表:sorted by schema then table
                assertEquals("columns", new String(name.get(0)));
                assertEquals("information_schema", new String(schem.get(0)));
                assertEquals("SYSTEM TABLE", new String(type.get(0)));
                assertEquals("t", new String(name.get(3)));
                assertEquals("other", new String(schem.get(3)));
                assertEquals("TABLE", new String(type.get(3)));
                assertEquals("users", new String(name.get(4)));
                assertEquals("public", new String(schem.get(4)));
            }
        }
    }

    @Test
    void tablesFilterBySchemaAndType() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createTable(new com.minidb.storage.common.TableSchema("public", "u",
                    java.util.List.of(new com.minidb.storage.common.ColumnMeta("id", com.minidb.storage.common.ColumnType.INTEGER))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.tables("public", null, new String[]{"VIEW"})) {
                assertEquals(0, root.getRowCount()); // VIEW matches nothing
            }
            try (VectorSchemaRoot root = exec.tables("public", null, new String[]{"TABLE"})) {
                assertEquals(1, root.getRowCount());
            }
        }
    }

    @Test
    void columnsReturnsAllColumnsWithOrdinalAndType() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createTable(new com.minidb.storage.common.TableSchema("public", "users",
                    java.util.List.of(
                            new com.minidb.storage.common.ColumnMeta("id", com.minidb.storage.common.ColumnType.INTEGER),
                            new com.minidb.storage.common.ColumnMeta("name", com.minidb.storage.common.ColumnType.VARCHAR))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.columns("public", null, null)) {
                assertEquals(2, root.getRowCount());
                VarCharVector col = (VarCharVector) root.getVector("COLUMN_NAME");
                VarCharVector typeName = (VarCharVector) root.getVector("TYPE_NAME");
                org.apache.arrow.vector.IntVector dataType =
                        (org.apache.arrow.vector.IntVector) root.getVector("DATA_TYPE");
                org.apache.arrow.vector.IntVector ordinal =
                        (org.apache.arrow.vector.IntVector) root.getVector("ORDINAL_POSITION");
                assertEquals("id", new String(col.get(0)));
                assertEquals("INTEGER", new String(typeName.get(0)));
                assertEquals(java.sql.Types.INTEGER, dataType.get(0));
                assertEquals(1, ordinal.get(0));
                assertEquals("name", new String(col.get(1)));
                assertEquals(2, ordinal.get(1));
            }
        }
    }

    @Test
    void columnsFilterByLikeColumnName() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createTable(new com.minidb.storage.common.TableSchema("public", "users",
                    java.util.List.of(
                            new com.minidb.storage.common.ColumnMeta("id", com.minidb.storage.common.ColumnType.INTEGER),
                            new com.minidb.storage.common.ColumnMeta("username", com.minidb.storage.common.ColumnType.VARCHAR))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.columns("public", null, "%name%")) {
                assertEquals(1, root.getRowCount());
                VarCharVector col = (VarCharVector) root.getVector("COLUMN_NAME");
                assertEquals("username", new String(col.get(0)));
            }
        }
    }

    @Test
    void columnsReportsNewTypesWithDecimalPrecisionAndScale() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createTable(new com.minidb.storage.common.TableSchema("public", "t",
                    java.util.List.of(
                            new com.minidb.storage.common.ColumnMeta("s", com.minidb.storage.common.ColumnType.SMALLINT),
                            new com.minidb.storage.common.ColumnMeta("p", com.minidb.storage.common.ColumnType.DECIMAL, 10, 2),
                            new com.minidb.storage.common.ColumnMeta("t", com.minidb.storage.common.ColumnType.TIME),
                            new com.minidb.storage.common.ColumnMeta("b", com.minidb.storage.common.ColumnType.VARBINARY))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.columns("public", null, null)) {
                assertEquals(4, root.getRowCount());
                VarCharVector col = (VarCharVector) root.getVector("COLUMN_NAME");
                VarCharVector typeName = (VarCharVector) root.getVector("TYPE_NAME");
                org.apache.arrow.vector.IntVector dataType =
                        (org.apache.arrow.vector.IntVector) root.getVector("DATA_TYPE");
                org.apache.arrow.vector.IntVector colSize =
                        (org.apache.arrow.vector.IntVector) root.getVector("COLUMN_SIZE");
                org.apache.arrow.vector.IntVector decDigits =
                        (org.apache.arrow.vector.IntVector) root.getVector("DECIMAL_DIGITS");

                assertEquals("s", new String(col.get(0)));
                assertEquals("SMALLINT", new String(typeName.get(0)));
                assertEquals(java.sql.Types.SMALLINT, dataType.get(0));

                assertEquals("p", new String(col.get(1)));
                assertEquals("DECIMAL", new String(typeName.get(1)));
                assertEquals(java.sql.Types.DECIMAL, dataType.get(1));
                assertEquals(10, colSize.get(1));
                assertEquals(2, decDigits.get(1));

                assertEquals("t", new String(col.get(2)));
                assertEquals("TIME", new String(typeName.get(2)));
                assertEquals(java.sql.Types.TIME, dataType.get(2));

                assertEquals("b", new String(col.get(3)));
                assertEquals("VARBINARY", new String(typeName.get(3)));
                assertEquals(java.sql.Types.VARBINARY, dataType.get(3));
            }
        }
    }

    @Test
    void compileLikeMatchesSingleCharWildcard() {
        assertTrue(MetadataExecutor.compileLike("t_%").matcher("t1").matches(), "t1 should match t_%");
        assertTrue(MetadataExecutor.compileLike("t_%").matcher("t12").matches(), "t12 should match t_%");
        assertFalse(MetadataExecutor.compileLike("t_%").matcher("t").matches(), "t should not match t_%");
    }

    @Test
    void compileLikeHonorsEscapeCharacter() {
        // getSearchStringEscape() = "\";DataGrip 会把 _ 转义为 \_ 以匹配字面下划线
        assertTrue(MetadataExecutor.compileLike("information\\_schema")
                .matcher("information_schema").matches());
        assertFalse(MetadataExecutor.compileLike("information\\_schema")
                .matcher("informationXschema").matches());
        // \% 匹配字面 %, \\ 匹配字面 \
        assertTrue(MetadataExecutor.compileLike("a\\%b").matcher("a%b").matches());
        assertTrue(MetadataExecutor.compileLike("a\\\\b").matcher("a\\b").matches());
    }

    @Test
    void tablesMatchesEscapedUnderscoreSchema() {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.tables("information\\_schema", null, null)) {
                assertEquals(3, root.getRowCount()); // schemata/tables/columns
                VarCharVector name = (VarCharVector) root.getVector("TABLE_NAME");
                assertEquals("columns", new String(name.get(0)));
            }
        }
    }
}
