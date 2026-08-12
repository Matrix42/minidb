package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                assertEquals(3, root.getRowCount()); // alpha, beta, public
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                assertEquals("alpha", new String(schem.get(0)));
                assertEquals("beta", new String(schem.get(1)));
                assertEquals("public", new String(schem.get(2)));
                assertTrue(root.getVector("TABLE_CAT").isNull(0));
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
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "users",
                    java.util.List.of(new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER))));
            cat.createSchema("other");
            cat.createTable(new com.minidb.server.catalog.TableSchema("other", "t",
                    java.util.List.of(new com.minidb.server.catalog.ColumnMeta("a", com.minidb.server.catalog.ColumnType.BIGINT))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.tables(null, null, null)) {
                assertEquals(2, root.getRowCount());
                VarCharVector name = (VarCharVector) root.getVector("TABLE_NAME");
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                VarCharVector type = (VarCharVector) root.getVector("TABLE_TYPE");
                // sorted by schema then table: other/t, public/users
                assertEquals("t", new String(name.get(0)));
                assertEquals("other", new String(schem.get(0)));
                assertEquals("TABLE", new String(type.get(0)));
                assertEquals("users", new String(name.get(1)));
                assertEquals("public", new String(schem.get(1)));
            }
        }
    }

    @Test
    void tablesFilterBySchemaAndType() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            MiniDbCatalog cat = new MiniDbCatalog();
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "u",
                    java.util.List.of(new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER))));
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
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "users",
                    java.util.List.of(
                            new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER),
                            new com.minidb.server.catalog.ColumnMeta("name", com.minidb.server.catalog.ColumnType.VARCHAR))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.columns(null, null, null)) {
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
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "users",
                    java.util.List.of(
                            new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER),
                            new com.minidb.server.catalog.ColumnMeta("username", com.minidb.server.catalog.ColumnType.VARCHAR))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.columns(null, null, "%name%")) {
                assertEquals(1, root.getRowCount());
                VarCharVector col = (VarCharVector) root.getVector("COLUMN_NAME");
                assertEquals("username", new String(col.get(0)));
            }
        }
    }
}
