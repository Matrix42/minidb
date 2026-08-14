package com.minidb.server.exec;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InformationSchemaTest {

    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() { allocator = new RootAllocator(); }
    @AfterAll
    static void tearDown() { allocator.close(); }

    private static MiniDbCatalog catalog() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(new TableSchema("public", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("price", ColumnType.DECIMAL, 10, 2))));
        catalog.createTable(new TableSchema("other", "u", List.of(
                new ColumnMeta("name", ColumnType.VARCHAR))));
        return catalog;
    }

    @Test
    void materializeSchemata() {
        MiniDbCatalog catalog = catalog();
        VectorSchemaRoot root = InformationSchema.materialize(catalog, "schemata", allocator);
        assertEquals(2, root.getRowCount());
        VarCharVector schemaName = (VarCharVector) root.getVector("SCHEMA_NAME");
        // schema 名按字典序:other < public
        assertEquals("other", new String(schemaName.get(0)));
        assertEquals("public", new String(schemaName.get(1)));
        VarCharVector catalogName = (VarCharVector) root.getVector("CATALOG_NAME");
        assertTrue(catalogName.isNull(0)); // 除 SCHEMA_NAME 外恒 null
        root.close();
    }

    @Test
    void materializeTables() {
        MiniDbCatalog catalog = catalog();
        VectorSchemaRoot root = InformationSchema.materialize(catalog, "tables", allocator);
        assertEquals(2, root.getRowCount());
        VarCharVector tableSchema = (VarCharVector) root.getVector("TABLE_SCHEMA");
        VarCharVector tableName = (VarCharVector) root.getVector("TABLE_NAME");
        // schema 排序后:行 0 = other.u,行 1 = public.t
        assertEquals("other", new String(tableSchema.get(0)));
        assertEquals("u", new String(tableName.get(0)));
        assertEquals("public", new String(tableSchema.get(1)));
        assertEquals("t", new String(tableName.get(1)));
        root.close();
    }

    @Test
    void materializeColumns() {
        MiniDbCatalog catalog = catalog();
        VectorSchemaRoot root = InformationSchema.materialize(catalog, "columns", allocator);
        // other.u.name + public.t.id + public.t.price = 3 列
        assertEquals(3, root.getRowCount());
        VarCharVector columnName = (VarCharVector) root.getVector("COLUMN_NAME");
        IntVector ordinal = (IntVector) root.getVector("ORDINAL_POSITION");
        IntVector precision = (IntVector) root.getVector("NUMERIC_PRECISION");
        IntVector scale = (IntVector) root.getVector("NUMERIC_SCALE");
        // 顺序:other.u.name, public.t.id, public.t.price
        assertEquals("price", new String(columnName.get(2)));
        assertEquals(2, ordinal.get(2));
        assertEquals(10, precision.get(2));
        assertEquals(2, scale.get(2));
        assertTrue(precision.isNull(0)); // 非 decimal 列 precision 保持 null
        assertTrue(scale.isNull(0));
        root.close();
    }
}
