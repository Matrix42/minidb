package com.minidb.server.exec;

import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.common.TableSchema;
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
        assertEquals(3, root.getRowCount());
        VarCharVector schemaName = (VarCharVector) root.getVector("SCHEMA_NAME");
        // schema 名按字典序:information_schema < other < public
        assertEquals("information_schema", new String(schemaName.get(0)));
        assertEquals("other", new String(schemaName.get(1)));
        assertEquals("public", new String(schemaName.get(2)));
        VarCharVector catalogName = (VarCharVector) root.getVector("CATALOG_NAME");
        assertTrue(catalogName.isNull(0)); // 除 SCHEMA_NAME 外恒 null
        root.close();
    }

    @Test
    void materializeTables() {
        MiniDbCatalog catalog = catalog();
        VectorSchemaRoot root = InformationSchema.materialize(catalog, "tables", allocator);
        // information_schema 自身 3 张系统表 + other.u + public.t
        assertEquals(5, root.getRowCount());
        VarCharVector tableSchema = (VarCharVector) root.getVector("TABLE_SCHEMA");
        VarCharVector tableName = (VarCharVector) root.getVector("TABLE_NAME");
        // 前 3 行是系统表(columns/schemata/tables),后 2 行是用户表
        assertEquals("information_schema", new String(tableSchema.get(0)));
        assertEquals("columns", new String(tableName.get(0)));
        assertEquals("other", new String(tableSchema.get(3)));
        assertEquals("u", new String(tableName.get(3)));
        assertEquals("public", new String(tableSchema.get(4)));
        assertEquals("t", new String(tableName.get(4)));
        root.close();
    }

    @Test
    void materializeColumns() {
        MiniDbCatalog catalog = catalog();
        VectorSchemaRoot root = InformationSchema.materialize(catalog, "columns", allocator);
        // 系统表 7+4+8=19 列 + other.u.name + public.t.id + public.t.price = 22 列
        assertEquals(22, root.getRowCount());
        VarCharVector columnName = (VarCharVector) root.getVector("COLUMN_NAME");
        IntVector ordinal = (IntVector) root.getVector("ORDINAL_POSITION");
        IntVector precision = (IntVector) root.getVector("NUMERIC_PRECISION");
        IntVector scale = (IntVector) root.getVector("NUMERIC_SCALE");
        // public.t.price 是最后一行(public 是最后的 schema,t 的第二列)
        int priceIdx = root.getRowCount() - 1;
        assertEquals("price", new String(columnName.get(priceIdx)));
        assertEquals(2, ordinal.get(priceIdx));
        assertEquals(10, precision.get(priceIdx));
        assertEquals(2, scale.get(priceIdx));
        // 前一列 public.t.id 是非 decimal 列,precision/scale 保持 null
        assertTrue(precision.isNull(priceIdx - 1));
        assertTrue(scale.isNull(priceIdx - 1));
        root.close();
    }
}
