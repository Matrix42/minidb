package com.minidb.storage.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexDefTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void emptyIndexesOnConstruction() {
        // 2 参构造
        TableSchema t1 = new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("a", ColumnType.INTEGER)));
        assertNotNull(t1.indexes());
        assertTrue(t1.indexes().isEmpty());

        // 3 参构造带 schema
        TableSchema t2 = new TableSchema("s", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER)));
        assertNotNull(t2.indexes());
        assertTrue(t2.indexes().isEmpty());

        // 6 参构造
        TableSchema t3 = new TableSchema("s", "t",
                List.of(new ColumnMeta("id", ColumnType.INTEGER)),
                List.of("id"), List.of(), List.of());
        assertNotNull(t3.indexes());
        assertTrue(t3.indexes().isEmpty());

        // 7 参构造
        TableSchema t4 = new TableSchema("s", "t",
                List.of(new ColumnMeta("id", ColumnType.INTEGER)),
                List.of("id"), List.of(), List.of(), StorageFormat.PARQUET);
        assertNotNull(t4.indexes());
        assertTrue(t4.indexes().isEmpty());
    }

    @Test
    void withIndexesPreservesOtherFields() {
        TableSchema original = new TableSchema("s", "t",
                List.of(new ColumnMeta("id", ColumnType.INTEGER),
                        new ColumnMeta("a", ColumnType.VARCHAR)),
                List.of("id"), List.of(), List.of(), StorageFormat.PARQUET);

        IndexDef idx = new IndexDef("idx_a", false, List.of("a"));
        TableSchema withIndex = original.withIndexes(List.of(idx));

        assertEquals("s", withIndex.schemaName());
        assertEquals("t", withIndex.name());
        assertEquals(original.columns(), withIndex.columns());
        assertEquals(original.primaryKey(), withIndex.primaryKey());
        assertEquals(original.uniqueKeys(), withIndex.uniqueKeys());
        assertEquals(original.foreignKeys(), withIndex.foreignKeys());
        assertEquals(original.storageFormat(), withIndex.storageFormat());
        assertEquals(original.tableType(), withIndex.tableType());
        assertEquals(1, withIndex.indexes().size());
        assertEquals("idx_a", withIndex.indexes().get(0).name());
        assertFalse(withIndex.indexes().get(0).unique());
        assertEquals(List.of("a"), withIndex.indexes().get(0).columns());
    }

    @Test
    void indexDefNullColumnsNormalized() {
        IndexDef idx = new IndexDef("idx", true, null);
        assertNotNull(idx.columns());
        assertTrue(idx.columns().isEmpty());
    }

    @Test
    void jacksonBackwardCompatibility() throws Exception {
        // 模拟旧版 catalog.json:TableSchema 无 indexes 字段
        String oldJson = "{"
                + "\"schemaName\":\"public\","
                + "\"name\":\"t\","
                + "\"columns\":[{\"name\":\"id\",\"type\":\"INTEGER\",\"precision\":-1,\"scale\":-1,\"nullable\":true}],"
                + "\"primaryKey\":[\"id\"],"
                + "\"uniqueKeys\":[],"
                + "\"foreignKeys\":[],"
                + "\"storageFormat\":\"PARQUET\","
                + "\"tableType\":\"LSM\""
                + "}";
        TableSchema ts = MAPPER.readValue(oldJson, TableSchema.class);
        assertNotNull(ts.indexes());
        assertTrue(ts.indexes().isEmpty());
        assertEquals("public", ts.schemaName());
        assertEquals("t", ts.name());
    }

    @Test
    void withStorageFormatPreservesIndexes() {
        IndexDef idx = new IndexDef("idx_a", false, List.of("a"));
        TableSchema original = new TableSchema("s", "t",
                List.of(new ColumnMeta("id", ColumnType.INTEGER),
                        new ColumnMeta("a", ColumnType.INTEGER)),
                List.of("id"), List.of(), List.of(), StorageFormat.PARQUET)
                .withIndexes(List.of(idx));

        TableSchema converted = original.withStorageFormat(StorageFormat.ARROW);
        assertEquals(StorageFormat.ARROW, converted.storageFormat());
        assertEquals(1, converted.indexes().size());
        assertEquals("idx_a", converted.indexes().get(0).name());
    }
}