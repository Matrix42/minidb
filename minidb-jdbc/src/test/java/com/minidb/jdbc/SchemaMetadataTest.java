package com.minidb.jdbc;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaMetadataTest {

    private VectorSchemaRoot rootWithSchema(String schemaName) {
        Schema schema =
                new Schema(
                        List.of(
                                new Field(
                                        "id",
                                        FieldType.nullable(new ArrowType.Int(32, true)),
                                        List.of())),
                        schemaName == null ? null : Map.of("schema", schemaName));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator());
        root.allocateNew();
        ((IntVector) root.getVector(0)).setSafe(0, 1);
        root.setRowCount(1);
        return root;
    }

    @Test
    void getSchemaNameReadsArrowMetadata() {
        try (VectorSchemaRoot root = rootWithSchema("public")) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals("public", md.getSchemaName(1));
        }
    }

    @Test
    void getSchemaNameReflectsNonDefaultSchema() {
        try (VectorSchemaRoot root = rootWithSchema("other")) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals("other", md.getSchemaName(1));
        }
    }

    @Test
    void getSchemaNameEmptyWhenMetadataAbsent() {
        try (VectorSchemaRoot root = rootWithSchema(null)) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals("", md.getSchemaName(1));
        }
    }
}
