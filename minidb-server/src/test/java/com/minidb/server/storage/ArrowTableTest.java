package com.minidb.server.storage;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.exec.BatchIterator;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ArrowTableTest {

    @TempDir
    Path tempDir;
    BufferAllocator allocator;
    ArrowTable table;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        table = new ArrowTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("name", ColumnType.VARCHAR))), allocator, tempDir);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private VectorSchemaRoot batch(int... ids) {
        VectorSchemaRoot root = table.newBatchRoot();
        IntVector id = (IntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        root.allocateNew();
        for (int i = 0; i < ids.length; i++) {
            id.setSafe(i, ids[i]);
            name.setSafe(i, ("n" + ids[i]).getBytes());
        }
        root.setRowCount(ids.length);
        return root;
    }

    @Test
    void writeAndScanParts() {
        VectorSchemaRoot p1 = batch(1, 2);
        table.writePart(p1);
        p1.close();
        VectorSchemaRoot p2 = batch(3);
        table.writePart(p2);
        p2.close();
        assertEquals(2, table.partCount());
        assertEquals(3, table.rowCount());
        List<Integer> ids = new ArrayList<>();
        try (BatchIterator it = table.scan()) {
            while (it.hasNext()) {
                IntVector v = (IntVector) it.next().getVector("id");
                for (int i = 0; i < v.getValueCount(); i++) {
                    ids.add(v.get(i));
                }
            }
        }
        assertEquals(List.of(1, 2, 3), ids);
    }

    @Test
    void emptyTableHasNoParts() {
        assertEquals(0, table.rowCount());
        assertEquals(0, table.partCount());
    }

    @Test
    void arrowSchemaCarriesSchemaMetadata() {
        ArrowTable t = new ArrowTable(new TableSchema("other", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))), allocator, tempDir.resolve("other"));
        java.util.Map<String, String> meta = t.arrowSchema().getCustomMetadata();
        assertNotNull(meta);
        assertEquals("other", meta.get("schema"));
    }

    @Test
    void arrowSchemaMetadataDefaultsToPublic() {
        java.util.Map<String, String> meta = table.arrowSchema().getCustomMetadata();
        assertNotNull(meta);
        assertEquals("public", meta.get("schema"));
    }
}
