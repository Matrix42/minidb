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
}
