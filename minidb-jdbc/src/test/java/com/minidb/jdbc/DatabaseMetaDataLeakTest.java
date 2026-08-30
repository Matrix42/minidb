package com.minidb.jdbc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DatabaseMetaData 返回的 ResultSet 关闭时应连带关闭其内部创建的 Statement(#19), 否则每次
 * getTables/getColumns/getSchemas/getTableTypes 泄漏一个 Statement。
 */
class DatabaseMetaDataLeakTest {

    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator();
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    private static VectorSchemaRoot singleIntRoot() {
        IntVector v = new IntVector("TABLE_CAT", allocator);
        v.setInitialCapacity(1);
        v.allocateNew();
        v.setSafe(0, 1);
        v.setValueCount(1);
        return VectorSchemaRoot.of(v);
    }

    @Test
    void closingMetaDataResultSetClosesItsStatement() throws Exception {
        // 无需真实网络:MiniDbClient 构造不 connect,仅分配本地资源。
        MiniDbClient client = new MiniDbClient();
        MiniDbConnection conn = new MiniDbConnection(client, "jdbc:minidb://localhost:1");
        MiniDbStatement stmt = new MiniDbStatement(conn, client);

        MiniDbResultSet rs = new MiniDbResultSet(stmt, singleIntRoot(), true);
        assertTrue(!stmt.isClosed(), "关闭前 statement 应打开");
        rs.close();
        assertTrue(stmt.isClosed(), "关闭元数据 ResultSet 应连带关闭其内部 statement");

        conn.close();
    }
}
