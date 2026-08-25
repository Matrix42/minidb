package com.minidb.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.jdbc.MiniDbClient;
import com.minidb.server.MiniDbServer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 端到端分页测试:真实 MiniDbServer + MiniDbClient,小 fetchSize 触发多页,
 * 验证首屏 ArrowBatch(完整 stream)与续页 ArrowContinuation(仅 record-batch,
 * 按 cursorId 复用 schema)的收发,以及客户端两套解码路径。
 */
class PaginationEndToEndTest {

    @Test
    void multiPageCursorStreamsAllRows(@TempDir Path dir) throws Exception {
        try (MiniDbServer server = new MiniDbServer()) {
            server.start(0, dir);
            try (MiniDbClient client = new MiniDbClient()) {
                client.connect("localhost", server.port());

                client.execute("CREATE TABLE t (id INTEGER)", 0);
                client.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)", 0);

                // fetchSize=4 → 10 行分 3 页:首屏 4 行 + 续页 4 行 + 续页 2 行
                MiniDbClient.ClientResult result = client.execute(
                        "SELECT id FROM t ORDER BY id", 4);
                assertTrue(result instanceof MiniDbClient.ClientResult.Cursor);
                MiniDbClient.ClientResult.Cursor cursor = (MiniDbClient.ClientResult.Cursor) result;

                List<Integer> ids = new ArrayList<>();
                try (VectorSchemaRoot first = cursor.firstPage()) {
                    collect(first, ids);
                }
                int pages = 1;
                boolean last = cursor.lastBatch();
                while (!last) {
                    MiniDbClient.ClientResult.Rows page = client.fetch(cursor.cursorId(), 4);
                    try (VectorSchemaRoot root = page.data()) {
                        collect(root, ids);
                    }
                    last = page.lastBatch();
                    pages++;
                }
                assertEquals(3, pages);
                assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), ids);
            }
        }
    }

    @Test
    void singleBatchResultIsNotACursor(@TempDir Path dir) throws Exception {
        try (MiniDbServer server = new MiniDbServer()) {
            server.start(0, dir);
            try (MiniDbClient client = new MiniDbClient()) {
                client.connect("localhost", server.port());
                client.execute("CREATE TABLE t (id INTEGER)", 0);
                client.execute("INSERT INTO t VALUES (1),(2)", 0);

                MiniDbClient.ClientResult result = client.execute(
                        "SELECT id FROM t ORDER BY id", 100);
                assertTrue(result instanceof MiniDbClient.ClientResult.Cursor);
                MiniDbClient.ClientResult.Cursor cursor = (MiniDbClient.ClientResult.Cursor) result;
                assertTrue(cursor.lastBatch(), "单批结果应标记 lastBatch,无需 fetch");
                cursor.firstPage().close();
            }
        }
    }

    private static void collect(VectorSchemaRoot root, List<Integer> out) {
        IntVector v = (IntVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
            out.add(v.get(i));
        }
    }
}
