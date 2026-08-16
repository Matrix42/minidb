package com.minidb.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.protocol.Message;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.MetadataExecutor;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionHandlerCursorTest {

    private static int rowCount(byte[] data, BufferAllocator allocator) throws Exception {
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(data), allocator)) {
            reader.loadNextBatch();
            return reader.getVectorSchemaRoot().getRowCount();
        }
    }

    @Test
    void selectStreamsPagesAndClosesOnExhaustion(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        EmbeddedChannel ch = new EmbeddedChannel(
                new SessionHandler(executor, new MetadataExecutor(catalog, allocator)));
        try {
            ch.writeInbound(new Message.ExecuteRequest(1, "CREATE TABLE t (id INTEGER)"));
            ch.outboundMessages().poll(); // UpdateCount
            ch.writeInbound(new Message.ExecuteRequest(2, "INSERT INTO t VALUES (1), (2), (3)"));
            ch.outboundMessages().poll(); // UpdateCount
            ch.writeInbound(new Message.ExecuteRequest(3, "SELECT id FROM t ORDER BY id", 2));

            Message.ArrowBatch first = (Message.ArrowBatch) ch.outboundMessages().poll();
            assertEquals(3, first.requestId());
            assertFalse(first.lastBatch());
            assertEquals(2, rowCount(first.data(), allocator));

            ch.writeInbound(new Message.FetchRequest(10, 3, 2));
            Message.ArrowBatch second = (Message.ArrowBatch) ch.outboundMessages().poll();
            assertEquals(10, second.requestId());
            assertTrue(second.lastBatch());
            assertEquals(1, rowCount(second.data(), allocator));

            // fetching a cursor that is already exhausted reports an error
            ch.writeInbound(new Message.FetchRequest(11, 3, 2));
            assertTrue(ch.outboundMessages().poll() instanceof Message.ExecuteResponse);

            // closing an unknown cursor is a no-op (no response)
            ch.writeInbound(new Message.CloseCursorRequest(3));
            assertNull(ch.outboundMessages().poll());
        } finally {
            storage.close();
            allocator.close();
            ch.close();
        }
    }
}
