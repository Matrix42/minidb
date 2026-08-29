package com.minidb.server.netty;

import com.minidb.protocol.Message;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.MetadataExecutor;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.stats.StatsManager;
import com.minidb.server.storage.StorageManager;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.Channels;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionHandlerCursorTest {

    private static int rowCount(ByteBuf data, BufferAllocator allocator) throws Exception {
        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteBufInputStream(data), allocator)) {
            reader.loadNextBatch();
            return reader.getVectorSchemaRoot().getRowCount();
        }
    }

    /**
     * 分页续批(仅 record-batch message,无 schema/stream 头)必须用 deserializeRecordBatch 解码,不能用
     * ArrowStreamReader。
     */
    private static int recordCount(ByteBuf data, BufferAllocator allocator) throws Exception {
        try (ReadChannel channel =
                new ReadChannel(Channels.newChannel(new ByteBufInputStream(data)))) {
            try (ArrowRecordBatch batch =
                    MessageSerializer.deserializeRecordBatch(channel, allocator)) {
                return batch.getLength();
            }
        }
    }

    @Test
    void selectStreamsPagesAndClosesOnExhaustion(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        EmbeddedChannel ch =
                new EmbeddedChannel(
                        new SessionHandler(
                                executor,
                                new MetadataExecutor(catalog, allocator),
                                MoreExecutors.newDirectExecutorService()));
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
            first.data().release();

            ch.writeInbound(new Message.FetchRequest(10, 3, 2));
            // 分页续批是 ArrowContinuation(仅 record-batch message,无 schema)
            Message.ArrowContinuation second =
                    (Message.ArrowContinuation) ch.outboundMessages().poll();
            assertEquals(10, second.requestId());
            assertEquals(3, second.cursorId());
            assertTrue(second.lastBatch());
            assertEquals(1, recordCount(second.data(), allocator));
            second.data().release();

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

    @Test
    void closeCursorRemovesOpenCursor(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        EmbeddedChannel ch =
                new EmbeddedChannel(
                        new SessionHandler(
                                executor,
                                new MetadataExecutor(catalog, allocator),
                                MoreExecutors.newDirectExecutorService()));
        try {
            ch.writeInbound(new Message.ExecuteRequest(1, "CREATE TABLE t (id INTEGER)"));
            ch.outboundMessages().poll(); // UpdateCount
            ch.writeInbound(new Message.ExecuteRequest(2, "INSERT INTO t VALUES (1), (2), (3)"));
            ch.outboundMessages().poll(); // UpdateCount
            // open a cursor: fetchSize=2 over 3 rows → first page, not done
            ch.writeInbound(new Message.ExecuteRequest(3, "SELECT id FROM t ORDER BY id", 2));
            Message.ArrowBatch first = (Message.ArrowBatch) ch.outboundMessages().poll();
            assertFalse(first.lastBatch());
            first.data().release();

            // close the still-open cursor
            ch.writeInbound(new Message.CloseCursorRequest(3));
            assertNull(ch.outboundMessages().poll()); // no response

            // fetching a closed cursor reports unknown-cursor
            ch.writeInbound(new Message.FetchRequest(10, 3, 2));
            assertTrue(ch.outboundMessages().poll() instanceof Message.ExecuteResponse);
        } finally {
            storage.close();
            allocator.close();
            ch.close();
        }
    }
}
