package com.minidb.server.netty;

import com.minidb.protocol.Message;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.MetadataExecutor;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.file.Path;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionHandlerSchemaTest {

    @Test
    void useSchemaUpdatesCurrentSchemaForSubsequentQueries(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        EmbeddedChannel ch = new EmbeddedChannel(
                new SessionHandler(executor, new MetadataExecutor(catalog, allocator)));
        try {
            ch.writeInbound(new Message.ExecuteRequest(1, "CREATE SCHEMA other"));
            ch.writeInbound(new Message.ExecuteRequest(2,
                    "CREATE TABLE other.t (id INTEGER)"));
            // USE SCHEMA other → UpdateCount, switches this channel's schema
            ch.writeInbound(new Message.ExecuteRequest(3, "USE SCHEMA other"));
            Object out3 = ch.outboundMessages().poll();
            assertTrue(out3 instanceof Message.UpdateCount);
            // unqualified CREATE TABLE now goes to other
            ch.writeInbound(new Message.ExecuteRequest(4, "CREATE TABLE u (id INTEGER)"));
            assertTrue(catalog.hasTable("other", "u"));
        } finally {
            storage.close();
            allocator.close();
            ch.close();
        }
    }

    @Test
    void useSchemaOnMissingSchemaSendsError(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        EmbeddedChannel ch = new EmbeddedChannel(
                new SessionHandler(executor, new MetadataExecutor(catalog, allocator)));
        try {
            ch.writeInbound(new Message.ExecuteRequest(1, "USE SCHEMA ghost"));
            Object out = ch.outboundMessages().poll();
            assertTrue(out instanceof Message.ExecuteResponse);
            // schema unchanged: still public, so unqualified table lands in public
            ch.writeInbound(new Message.ExecuteRequest(2, "CREATE TABLE p (id INTEGER)"));
            assertTrue(catalog.hasTable("public", "p"));
        } finally {
            storage.close();
            allocator.close();
            ch.close();
        }
    }
}
