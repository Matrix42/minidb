package com.minidb.jdbc;

import com.minidb.protocol.Message;
import com.minidb.protocol.MessageDecoder;
import com.minidb.protocol.MessageEncoder;
import com.minidb.protocol.Protocol;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class MiniDbClient implements AutoCloseable {

    public sealed interface ClientResult {
        record Cursor(long cursorId, int fetchSize, VectorSchemaRoot firstPage, boolean lastBatch)
                implements ClientResult {}

        record Rows(VectorSchemaRoot data, boolean lastBatch) implements ClientResult {}

        record Update(long count) implements ClientResult {}
    }

    private static final long DEFAULT_TIMEOUT_SECONDS = 30;
    private static final long HANDSHAKE_TIMEOUT_SECONDS = 5;

    private final long timeoutSeconds;
    private final boolean noTimeout;
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final BufferAllocator allocator = new RootAllocator();
    private final Map<Long, CompletableFuture<ClientResult>> pending = new ConcurrentHashMap<>();
    private final AtomicLong nextRequestId = new AtomicLong(1);
    private volatile boolean connected = false;
    // 是否已通过 close()/finalize() 释放资源,防止重复释放。
    private volatile boolean released = false;

    private Channel channel;

    public MiniDbClient() {
        this(DEFAULT_TIMEOUT_SECONDS);
    }

    /**
     * 兜底:用户忘记 close() 而让 client 被 GC 时,释放 EventLoopGroup 与 RootAllocator, 避免堆外内存与 Netty 线程永久泄漏。转发
     * close()(其已做幂等)。
     */
    @Override
    protected void finalize() {
        close();
    }

    /**
     * @param timeoutSeconds 0 = 永不超时,大于 0 = 等待上限秒数
     */
    public MiniDbClient(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        this.noTimeout = timeoutSeconds <= 0;
    }

    /**
     * Reports whether the underlying channel is still connected. Goes false when the channel goes
     * inactive (server closed, network dropped, etc.) — see {@link
     * ResponseCollector#channelInactive}. MiniDbConnection delegates isClosed()/isValid() here so a
     * pool learns the connection is dead without having to try a query.
     */
    public boolean isConnected() {
        return connected;
    }

    /** Shared allocator for client-built result sets (e.g. getTableTypes). */
    BufferAllocator allocator() {
        return allocator;
    }

    public void connect(String host, int port) throws SQLException {
        CompletableFuture<Void> handshake = new CompletableFuture<>();
        try {
            Bootstrap bootstrap =
                    new Bootstrap()
                            .group(group)
                            .channel(NioSocketChannel.class)
                            .handler(
                                    new ChannelInitializer<SocketChannel>() {
                                        @Override
                                        protected void initChannel(SocketChannel ch) {
                                            ch.pipeline().addLast(new MessageDecoder());
                                            ch.pipeline().addLast(new MessageEncoder());
                                            ch.pipeline()
                                                    .addLast(
                                                            new ResponseCollector(
                                                                    handshake,
                                                                    pending,
                                                                    MiniDbClient.this
                                                                            ::markDisconnected,
                                                                    new ResponseCollector
                                                                            .ArrowDecoder() {
                                                                        @Override
                                                                        public VectorSchemaRoot
                                                                                decodeFull(
                                                                                        ByteBuf
                                                                                                data)
                                                                                        throws
                                                                                                SQLException {
                                                                            return readArrow(data);
                                                                        }

                                                                        @Override
                                                                        public VectorSchemaRoot
                                                                                decodeContinuation(
                                                                                        Schema
                                                                                                schema,
                                                                                        ByteBuf
                                                                                                data)
                                                                                        throws
                                                                                                SQLException {
                                                                            return readContinuation(
                                                                                    schema, data);
                                                                        }
                                                                    }));
                                        }
                                    });
            channel = bootstrap.connect(host, port).sync().channel();
            channel.writeAndFlush(new Message.Handshake(Protocol.VERSION)).sync();
            handshake.get(HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            connected = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted during connect", e);
        } catch (TimeoutException e) {
            channel.close();
            throw new SQLException("handshake timeout", e);
        } catch (ExecutionException e) {
            channel.close();
            throw new SQLException("handshake failed: " + e.getCause().getMessage(), e.getCause());
        }
    }

    public ClientResult execute(String sql, int fetchSize) throws SQLException {
        if (!connected) {
            throw new SQLException("connection is closed");
        }
        long id = nextRequestId.getAndIncrement();
        CompletableFuture<ClientResult> fut = new CompletableFuture<>();
        pending.put(id, fut);
        // Race guard: the channel may have died between the connected check
        // and the put. If so, channelInactive already cleared pending and
        // missed our entry — fail it ourselves.
        if (!connected) {
            pending.remove(id, fut);
            throw new SQLException("connection is closed");
        }
        try {
            channel.writeAndFlush(new Message.ExecuteRequest(id, sql, fetchSize)).sync();
        } catch (Exception e) {
            pending.remove(id, fut);
            throw new SQLException("failed to send request", e);
        }
        try {
            ClientResult result = await(fut);
            if (result instanceof ClientResult.Rows rows) {
                return new ClientResult.Cursor(id, fetchSize, rows.data(), rows.lastBatch());
            }
            return result;
        } finally {
            pending.remove(id, fut);
        }
    }

    public ClientResult.Rows fetch(long cursorId, int maxRows) throws SQLException {
        if (!connected) {
            throw new SQLException("connection is closed");
        }
        long id = nextRequestId.getAndIncrement();
        CompletableFuture<ClientResult> fut = new CompletableFuture<>();
        pending.put(id, fut);
        // Race guard: the channel may have died between the connected check and the put.
        if (!connected) {
            pending.remove(id, fut);
            throw new SQLException("connection is closed");
        }
        try {
            channel.writeAndFlush(new Message.FetchRequest(id, cursorId, maxRows)).sync();
        } catch (Exception e) {
            pending.remove(id, fut);
            throw new SQLException("failed to send fetch", e);
        }
        try {
            ClientResult result = await(fut);
            if (result instanceof ClientResult.Rows rows) {
                return rows;
            }
            throw new SQLException("unexpected response to fetch");
        } finally {
            pending.remove(id, fut);
        }
    }

    public void closeCursor(long cursorId) {
        if (connected && channel != null) {
            channel.writeAndFlush(new Message.CloseCursorRequest(cursorId));
        }
    }

    /** 分配一个新的请求 ID，供事务控制消息使用。 */
    public long nextRequestId() {
        return nextRequestId.getAndIncrement();
    }

    /**
     * 发送事务控制消息（Begin/Commit/Rollback/SetAutoCommit）并同步等待响应。
     *
     * @param requestId 消息的请求 ID，需与 msg 内嵌的 requestId 一致
     * @param msg 事务控制消息
     */
    public void sendAndWait(long requestId, Message msg) throws SQLException {
        if (!connected) {
            throw new SQLException("connection is closed");
        }
        CompletableFuture<ClientResult> fut = new CompletableFuture<>();
        pending.put(requestId, fut);
        if (!connected) {
            pending.remove(requestId, fut);
            throw new SQLException("connection is closed");
        }
        try {
            channel.writeAndFlush(msg).sync();
        } catch (Exception e) {
            pending.remove(requestId, fut);
            throw new SQLException("failed to send request", e);
        }
        try {
            await(fut);
        } finally {
            pending.remove(requestId, fut);
        }
    }

    public VectorSchemaRoot schemas(String schemaPattern) throws SQLException {
        return sendMetadata(new Message.SchemasRequest(allocateRequestId(), schemaPattern));
    }

    public VectorSchemaRoot tables(String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        return sendMetadata(
                new Message.TablesRequest(
                        allocateRequestId(), schemaPattern, tableNamePattern, types));
    }

    public VectorSchemaRoot columns(
            String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        return sendMetadata(
                new Message.ColumnsRequest(
                        allocateRequestId(), schemaPattern, tableNamePattern, columnNamePattern));
    }

    private long allocateRequestId() throws SQLException {
        if (!connected) {
            throw new SQLException("connection is closed");
        }
        return nextRequestId.getAndIncrement();
    }

    private VectorSchemaRoot sendMetadata(Message req) throws SQLException {
        long id =
                req instanceof Message.SchemasRequest r
                        ? r.requestId()
                        : req instanceof Message.TablesRequest t
                                ? t.requestId()
                                : ((Message.ColumnsRequest) req).requestId();
        CompletableFuture<ClientResult> fut = new CompletableFuture<>();
        pending.put(id, fut);
        if (!connected) {
            pending.remove(id, fut);
            throw new SQLException("connection is closed");
        }
        try {
            channel.writeAndFlush(req).sync();
        } catch (Exception e) {
            pending.remove(id, fut);
            throw new SQLException("failed to send request", e);
        }
        try {
            ClientResult result = await(fut);
            if (result instanceof ClientResult.Rows rows) {
                return rows.data();
            }
            throw new SQLException("unexpected result type for metadata request");
        } finally {
            pending.remove(id, fut);
        }
    }

    private void markDisconnected() {
        connected = false;
    }

    @Override
    public void close() {
        if (released) {
            return;
        }
        released = true;
        connected = false;
        if (channel != null) {
            channel.writeAndFlush(new Message.CloseRequest());
            channel.close();
        }
        group.shutdownGracefully();
        allocator.close();
        failAllPending("connection is closed");
    }

    private void failAllPending(String reason) {
        for (CompletableFuture<ClientResult> f : new ArrayList<>(pending.values())) {
            f.completeExceptionally(new SQLException(reason));
        }
        pending.clear();
    }

    private <T> T await(CompletableFuture<T> fut) throws SQLException {
        try {
            return noTimeout ? fut.get() : fut.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted", e);
        } catch (TimeoutException e) {
            throw new SQLException("timeout waiting for server response");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException sqle) {
                throw sqle;
            }
            throw new SQLException(
                    cause != null ? cause.getMessage() : "query failed", cause != null ? cause : e);
        }
    }

    private VectorSchemaRoot readArrow(ByteBuf data) throws SQLException {
        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteBufInputStream(data), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot source = reader.getVectorSchemaRoot();
            VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
            ArrowRecordBatch recordBatch = new VectorUnloader(source).getRecordBatch();
            new VectorLoader(copy).load(recordBatch);
            recordBatch.close();
            return copy;
        } catch (IOException e) {
            throw new SQLException("failed to decode arrow result", e);
        }
    }

    /**
     * 解码分页续批(仅 record-batch message,无 schema):按首批发来的 schema 建 root, 直接把消息 body load 进 root —— 省掉
     * ArrowStreamReader 路径必须的整页拷贝。 返回的 root 归调用方(close 时释放 body buffers)。
     */
    private VectorSchemaRoot readContinuation(Schema schema, ByteBuf data) throws SQLException {
        try {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            try (ReadChannel channel =
                    new ReadChannel(Channels.newChannel(new ByteBufInputStream(data)))) {
                try (ArrowRecordBatch recordBatch =
                        MessageSerializer.deserializeRecordBatch(channel, allocator)) {
                    new VectorLoader(root).load(recordBatch);
                }
            }
            return root;
        } catch (IOException e) {
            throw new SQLException("failed to decode arrow continuation", e);
        }
    }

    /**
     * Routes inbound messages to the per-request future, and fans connection loss out to all
     * pending futures so execute() fails fast instead of blocking until the timeout.
     */
    private static class ResponseCollector extends SimpleChannelInboundHandler<Message> {
        interface ArrowDecoder {
            /** 完整 stream(含 schema):结果集首/单批、元数据。 */
            VectorSchemaRoot decodeFull(ByteBuf data) throws SQLException;

            /** 分页续批(仅 record-batch):按首个批次缓存的 schema 解码,零拷贝 load。 */
            VectorSchemaRoot decodeContinuation(Schema schema, ByteBuf data) throws SQLException;
        }

        private final CompletableFuture<Void> handshake;
        private final Map<Long, CompletableFuture<ClientResult>> pending;
        private final Runnable onDisconnect;
        private final ArrowDecoder arrowDecoder;
        // cursor 分页的 schema 缓存:key = execute 的 requestId(即 cursorId),首批发来后存、
        // 最后一页发来后移除、连接断开清空。Schema 是纯元数据,残留条目无内存风险。
        private final Map<Long, Schema> cursorSchemas = new ConcurrentHashMap<>();

        ResponseCollector(
                CompletableFuture<Void> handshake,
                Map<Long, CompletableFuture<ClientResult>> pending,
                Runnable onDisconnect,
                ArrowDecoder arrowDecoder) {
            this.handshake = handshake;
            this.pending = pending;
            this.onDisconnect = onDisconnect;
            this.arrowDecoder = arrowDecoder;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            if (msg instanceof Message.HandshakeAck) {
                handshake.complete(null);
                return;
            }
            if (msg instanceof Message.CommitResponse r) {
                CompletableFuture<ClientResult> f = pending.remove(r.requestId());
                if (f == null) {
                    return;
                }
                if (r.ok()) {
                    f.complete(new ClientResult.Update(0));
                } else {
                    f.completeExceptionally(new SQLException(r.error()));
                }
                return;
            }
            if (msg instanceof Message.ExecuteResponse r) {
                CompletableFuture<ClientResult> f = pending.remove(r.requestId());
                if (f == null) {
                    return; // late/orphan response, drop it
                }
                if (r.ok()) {
                    f.complete(new ClientResult.Update(0));
                } else {
                    f.completeExceptionally(new SQLException(r.error()));
                }
                return;
            }
            if (msg instanceof Message.UpdateCount u) {
                CompletableFuture<ClientResult> f = pending.remove(u.requestId());
                if (f != null) {
                    f.complete(new ClientResult.Update(u.count()));
                }
                return;
            }
            if (msg instanceof Message.ArrowBatch b) {
                CompletableFuture<ClientResult> f = pending.remove(b.requestId());
                if (f == null) {
                    b.data().release();
                    return;
                }
                try {
                    VectorSchemaRoot root = arrowDecoder.decodeFull(b.data());
                    if (!b.lastBatch()) {
                        // 分页结果集的首批:cursorId = 本条消息的 requestId,缓存 schema 供续批解码。
                        cursorSchemas.put(b.requestId(), root.getSchema());
                    }
                    f.complete(new ClientResult.Rows(root, b.lastBatch()));
                } catch (SQLException e) {
                    f.completeExceptionally(e);
                } finally {
                    b.data().release();
                }
                return;
            }
            if (msg instanceof Message.ArrowContinuation c) {
                CompletableFuture<ClientResult> f = pending.remove(c.requestId());
                if (f == null) {
                    c.data().release();
                    return;
                }
                try {
                    Schema schema = cursorSchemas.get(c.cursorId());
                    if (schema == null) {
                        throw new SQLException(
                                "continuation without schema: cursor " + c.cursorId());
                    }
                    VectorSchemaRoot root = arrowDecoder.decodeContinuation(schema, c.data());
                    if (c.lastBatch()) {
                        cursorSchemas.remove(c.cursorId());
                    }
                    f.complete(new ClientResult.Rows(root, c.lastBatch()));
                } catch (SQLException e) {
                    f.completeExceptionally(e);
                } finally {
                    c.data().release();
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            cursorSchemas.clear();
            onDisconnect.run();
            for (CompletableFuture<ClientResult> f : new ArrayList<>(pending.values())) {
                f.completeExceptionally(new SQLException("connection closed"));
            }
            pending.clear();
            if (!handshake.isDone()) {
                handshake.completeExceptionally(new SQLException("connection closed"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cursorSchemas.clear();
            onDisconnect.run();
            SQLException sqle = new SQLException("connection error", cause);
            for (CompletableFuture<ClientResult> f : new ArrayList<>(pending.values())) {
                f.completeExceptionally(sqle);
            }
            pending.clear();
            if (!handshake.isDone()) {
                handshake.completeExceptionally(sqle);
            }
            ctx.close();
        }
    }
}
