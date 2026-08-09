package com.minidb.jdbc;

import com.minidb.protocol.Message;
import com.minidb.protocol.MessageDecoder;
import com.minidb.protocol.MessageEncoder;
import com.minidb.protocol.Protocol;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

public class MiniDbClient implements AutoCloseable {

    public sealed interface ClientResult {
        record Rows(VectorSchemaRoot data) implements ClientResult {
        }

        record Update(long count) implements ClientResult {
        }
    }

    private static final long TIMEOUT_SECONDS = 30;

    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final BufferAllocator allocator = new RootAllocator();
    private final BlockingQueue<Object> responses = new LinkedBlockingQueue<>();
    private Channel channel;
    private long nextRequestId = 1;

    public void connect(String host, int port) throws SQLException {
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new MessageDecoder());
                            ch.pipeline().addLast(new MessageEncoder());
                            ch.pipeline().addLast(new ResponseCollector(responses));
                        }
                    });
            channel = bootstrap.connect(host, port).sync().channel();
            channel.writeAndFlush(new Message.Handshake(Protocol.VERSION)).sync();
            Object ack = poll();
            if (!(ack instanceof Message.HandshakeAck)) {
                throw new SQLException("bad handshake response: " + ack);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted during connect", e);
        }
    }

    public ClientResult execute(String sql) throws SQLException {
        long requestId = nextRequestId++;
        channel.writeAndFlush(new Message.ExecuteRequest(requestId, sql));
        try {
            while (true) {
                Object msg = poll();
                if (msg instanceof Message.ExecuteResponse r) {
                    if (!r.ok()) {
                        throw new SQLException(r.error());
                    }
                } else if (msg instanceof Message.UpdateCount u) {
                    return new ClientResult.Update(u.count());
                } else if (msg instanceof Message.ArrowBatch b) {
                    if (b.lastBatch()) {
                        return new ClientResult.Rows(readArrow(b.data()));
                    }
                } else {
                    throw new SQLException("unexpected message: " + msg);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted during execute", e);
        }
    }

    private VectorSchemaRoot readArrow(byte[] data) throws SQLException {
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(data), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot source = reader.getVectorSchemaRoot();
            VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
            org.apache.arrow.vector.ipc.message.ArrowRecordBatch recordBatch =
                    new VectorUnloader(source).getRecordBatch();
            new VectorLoader(copy).load(recordBatch);
            recordBatch.close();
            return copy;
        } catch (IOException e) {
            throw new SQLException("failed to decode arrow result", e);
        }
    }

    private Object poll() throws InterruptedException, SQLException {
        Object msg = responses.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (msg == null) {
            throw new SQLException("timeout waiting for server response");
        }
        if (msg instanceof Throwable t) {
            throw new SQLException("connection error", t);
        }
        return msg;
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.writeAndFlush(new Message.CloseRequest());
            channel.close();
        }
        group.shutdownGracefully();
        allocator.close();
    }

    private static class ResponseCollector extends SimpleChannelInboundHandler<Message> {
        private final BlockingQueue<Object> queue;

        ResponseCollector(BlockingQueue<Object> queue) {
            this.queue = queue;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            queue.offer(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            queue.offer(cause);
            ctx.close();
        }
    }
}
