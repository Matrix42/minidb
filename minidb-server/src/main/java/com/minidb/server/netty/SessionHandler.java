package com.minidb.server.netty;

import com.minidb.protocol.Message;
import com.minidb.protocol.Protocol;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHandler.class);

    private final QueryExecutor executor;

    public SessionHandler(QueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        if (msg instanceof Message.Handshake h) {
            ctx.writeAndFlush(new Message.HandshakeAck(Protocol.VERSION));
        } else if (msg instanceof Message.ExecuteRequest req) {
            handleExecute(ctx, req);
        } else if (msg instanceof Message.CloseRequest) {
            ctx.close();
        }
    }

    private void handleExecute(ChannelHandlerContext ctx, Message.ExecuteRequest req) {
        LOG.debug("executing: {}", req.sql());
        long start = System.nanoTime();
        try {
            QueryResult result = executor.execute(req.sql());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            if (result instanceof QueryResult.Update update) {
                LOG.info("query ok: {} rows affected in {} ms", update.count(), elapsedMs);
                ctx.writeAndFlush(new Message.UpdateCount(req.requestId(), update.count()));
            } else if (result instanceof QueryResult.Rows rows) {
                LOG.info("query ok: {} rows returned in {} ms", rows.data().getRowCount(), elapsedMs);
                sendRows(ctx, req.requestId(), rows.data());
                rows.data().close();
            }
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            LOG.warn("query failed in {} ms: {}", elapsedMs, req.sql(), e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(req.requestId(), message));
        }
    }

    private void sendRows(ChannelHandlerContext ctx, long requestId, VectorSchemaRoot root) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            ctx.writeAndFlush(new Message.ArrowBatch(requestId, true, out.toByteArray()));
        } catch (Exception e) {
            LOG.warn("failed to send rows for request {}", requestId, e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("channel exception, closing", cause);
        ctx.close();
    }
}
