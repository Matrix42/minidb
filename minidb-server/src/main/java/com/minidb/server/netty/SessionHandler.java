package com.minidb.server.netty;

import com.minidb.protocol.Message;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

public class SessionHandler extends SimpleChannelInboundHandler<Message> {

    private final QueryExecutor executor;

    public SessionHandler(QueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        if (msg instanceof Message.Handshake h) {
            ctx.writeAndFlush(new Message.HandshakeAck(h.version()));
        } else if (msg instanceof Message.ExecuteRequest req) {
            handleExecute(ctx, req);
        } else if (msg instanceof Message.CloseRequest) {
            ctx.close();
        }
    }

    private void handleExecute(ChannelHandlerContext ctx, Message.ExecuteRequest req) {
        try {
            QueryResult result = executor.execute(req.sql());
            if (result instanceof QueryResult.Update update) {
                ctx.writeAndFlush(new Message.UpdateCount(req.requestId(), update.count()));
            } else if (result instanceof QueryResult.Rows rows) {
                sendRows(ctx, req.requestId(), rows.data());
                rows.data().close();
            }
        } catch (Exception e) {
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
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
