package com.minidb.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.charset.StandardCharsets;

public class MessageEncoder extends MessageToByteEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        out.writeShort(Protocol.MAGIC);
        if (msg instanceof Message.Handshake h) {
            out.writeByte(MessageType.HANDSHAKE);
            out.writeInt(1);
            out.writeByte(h.version());
        } else if (msg instanceof Message.HandshakeAck a) {
            out.writeByte(MessageType.HANDSHAKE_ACK);
            out.writeInt(1);
            out.writeByte(a.version());
        } else if (msg instanceof Message.ExecuteRequest r) {
            byte[] sql = r.sql().getBytes(StandardCharsets.UTF_8);
            out.writeByte(MessageType.EXECUTE_REQUEST);
            out.writeInt(8 + 4 + sql.length);
            out.writeLong(r.requestId());
            out.writeInt(sql.length);
            out.writeBytes(sql);
        } else if (msg instanceof Message.CloseRequest) {
            out.writeByte(MessageType.CLOSE_REQUEST);
            out.writeInt(0);
        } else if (msg instanceof Message.ExecuteResponse r) {
            byte[] err = r.error() == null
                    ? new byte[0] : r.error().getBytes(StandardCharsets.UTF_8);
            out.writeByte(MessageType.EXECUTE_RESPONSE);
            out.writeInt(8 + 1 + 4 + err.length);
            out.writeLong(r.requestId());
            out.writeByte(r.ok() ? 0 : 1);
            out.writeInt(err.length);
            out.writeBytes(err);
        } else if (msg instanceof Message.ArrowBatch b) {
            out.writeByte(MessageType.ARROW_BATCH);
            out.writeInt(8 + 1 + 4 + b.data().length);
            out.writeLong(b.requestId());
            out.writeByte(b.lastBatch() ? 1 : 0);
            out.writeInt(b.data().length);
            out.writeBytes(b.data());
        } else if (msg instanceof Message.UpdateCount u) {
            out.writeByte(MessageType.UPDATE_COUNT);
            out.writeInt(16);
            out.writeLong(u.requestId());
            out.writeLong(u.count());
        } else {
            throw new IllegalArgumentException("unknown message: " + msg);
        }
    }
}
