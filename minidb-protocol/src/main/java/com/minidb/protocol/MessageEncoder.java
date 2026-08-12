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
        } else if (msg instanceof Message.SchemasRequest r) {
            out.writeByte(MessageType.SCHEMAS_REQUEST);
            byte[] p = bytes(r.schemaPattern());
            out.writeInt(8 + 4 + p.length);
            out.writeLong(r.requestId());
            out.writeInt(r.schemaPattern() == null ? -1 : p.length);
            if (p.length > 0) out.writeBytes(p);
        } else if (msg instanceof Message.TablesRequest r) {
            byte[] sp = bytes(r.schemaPattern());
            byte[] tp = bytes(r.tableNamePattern());
            int typesLen = r.types() == null ? -1 : r.types().length;
            int body = 8 + 4 + sp.length + 4 + tp.length + 4;
            byte[][] typeBytes = new byte[typesLen < 0 ? 0 : typesLen][];
            for (int i = 0; i < (typesLen < 0 ? 0 : typesLen); i++) {
                typeBytes[i] = bytes(r.types()[i]);
                body += 4 + typeBytes[i].length;
            }
            out.writeByte(MessageType.TABLES_REQUEST);
            out.writeInt(body);
            out.writeLong(r.requestId());
            out.writeInt(r.schemaPattern() == null ? -1 : sp.length);
            if (sp.length > 0) out.writeBytes(sp);
            out.writeInt(r.tableNamePattern() == null ? -1 : tp.length);
            if (tp.length > 0) out.writeBytes(tp);
            out.writeInt(typesLen);
            for (int i = 0; i < (typesLen < 0 ? 0 : typesLen); i++) {
                out.writeInt(typeBytes[i].length);
                if (typeBytes[i].length > 0) out.writeBytes(typeBytes[i]);
            }
        } else if (msg instanceof Message.ColumnsRequest r) {
            byte[] sp = bytes(r.schemaPattern());
            byte[] tp = bytes(r.tableNamePattern());
            byte[] cp = bytes(r.columnNamePattern());
            out.writeByte(MessageType.COLUMNS_REQUEST);
            out.writeInt(8 + 4 + sp.length + 4 + tp.length + 4 + cp.length);
            out.writeLong(r.requestId());
            out.writeInt(r.schemaPattern() == null ? -1 : sp.length);
            if (sp.length > 0) out.writeBytes(sp);
            out.writeInt(r.tableNamePattern() == null ? -1 : tp.length);
            if (tp.length > 0) out.writeBytes(tp);
            out.writeInt(r.columnNamePattern() == null ? -1 : cp.length);
            if (cp.length > 0) out.writeBytes(cp);
        } else {
            throw new IllegalArgumentException("unknown message: " + msg);
        }
    }

    private static byte[] bytes(String s) {
        return s == null ? new byte[0] : s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
