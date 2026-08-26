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
            out.writeInt(8 + 4 + sql.length + 4);
            out.writeLong(r.requestId());
            out.writeInt(sql.length);
            out.writeBytes(sql);
            out.writeInt(r.fetchSize());
        } else if (msg instanceof Message.CloseRequest) {
            out.writeByte(MessageType.CLOSE_REQUEST);
            out.writeInt(0);
        } else if (msg instanceof Message.FetchRequest r) {
            out.writeByte(MessageType.FETCH_REQUEST);
            out.writeInt(8 + 8 + 4);
            out.writeLong(r.requestId());
            out.writeLong(r.cursorId());
            out.writeInt(r.maxRows());
        } else if (msg instanceof Message.CloseCursorRequest r) {
            out.writeByte(MessageType.CLOSE_CURSOR_REQUEST);
            out.writeInt(8);
            out.writeLong(r.cursorId());
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
            out.writeInt(8 + 1 + 4 + b.data().readableBytes());
            out.writeLong(b.requestId());
            out.writeByte(b.lastBatch() ? 1 : 0);
            out.writeInt(b.data().readableBytes());
            out.writeBytes(b.data());
            // 数据所有权转给 outbound buffer,消息的引用在此释放(Encoder 是唯一释放点)。
            b.data().release();
        } else if (msg instanceof Message.ArrowContinuation c) {
            out.writeByte(MessageType.ARROW_CONTINUATION);
            out.writeInt(8 + 8 + 1 + 4 + c.data().readableBytes());
            out.writeLong(c.requestId());
            out.writeLong(c.cursorId());
            out.writeByte(c.lastBatch() ? 1 : 0);
            out.writeInt(c.data().readableBytes());
            out.writeBytes(c.data());
            c.data().release();
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
        } else if (msg instanceof Message.BeginRequest r) {
            out.writeByte(MessageType.BEGIN_REQUEST);
            out.writeInt(8);
            out.writeLong(r.requestId());
        } else if (msg instanceof Message.CommitRequest r) {
            out.writeByte(MessageType.COMMIT_REQUEST);
            out.writeInt(8);
            out.writeLong(r.requestId());
        } else if (msg instanceof Message.RollbackRequest r) {
            out.writeByte(MessageType.ROLLBACK_REQUEST);
            out.writeInt(8);
            out.writeLong(r.requestId());
        } else if (msg instanceof Message.SetAutoCommitRequest r) {
            out.writeByte(MessageType.SET_AUTOCOMMIT);
            out.writeInt(8 + 1);
            out.writeLong(r.requestId());
            out.writeByte(r.autoCommit() ? 1 : 0);
        } else if (msg instanceof Message.CommitResponse r) {
            byte[] err = r.error() == null
                    ? new byte[0] : r.error().getBytes(StandardCharsets.UTF_8);
            out.writeByte(MessageType.COMMIT_RESPONSE);
            out.writeInt(8 + 1 + 4 + err.length);
            out.writeLong(r.requestId());
            out.writeByte(r.ok() ? 0 : 1);
            out.writeInt(err.length);
            out.writeBytes(err);
        } else {
            throw new IllegalArgumentException("unknown message: " + msg);
        }
    }

    private static byte[] bytes(String s) {
        return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
    }
}
