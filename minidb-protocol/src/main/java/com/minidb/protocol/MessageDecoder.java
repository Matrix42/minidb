package com.minidb.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {

    private static final int HEADER_SIZE = 7; // magic(2) + type(1) + len(4)

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.readableBytes() >= HEADER_SIZE) {
            in.markReaderIndex();
            int magic = in.readUnsignedShort();
            if (magic != Protocol.MAGIC) {
                throw new IllegalStateException(String.format("bad magic: 0x%04X", magic));
            }
            byte type = in.readByte();
            int len = in.readInt();
            if (in.readableBytes() < len) {
                in.resetReaderIndex();
                return;
            }
            out.add(decodePayload(type, in));
        }
    }

    private Message decodePayload(byte type, ByteBuf in) {
        switch (type) {
            case MessageType.HANDSHAKE -> {
                return new Message.Handshake(in.readByte());
            }
            case MessageType.HANDSHAKE_ACK -> {
                return new Message.HandshakeAck(in.readByte());
            }
            case MessageType.EXECUTE_REQUEST -> {
                long requestId = in.readLong();
                int sqlLen = in.readInt();
                byte[] sql = new byte[sqlLen];
                in.readBytes(sql);
                int fetchSize = in.readInt();
                return new Message.ExecuteRequest(
                        requestId, new String(sql, StandardCharsets.UTF_8), fetchSize);
            }
            case MessageType.CLOSE_REQUEST -> {
                return new Message.CloseRequest();
            }
            case MessageType.FETCH_REQUEST -> {
                long requestId = in.readLong();
                long cursorId = in.readLong();
                int maxRows = in.readInt();
                return new Message.FetchRequest(requestId, cursorId, maxRows);
            }
            case MessageType.CLOSE_CURSOR_REQUEST -> {
                long cursorId = in.readLong();
                return new Message.CloseCursorRequest(cursorId);
            }
            case MessageType.EXECUTE_RESPONSE -> {
                long requestId = in.readLong();
                boolean ok = in.readByte() == 0;
                int msgLen = in.readInt();
                byte[] msg = new byte[msgLen];
                in.readBytes(msg);
                return new Message.ExecuteResponse(
                        requestId, ok, new String(msg, StandardCharsets.UTF_8));
            }
            case MessageType.ARROW_BATCH -> {
                long requestId = in.readLong();
                boolean lastBatch = in.readByte() != 0;
                int dataLen = in.readInt();
                // readBytes 返回独立引用计数的新 ByteBuf,由消息消费方(客户端解码)负责 release。
                ByteBuf data = in.readBytes(dataLen);
                return new Message.ArrowBatch(requestId, lastBatch, data);
            }
            case MessageType.ARROW_CONTINUATION -> {
                long requestId = in.readLong();
                long cursorId = in.readLong();
                boolean lastBatch = in.readByte() != 0;
                int dataLen = in.readInt();
                ByteBuf data = in.readBytes(dataLen);
                return new Message.ArrowContinuation(requestId, cursorId, lastBatch, data);
            }
            case MessageType.UPDATE_COUNT -> {
                long requestId = in.readLong();
                long count = in.readLong();
                return new Message.UpdateCount(requestId, count);
            }
            case MessageType.SCHEMAS_REQUEST -> {
                long requestId = in.readLong();
                int pLen = in.readInt();
                String pattern = readNullableString(in, pLen);
                return new Message.SchemasRequest(requestId, pattern);
            }
            case MessageType.TABLES_REQUEST -> {
                long requestId = in.readLong();
                int spLen = in.readInt();
                String schemaPattern = readNullableString(in, spLen);
                int tpLen = in.readInt();
                String tablePattern = readNullableString(in, tpLen);
                int typesLen = in.readInt();
                String[] types;
                if (typesLen < 0) {
                    types = null;
                } else {
                    types = new String[typesLen];
                    for (int i = 0; i < typesLen; i++) {
                        int tLen = in.readInt();
                        types[i] = readNullableString(in, tLen);
                    }
                }
                return new Message.TablesRequest(requestId, schemaPattern, tablePattern, types);
            }
            case MessageType.COLUMNS_REQUEST -> {
                long requestId = in.readLong();
                int spLen = in.readInt();
                String schemaPattern = readNullableString(in, spLen);
                int tpLen = in.readInt();
                String tablePattern = readNullableString(in, tpLen);
                int cpLen = in.readInt();
                String columnPattern = readNullableString(in, cpLen);
                return new Message.ColumnsRequest(
                        requestId, schemaPattern, tablePattern, columnPattern);
            }
            case MessageType.BEGIN_REQUEST -> {
                long requestId = in.readLong();
                return new Message.BeginRequest(requestId);
            }
            case MessageType.COMMIT_REQUEST -> {
                long requestId = in.readLong();
                return new Message.CommitRequest(requestId);
            }
            case MessageType.ROLLBACK_REQUEST -> {
                long requestId = in.readLong();
                return new Message.RollbackRequest(requestId);
            }
            case MessageType.SET_AUTOCOMMIT -> {
                long requestId = in.readLong();
                boolean autoCommit = in.readByte() != 0;
                return new Message.SetAutoCommitRequest(requestId, autoCommit);
            }
            case MessageType.COMMIT_RESPONSE -> {
                long requestId = in.readLong();
                boolean ok = in.readByte() == 0;
                int msgLen = in.readInt();
                byte[] msg = new byte[msgLen];
                in.readBytes(msg);
                return new Message.CommitResponse(
                        requestId, ok, new String(msg, StandardCharsets.UTF_8));
            }
            default ->
                    throw new IllegalStateException(
                            String.format("unknown message type: 0x%02X", type));
        }
    }

    private static String readNullableString(ByteBuf in, int len) {
        if (len < 0) return null;
        if (len == 0) return "";
        byte[] b = new byte[len];
        in.readBytes(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
