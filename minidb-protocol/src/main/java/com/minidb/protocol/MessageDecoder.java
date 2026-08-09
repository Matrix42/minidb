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
                throw new IllegalStateException(
                        String.format("bad magic: 0x%04X", magic));
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
                return new Message.ExecuteRequest(requestId,
                        new String(sql, StandardCharsets.UTF_8));
            }
            case MessageType.CLOSE_REQUEST -> {
                return new Message.CloseRequest();
            }
            case MessageType.EXECUTE_RESPONSE -> {
                long requestId = in.readLong();
                boolean ok = in.readByte() == 0;
                int msgLen = in.readInt();
                byte[] msg = new byte[msgLen];
                in.readBytes(msg);
                return new Message.ExecuteResponse(requestId, ok,
                        new String(msg, StandardCharsets.UTF_8));
            }
            case MessageType.ARROW_BATCH -> {
                long requestId = in.readLong();
                boolean lastBatch = in.readByte() != 0;
                int dataLen = in.readInt();
                byte[] data = new byte[dataLen];
                in.readBytes(data);
                return new Message.ArrowBatch(requestId, lastBatch, data);
            }
            case MessageType.UPDATE_COUNT -> {
                long requestId = in.readLong();
                long count = in.readLong();
                return new Message.UpdateCount(requestId, count);
            }
            default -> throw new IllegalStateException(
                    String.format("unknown message type: 0x%02X", type));
        }
    }
}
