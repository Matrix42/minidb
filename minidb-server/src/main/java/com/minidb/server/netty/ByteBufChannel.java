package com.minidb.server.netty;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Arrow IPC 写通道适配:让 ArrowStreamWriter 直接编码进 Netty ByteBuf, 消除 ByteArrayOutputStream + toByteArray
 * 的中间拷贝(大结果集每批省一次全量 copy)。 ByteBuf 动态扩容(ensureWritable),channel close 不释放 buf(所有权归调用方)。
 */
final class ByteBufChannel implements WritableByteChannel {

    private final ByteBuf buf;
    private boolean open = true;

    ByteBufChannel(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public int write(ByteBuffer src) {
        int remaining = src.remaining();
        if (remaining == 0) {
            return 0;
        }
        buf.ensureWritable(remaining);
        buf.writeBytes(src);
        return remaining;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
