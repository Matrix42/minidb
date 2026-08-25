package com.minidb.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CodecTest {

    private Message roundTrip(Message in) {
        EmbeddedChannel ch = new EmbeddedChannel(new MessageEncoder(), new MessageDecoder());
        assertTrue(ch.writeOutbound(in));
        Object encoded = ch.readOutbound();
        assertTrue(ch.writeInbound(encoded));
        Message out = ch.readInbound();
        ch.finishAndReleaseAll();
        return out;
    }

    @Test
    void handshakeRoundTrip() {
        Message.Handshake out = (Message.Handshake) roundTrip(new Message.Handshake((byte) 1));
        assertEquals(1, out.version());
    }

    @Test
    void executeRequestRoundTrip() {
        String sql = "SELECT * FROM t WHERE name = 'abc'";
        Message.ExecuteRequest out =
                (Message.ExecuteRequest) roundTrip(new Message.ExecuteRequest(42L, sql, 128));
        assertEquals(42L, out.requestId());
        assertEquals(sql, out.sql());
        assertEquals(128, out.fetchSize());
    }

    @Test
    void executeRequestTwoArgDefaultsFetchSizeZero() {
        Message.ExecuteRequest out =
                (Message.ExecuteRequest) roundTrip(new Message.ExecuteRequest(5L, "SELECT 1"));
        assertEquals(5L, out.requestId());
        assertEquals("SELECT 1", out.sql());
        assertEquals(0, out.fetchSize());
    }

    @Test
    void fetchRequestRoundTrip() {
        Message.FetchRequest out =
                (Message.FetchRequest) roundTrip(new Message.FetchRequest(9L, 42L, 100));
        assertEquals(9L, out.requestId());
        assertEquals(42L, out.cursorId());
        assertEquals(100, out.maxRows());
    }

    @Test
    void closeCursorRequestRoundTrip() {
        Message.CloseCursorRequest out =
                (Message.CloseCursorRequest) roundTrip(new Message.CloseCursorRequest(42L));
        assertEquals(42L, out.cursorId());
    }

    @Test
    void executeResponseErrorRoundTrip() {
        Message.ExecuteResponse out = (Message.ExecuteResponse)
                roundTrip(new Message.ExecuteResponse(7L, false, "table not found"));
        assertEquals(7L, out.requestId());
        assertEquals(false, out.ok());
        assertEquals("table not found", out.error());
    }

    @Test
    void arrowBatchRoundTrip() {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4, 5});
        Message.ArrowBatch out =
                (Message.ArrowBatch) roundTrip(new Message.ArrowBatch(9L, true, payload));
        assertEquals(9L, out.requestId());
        assertTrue(out.lastBatch());
        assertEquals(5, out.data().readableBytes());
        assertEquals(5, out.data().getByte(4));
        out.data().release();
    }

    @Test
    void arrowContinuationRoundTrip() {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {9, 8, 7});
        Message.ArrowContinuation out = (Message.ArrowContinuation) roundTrip(
                new Message.ArrowContinuation(5L, 9L, false, payload));
        assertEquals(5L, out.requestId());
        assertEquals(9L, out.cursorId());
        assertEquals(3, out.data().readableBytes());
        assertEquals(9, out.data().getByte(0));
        out.data().release();
    }

    @Test
    void updateCountRoundTrip() {
        Message.UpdateCount out =
                (Message.UpdateCount) roundTrip(new Message.UpdateCount(3L, 123L));
        assertEquals(3L, out.requestId());
        assertEquals(123L, out.count());
    }

    @Test
    void fragmentedFramesReassemble() {
        EmbeddedChannel enc = new EmbeddedChannel(new MessageEncoder());
        enc.writeOutbound(new Message.ExecuteRequest(1L, "SELECT 1"));
        io.netty.buffer.ByteBuf full = enc.readOutbound();
        byte[] bytes = new byte[full.readableBytes()];
        full.readBytes(bytes);
        full.release();
        enc.finishAndReleaseAll();

        EmbeddedChannel dec = new EmbeddedChannel(new MessageDecoder());
        dec.writeInbound(io.netty.buffer.Unpooled.wrappedBuffer(bytes, 0, 5));
        assertNull(dec.readInbound());
        dec.writeInbound(io.netty.buffer.Unpooled.wrappedBuffer(bytes, 5, bytes.length - 5));
        Message.ExecuteRequest out = dec.readInbound();
        assertEquals("SELECT 1", out.sql());
        dec.finishAndReleaseAll();
    }
}
