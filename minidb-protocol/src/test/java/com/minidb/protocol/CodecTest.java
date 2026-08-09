package com.minidb.protocol;

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
                (Message.ExecuteRequest) roundTrip(new Message.ExecuteRequest(42L, sql));
        assertEquals(42L, out.requestId());
        assertEquals(sql, out.sql());
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
        byte[] payload = new byte[] {1, 2, 3, 4, 5};
        Message.ArrowBatch out =
                (Message.ArrowBatch) roundTrip(new Message.ArrowBatch(9L, true, payload));
        assertEquals(9L, out.requestId());
        assertTrue(out.lastBatch());
        assertEquals(5, out.data().length);
        assertEquals(5, out.data()[4]);
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
