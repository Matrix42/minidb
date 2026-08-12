package com.minidb.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MetadataMessageCodecTest {

    private Message roundTrip(Message msg) {
        ByteBuf buf = Unpooled.buffer();
        new MessageEncoder().encode(null, msg, buf);
        java.util.List<Object> out = new java.util.ArrayList<>();
        new MessageDecoder().decode(null, buf, out);
        return (Message) out.get(0);
    }

    @Test
    void schemasRequestRoundTrip() {
        Message out = roundTrip(new Message.SchemasRequest(7L, "pub%"));
        Message.SchemasRequest s = (Message.SchemasRequest) out;
        assertEquals(7L, s.requestId());
        assertEquals("pub%", s.schemaPattern());
    }

    @Test
    void schemasRequestNullPatternRoundTrip() {
        Message out = roundTrip(new Message.SchemasRequest(1L, null));
        assertNull(((Message.SchemasRequest) out).schemaPattern());
    }

    @Test
    void schemasRequestEmptyPatternRoundTrip() {
        Message out = roundTrip(new Message.SchemasRequest(9L, ""));
        Message.SchemasRequest s = (Message.SchemasRequest) out;
        assertEquals(9L, s.requestId());
        assertEquals("", s.schemaPattern());
    }

    @Test
    void tablesRequestRoundTripWithNulls() {
        Message out = roundTrip(new Message.TablesRequest(2L, null, "t_%", null));
        Message.TablesRequest t = (Message.TablesRequest) out;
        assertEquals(2L, t.requestId());
        assertNull(t.schemaPattern());
        assertEquals("t_%", t.tableNamePattern());
        assertNull(t.types());
    }

    @Test
    void tablesRequestRoundTripWithTypes() {
        Message out = roundTrip(new Message.TablesRequest(3L, "s", "t", new String[]{"TABLE", "VIEW"}));
        Message.TablesRequest t = (Message.TablesRequest) out;
        assertEquals(3L, t.requestId());
        assertEquals("s", t.schemaPattern());
        assertArrayEquals(new String[]{"TABLE", "VIEW"}, t.types());
    }

    @Test
    void tablesRequestEmptyTypesRoundTrip() {
        Message out = roundTrip(new Message.TablesRequest(4L, null, null, new String[0]));
        Message.TablesRequest t = (Message.TablesRequest) out;
        assertEquals(0, t.types().length);
    }

    @Test
    void columnsRequestRoundTrip() {
        Message out = roundTrip(new Message.ColumnsRequest(5L, "public", "users", "%name%"));
        Message.ColumnsRequest c = (Message.ColumnsRequest) out;
        assertEquals(5L, c.requestId());
        assertEquals("public", c.schemaPattern());
        assertEquals("users", c.tableNamePattern());
        assertEquals("%name%", c.columnNamePattern());
    }
}
