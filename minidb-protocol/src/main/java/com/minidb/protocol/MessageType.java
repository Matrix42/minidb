package com.minidb.protocol;

public final class MessageType {
    public static final byte HANDSHAKE = 0x01;
    public static final byte HANDSHAKE_ACK = 0x02;
    public static final byte EXECUTE_REQUEST = 0x10;
    public static final byte CLOSE_REQUEST = 0x11;
    public static final byte EXECUTE_RESPONSE = 0x20;
    public static final byte ARROW_BATCH = 0x21;
    public static final byte UPDATE_COUNT = 0x22;
    public static final byte SCHEMAS_REQUEST = 0x12;
    public static final byte TABLES_REQUEST = 0x13;
    public static final byte COLUMNS_REQUEST = 0x14;
    public static final byte FETCH_REQUEST = 0x15;
    public static final byte CLOSE_CURSOR_REQUEST = 0x16;

    private MessageType() {
    }
}
