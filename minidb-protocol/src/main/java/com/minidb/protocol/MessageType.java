package com.minidb.protocol;

public final class MessageType {
    public static final byte HANDSHAKE = 0x01;
    public static final byte HANDSHAKE_ACK = 0x02;
    public static final byte EXECUTE_REQUEST = 0x10;
    public static final byte CLOSE_REQUEST = 0x11;
    public static final byte EXECUTE_RESPONSE = 0x20;
    public static final byte ARROW_BATCH = 0x21;
    public static final byte ARROW_CONTINUATION = 0x23;
    public static final byte UPDATE_COUNT = 0x22;
    public static final byte SCHEMAS_REQUEST = 0x12;
    public static final byte TABLES_REQUEST = 0x13;
    public static final byte COLUMNS_REQUEST = 0x14;
    public static final byte FETCH_REQUEST = 0x15;
    public static final byte CLOSE_CURSOR_REQUEST = 0x16;
    public static final byte BEGIN_REQUEST      = 0x17;
    public static final byte COMMIT_REQUEST     = 0x18;
    public static final byte ROLLBACK_REQUEST   = 0x19;
    public static final byte SET_AUTOCOMMIT     = 0x1A;
    public static final byte COMMIT_RESPONSE    = 0x24;

    private MessageType() {
    }
}
