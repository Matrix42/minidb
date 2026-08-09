package com.minidb.protocol;

public final class MessageType {
    public static final byte HANDSHAKE = 0x01;
    public static final byte HANDSHAKE_ACK = 0x02;
    public static final byte EXECUTE_REQUEST = 0x10;
    public static final byte CLOSE_REQUEST = 0x11;
    public static final byte EXECUTE_RESPONSE = 0x20;
    public static final byte ARROW_BATCH = 0x21;
    public static final byte UPDATE_COUNT = 0x22;

    private MessageType() {
    }
}
