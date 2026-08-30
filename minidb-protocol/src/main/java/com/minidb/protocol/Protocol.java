package com.minidb.protocol;

public final class Protocol {
    public static final int MAGIC = 0x4D49; // "MI"
    public static final byte VERSION = 2;
    public static final int DEFAULT_PORT = 8899;

    private Protocol() {}
}
