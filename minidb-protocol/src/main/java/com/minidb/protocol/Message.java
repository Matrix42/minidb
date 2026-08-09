package com.minidb.protocol;

public interface Message {

    record Handshake(byte version) implements Message {
    }

    record HandshakeAck(byte version) implements Message {
    }

    record ExecuteRequest(long requestId, String sql) implements Message {
    }

    record CloseRequest() implements Message {
    }

    record ExecuteResponse(long requestId, boolean ok, String error) implements Message {
        public static ExecuteResponse ok(long requestId) {
            return new ExecuteResponse(requestId, true, "");
        }

        public static ExecuteResponse error(long requestId, String error) {
            return new ExecuteResponse(requestId, false, error);
        }
    }

    record ArrowBatch(long requestId, boolean lastBatch, byte[] data) implements Message {
    }

    record UpdateCount(long requestId, long count) implements Message {
    }
}
