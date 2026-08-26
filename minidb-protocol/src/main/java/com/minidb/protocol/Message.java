package com.minidb.protocol;
import io.netty.buffer.ByteBuf;

public interface Message {

    record Handshake(byte version) implements Message {
    }

    record HandshakeAck(byte version) implements Message {
    }

    record ExecuteRequest(long requestId, String sql, int fetchSize) implements Message {
        public ExecuteRequest(long requestId, String sql) {
            this(requestId, sql, 0);
        }
    }

    record CloseRequest() implements Message {
    }

    record FetchRequest(long requestId, long cursorId, int maxRows) implements Message {
    }

    record CloseCursorRequest(long cursorId) implements Message {
    }

    record BeginRequest(long requestId) implements Message {}

    record CommitRequest(long requestId) implements Message {}

    record RollbackRequest(long requestId) implements Message {}

    record SetAutoCommitRequest(long requestId, boolean autoCommit) implements Message {}

    record CommitResponse(long requestId, boolean ok, String error) implements Message {
        public static CommitResponse ok(long requestId) {
            return new CommitResponse(requestId, true, "");
        }
        public static CommitResponse error(long requestId, String error) {
            return new CommitResponse(requestId, false, error);
        }
    }

    record ExecuteResponse(long requestId, boolean ok, String error) implements Message {
        public static ExecuteResponse ok(long requestId) {
            return new ExecuteResponse(requestId, true, "");
        }

        public static ExecuteResponse error(long requestId, String error) {
            return new ExecuteResponse(requestId, false, error);
        }
    }

    record ArrowBatch(long requestId, boolean lastBatch, ByteBuf data) implements Message {
    }

    /** 分页续批:仅 IPC record-batch message(无 magic/schema/EOS),schema 在首批发过,按 cursorId 复用。 */
    record ArrowContinuation(long requestId, long cursorId, boolean lastBatch,
                             ByteBuf data) implements Message {
    }

    record UpdateCount(long requestId, long count) implements Message {
    }

    record SchemasRequest(long requestId, String schemaPattern) implements Message {
    }

    record TablesRequest(long requestId, String schemaPattern,
                         String tableNamePattern, String[] types) implements Message {
    }

    record ColumnsRequest(long requestId, String schemaPattern,
                          String tableNamePattern, String columnNamePattern) implements Message {
    }
}
