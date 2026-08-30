package com.minidb.server.exec;

import org.apache.arrow.vector.VectorSchemaRoot;

public sealed interface QueryResult {

    record Rows(VectorSchemaRoot data) implements QueryResult {}

    record Cursor(CursorHandle handle) implements QueryResult {}

    record Update(long count) implements QueryResult {}

    record UseSchema(String schemaName) implements QueryResult {}
}
