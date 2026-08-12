package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.ArrowTable;
import com.minidb.server.storage.StorageManager;
import org.apache.arrow.memory.BufferAllocator;

public class ExecContext {

    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final RexInterpreter interpreter;
    private final String currentSchema;

    public ExecContext(StorageManager storage, BufferAllocator allocator) {
        this(storage, allocator, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public ExecContext(StorageManager storage, BufferAllocator allocator, String currentSchema) {
        this.storage = storage;
        this.allocator = allocator;
        this.currentSchema = currentSchema;
        this.interpreter = new RexInterpreter(allocator);
    }

    public StorageManager storage() {
        return storage;
    }

    public BufferAllocator allocator() {
        return allocator;
    }

    public RexInterpreter interpreter() {
        return interpreter;
    }

    public String currentSchema() {
        return currentSchema;
    }

    /**
     * Resolve a (schemaName, tableName) pair against storage. Operators extract
     * these from {@code RelOptTable.getQualifiedName()} — the schema is the
     * second-to-last segment, the table the last. Operators stay generic about
     * where the name came from; this is the storage-facing boundary.
     */
    public ArrowTable getTable(String schemaName, String tableName) {
        return storage.getTable(schemaName, tableName);
    }

    public void markDirty(String schemaName, String tableName) {
        storage.markDirty(schemaName, tableName);
    }

    public void markDirty(String tableName) {
        storage.markDirty(currentSchema, tableName);
    }

    /**
     * Resolve a bare table name against the current schema. Used by EXPLAIN
     * paths that only have a bare name from {@code TableScan.getQualifiedName()}
     * last segment, without a live operator tree carrying the schema.
     */
    public ArrowTable getTable(String tableName) {
        return storage.getTable(currentSchema, tableName);
    }
}
