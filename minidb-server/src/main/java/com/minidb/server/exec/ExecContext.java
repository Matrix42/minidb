package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.common.TableHandle;
import com.minidb.server.storage.StorageManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ExecContext {

    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final RexInterpreter interpreter;
    private final String currentSchema;
    // Transient tables for recursive CTE (WITH RECURSIVE). Keyed by the CTE
    // name; MiniDbRepeatUnion registers the current working rows here and the
    // recursive body's scan reads them back. Per-query, so a plain HashMap
    // suffices (no concurrent access).
    private final Map<String, List<Object[]>> transientTables = new HashMap<>();

    // CSE 缓存:key 为子树 digest,value 为物化后的批列表
    private final Map<String, List<VectorSchemaRoot>> cseCache = new HashMap<>();

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
    public TableHandle getTable(String schemaName, String tableName) {
        return storage.getTable(schemaName, tableName);
    }

    /**
     * Resolve a bare table name against the current schema. Used by EXPLAIN
     * paths that only have a bare name from {@code TableScan.getQualifiedName()}
     * last segment, without a live operator tree carrying the schema.
     */
    public TableHandle getTable(String tableName) {
        return storage.getTable(currentSchema, tableName);
    }

    public void putTransientTable(String name, List<Object[]> rows) {
        transientTables.put(name, rows);
    }

    public List<Object[]> transientTable(String name) {
        return transientTables.get(name);
    }

    public void removeTransientTable(String name) {
        transientTables.remove(name);
    }

    /** CSE 缓存:按 key 取已物化的批列表,无命中返回 null。 */
    public List<VectorSchemaRoot> getCseCache(String key) {
        return cseCache.get(key);
    }

    /** CSE 缓存:存入物化结果。 */
    public void putCseCache(String key, List<VectorSchemaRoot> batches) {
        cseCache.put(key, batches);
    }

    /** 释放 CSE 缓存中所有批(查询结束时调用)。 */
    public void close() {
        for (List<VectorSchemaRoot> batches : cseCache.values()) {
            for (VectorSchemaRoot b : batches) {
                b.close();
            }
        }
        cseCache.clear();
    }

    /** 检查当前线程是否被中断(客户端断连→Future.cancel→线程中断),是则抛异常。 */
    public static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new RuntimeException("query cancelled");
        }
    }
}
