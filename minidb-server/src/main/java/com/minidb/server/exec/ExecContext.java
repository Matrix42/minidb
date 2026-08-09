package com.minidb.server.exec;

import com.minidb.server.storage.StorageManager;
import org.apache.arrow.memory.BufferAllocator;

public class ExecContext {

    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final RexInterpreter interpreter;

    public ExecContext(StorageManager storage, BufferAllocator allocator) {
        this.storage = storage;
        this.allocator = allocator;
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
}
