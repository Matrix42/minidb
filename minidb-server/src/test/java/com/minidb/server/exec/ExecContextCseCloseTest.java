package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExecContext.close() 必须释放 CSE 缓存里物化的批:EXPLAIN ANALYZE 的 analyze() 路径 不再(经由 CursorHandle)自动
 * close,groovy 会泄漏 Arrow 内存。本测试验证 close 后回收。
 */
class ExecContextCseCloseTest {

    @Test
    void closeReleasesCseCache(@TempDir Path dataDir) {
        try (BufferAllocator allocator = new RootAllocator();
                StorageManager storage =
                        new StorageManager(new MiniDbCatalog(), allocator, dataDir)) {
            ExecContext ctx = new ExecContext(storage, allocator, "public", null);
            long baseline = allocator.getAllocatedMemory();

            // 模拟 CSE 首次执行物化:一个批被放进缓存。
            IntVector v = new IntVector("c", allocator);
            v.setInitialCapacity(4);
            v.allocateNew();
            for (int i = 0; i < 4; i++) {
                v.setSafe(i, i);
            }
            v.setValueCount(4);
            VectorSchemaRoot batch = VectorSchemaRoot.of(v);
            batch.setRowCount(4);
            ctx.putCseCache("k", List.of(batch));
            assertTrue(allocator.getAllocatedMemory() > baseline, "缓存批应占用内存");

            ctx.close();
            assertEquals(baseline, allocator.getAllocatedMemory(), "close 后 CSE 缓存批应被释放");
        }
    }
}
