package com.minidb.storage.parquet;

import com.minidb.storage.common.PartFormat;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Parquet 格式的 part 读写占位:SQL 层 {@code FORMAT parquet} 已接入、元数据可落盘,
 * 但真正读写未实现(需 parquet-java 手写 Arrow↔Parquet 转换)。读写抛未实现,明确暴露边界。
 */
public class ParquetPartFormat implements PartFormat {

    @Override
    public void write(Path part, VectorSchemaRoot batch) {
        throw new UnsupportedOperationException("parquet storage not implemented yet");
    }

    @Override
    public VectorSchemaRoot read(Path part, BufferAllocator allocator) {
        throw new UnsupportedOperationException("parquet storage not implemented yet");
    }

    @Override
    public long rowCount(Path part, BufferAllocator allocator) {
        throw new UnsupportedOperationException("parquet storage not implemented yet");
    }
}
