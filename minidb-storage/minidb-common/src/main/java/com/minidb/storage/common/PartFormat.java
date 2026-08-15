package com.minidb.storage.common;

import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * part 文件的物理编码格式。一个 part 是一个数据段(文件),本接口抽象它的读写;
 * {@code ArrowPartFormat}(Arrow IPC)与将来的 {@code ParquetPartFormat} 是不同实现。
 */
public interface PartFormat {

    /** 把一个 batch 写成一个 part 文件。 */
    void write(Path part, VectorSchemaRoot batch);

    /** 读一个 part 文件为一个 batch。 */
    VectorSchemaRoot read(Path part, BufferAllocator allocator);

    /** 统计一个 part 文件的行数。 */
    long rowCount(Path part, BufferAllocator allocator);
}
