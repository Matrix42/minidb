package com.minidb.storage.common;

import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * part 文件的物理编码格式。一个 part 是一个数据段(文件),本接口抽象它的读写;
 * {@code ArrowPartFormat}(Arrow IPC)与 {@code ParquetPartFormat} 是不同实现。
 */
public interface PartFormat {

    /** 把一个 batch 写成一个 part 文件。 */
    void write(Path part, VectorSchemaRoot batch);

    /**
     * 读一个 part 文件为一个 batch,产出 schema 必须等于 {@code schema}。
     * schema 由 catalog 提供(权威,含列名/类型/元数据);Arrow IPC 文件自描述、参数冗余,
     * Parquet 靠它重建带元数据的正确 schema(否则 CHAR/BINARY 等声明类型名会丢)。
     */
    VectorSchemaRoot read(Path part, Schema schema, BufferAllocator allocator);

    /** 统计一个 part 文件的行数。 */
    long rowCount(Path part, BufferAllocator allocator);

    /** part 文件的扩展名(不含点)。默认 arrow,Parquet 覆写为 parquet。 */
    default String fileExtension() {
        return "arrow";
    }
}
