package com.minidb.server.storage;

import com.minidb.server.catalog.TableSchema;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;

/**
 * 存储引擎:决定一张表的数据怎么持久化、怎么加载、怎么删除。
 *
 * <p>内存态恒为 {@link ArrowTable}(Arrow 列式,等价于 ClickHouse 的 Block);本接口抽象的是
 * 「落盘组织方式」这一层——当前默认实现 {@link IpcFileTableStorage} 是每表一个 Arrow IPC
 * 文件,将来可加 MergeTree 类(排序/分区/索引)、内存表、Parquet 等实现,而算子/执行层不变。
 */
public interface TableStorage {

    /** 一张表在存储中的定位。 */
    record TableRef(String schemaName, String tableName) {
    }

    /** 加载结果:内存表 + 实际生效的 schema(无 catalog 回退时由存储推断)。 */
    record LoadedTable(ArrowTable table, TableSchema schema) {
    }

    /** 启动时列出所有已持久化的表。 */
    List<TableRef> listTables();

    /**
     * 加载一张表到内存。{@code schema} 为 null 时(旧目录无 catalog.json 的回退路径)
     * 由存储引擎从自身格式推断 schema;否则以传入 schema 为准。
     */
    LoadedTable load(String schemaName, String tableName, TableSchema schema,
                     BufferAllocator allocator);

    /** 持久化一张内存表。 */
    void save(String schemaName, String tableName, ArrowTable table);

    /** 删除一张表的持久化数据。 */
    void delete(String schemaName, String tableName);

    /** 删除一个 schema 的所有持久化数据。 */
    void deleteSchema(String schemaName);
}
