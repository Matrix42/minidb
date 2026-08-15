package com.minidb.storage.common;

import java.nio.file.Path;
import java.util.List;

/**
 * 存储引擎:决定「一张表的目录怎么定位、怎么删除」。数据本身是目录里的 part 文件,
 * 由 {@link ArrowTable} 直接读写(写入落盘、读取递归读 part)。本接口只负责目录级的
 * 组织,为将来换 part 格式(如 Parquet part)留扩展点。
 */
public interface TableStorage {

    /** 一张表在存储中的定位。 */
    record TableRef(String schemaName, String tableName) {
    }

    /** 列出所有已存在的表。 */
    List<TableRef> listTables();

    /** 一张表的目录路径(可能尚未创建)。 */
    Path tableDir(String schemaName, String tableName);

    /** 删除一张表的数据目录。 */
    void delete(String schemaName, String tableName);

    /** 删除一个 schema 的数据目录。 */
    void deleteSchema(String schemaName);
}
