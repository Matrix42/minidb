package com.minidb.server.storage;

import com.minidb.server.catalog.CatalogSnapshot;

import java.io.IOException;

/** 元数据持久化接口:JSON 现在,Avro 以后加实现类即可(调用方不动)。 */
public interface CatalogStore extends AutoCloseable {
    CatalogSnapshot load() throws IOException;

    void save(CatalogSnapshot snapshot) throws IOException;

    @Override
    default void close() {}
}
