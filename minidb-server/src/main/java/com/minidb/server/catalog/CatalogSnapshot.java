package com.minidb.server.catalog;

import java.util.List;

/** 格式无关的 catalog 快照:schema 名 + 全部表定义(每表自带所属 schema)。 */
public record CatalogSnapshot(List<String> schemas, List<TableSchema> tables) {
}
