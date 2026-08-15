package com.minidb.server.calcite;

import com.minidb.storage.common.ArrowTypes;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.storage.common.TableSchema;
import com.minidb.server.catalog.ViewDefinition;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;

public class MiniDbCalciteSchema extends AbstractSchema {

    private final MiniDbCatalog catalog;
    private final String schemaName;

    public MiniDbCalciteSchema(MiniDbCatalog catalog, String schemaName) {
        this.catalog = catalog;
        this.schemaName = schemaName.toLowerCase(Locale.ROOT);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap(catalog, schemaName);
    }

    /** 把一个 schema 的表 + 视图合成 Calcite 表映射(视图用 ViewTable,查询时内联展开)。 */
    static Map<String, Table> tableMap(MiniDbCatalog catalog, String schemaName) {
        Map<String, Table> tables = new HashMap<>();
        for (String name : catalog.tableNames(schemaName)) {
            TableSchema ts = catalog.getTable(schemaName, name);
            tables.put(name, new MiniDbCalciteTable(ts, catalog));
        }
        for (ViewDefinition view : catalog.views(schemaName)) {
            tables.put(view.name(), new ViewTable(Object[].class,
                    protoRowType(view), view.querySql(),
                    List.of(CalciteContext.SCHEMA_NAME, schemaName),
                    List.of(schemaName, view.name())));
        }
        return tables;
    }

    /** 视图的列类型延迟求值:ViewTable 需要 RelProtoDataType(展开时才用 typeFactory 物化)。 */
    private static RelProtoDataType protoRowType(ViewDefinition view) {
        return typeFactory -> {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (ColumnMeta column : view.columns()) {
                builder.add(column.name(), ArrowTypes.toCalciteType(column, typeFactory)).nullable(true);
            }
            return builder.build();
        };
    }
}
