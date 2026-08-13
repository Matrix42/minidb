package com.minidb.server.calcite;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.TableSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;

public class MiniDbCalciteTable extends AbstractTable {

    private final TableSchema schema;

    public MiniDbCalciteTable(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnMeta column : schema.columns()) {
            RelDataType type = ArrowTypes.toCalciteType(column, typeFactory);
            builder.add(column.name(), type).nullable(true);
        }
        return builder.build();
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }
}
