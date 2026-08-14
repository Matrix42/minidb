package com.minidb.server.calcite;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.stats.Histogram;
import com.minidb.server.stats.StatsEstimator;
import com.minidb.server.stats.TableStats;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

public class MiniDbCalciteTable extends AbstractTable {

    private final TableSchema schema;
    private final MiniDbCatalog catalog;
    private final SelectivityHandler selectivityHandler = new SelectivityHandler();
    private final DistinctRowCountHandler distinctRowCountHandler = new DistinctRowCountHandler();

    public MiniDbCalciteTable(TableSchema schema, MiniDbCatalog catalog) {
        this.schema = schema;
        this.catalog = catalog;
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
        TableStats ts = catalog.getStats(schema.schemaName(), schema.name());
        if (ts == null || ts.stale()) {
            return Statistics.UNKNOWN;
        }
        return Statistics.of((double) ts.rowCount(), List.of());
    }

    /**
     * Returns the metadata handler for this table. Selectivity and distinct-row
     * count cannot be implemented on this class directly: both
     * {@code BuiltInMetadata.Selectivity.Handler} and
     * {@code BuiltInMetadata.DistinctRowCount.Handler} declare a
     * {@code getDef()} whose return types are unrelated
     * ({@code MetadataDef<Selectivity>} vs {@code MetadataDef<DistinctRowCount>}),
     * so a single class implementing both triggers a name clash. Each handler
     * therefore lives in its own private class below.
     */
    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass == BuiltInMetadata.Selectivity.Handler.class) {
            return aClass.cast(selectivityHandler);
        }
        if (aClass == BuiltInMetadata.DistinctRowCount.Handler.class) {
            return aClass.cast(distinctRowCountHandler);
        }
        return super.unwrap(aClass);
    }

    private final class SelectivityHandler implements BuiltInMetadata.Selectivity.Handler {
        @Override
        public @Nullable Double getSelectivity(RelNode r, RelMetadataQuery mq,
                                               @Nullable RexNode predicate) {
            if (predicate == null) {
                return null;
            }
            TableStats ts = catalog.getStats(schema.schemaName(), schema.name());
            if (ts == null || ts.stale()) {
                return null;
            }
            Histogram h = StatsEstimator.histogramForCondition(predicate, schema, ts);
            return h == null ? null : h.selectivity(predicate, h.totalRows());
        }
    }

    private final class DistinctRowCountHandler implements BuiltInMetadata.DistinctRowCount.Handler {
        @Override
        public @Nullable Double getDistinctRowCount(RelNode r, RelMetadataQuery mq,
                                                    ImmutableBitSet groupKey, @Nullable RexNode predicate) {
            if (groupKey.cardinality() != 1) {
                return null;
            }
            TableStats ts = catalog.getStats(schema.schemaName(), schema.name());
            if (ts == null || ts.stale()) {
                return null;
            }
            int col = groupKey.nextSetBit(0);
            if (col < 0 || col >= schema.columns().size()) {
                return null;
            }
            String colName = schema.columns().get(col).name().toLowerCase(Locale.ROOT);
            Histogram h = ts.columnHistograms().get(colName);
            return h == null ? null : (double) h.distinctCount();
        }
    }
}
