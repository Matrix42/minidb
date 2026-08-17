package com.minidb.tpcds;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.functions.Kernels;
import com.minidb.server.storage.StorageManager;
import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.SimpleTable;
import com.minidb.storage.common.TableSchema;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.util.DateString;

/**
 * TPC-DS 数据生成:用 teradata tpcds 库在 JVM 内生成行,按列类型解析成 Arrow
 * 向量,直接写 part 文件到表目录(绕过 SQL INSERT),并注册 catalog。
 */
public class TpcdsDataGenerator {

    private static final int MAX_BATCH_ROWS = 4096;

    /**
     * 维度表的单列主键(TPC-DS 标准)。给维度表建主键后,CBO 的
     * RelMdDistinctRowCount 能算出 join 键的 distinct 值,否则 join 行数估算退化为
     * 笛卡尔积(left×right),代价爆炸、plan 选错。事实表是复合主键,对估算帮助小,不设。
     */
    private static final Map<String, List<String>> PRIMARY_KEYS = Map.ofEntries(
            Map.entry("call_center", List.of("cc_call_center_sk")),
            Map.entry("catalog_page", List.of("cp_catalog_page_sk")),
            Map.entry("customer", List.of("c_customer_sk")),
            Map.entry("customer_address", List.of("ca_address_sk")),
            Map.entry("customer_demographics", List.of("cd_demo_sk")),
            Map.entry("date_dim", List.of("d_date_sk")),
            Map.entry("household_demographics", List.of("hd_demo_sk")),
            Map.entry("income_band", List.of("ib_income_band_sk")),
            Map.entry("item", List.of("i_item_sk")),
            Map.entry("promotion", List.of("p_promo_sk")),
            Map.entry("reason", List.of("r_reason_sk")),
            Map.entry("ship_mode", List.of("sm_ship_mode_sk")),
            Map.entry("store", List.of("s_store_sk")),
            Map.entry("time_dim", List.of("t_time_sk")),
            Map.entry("warehouse", List.of("w_warehouse_sk")),
            Map.entry("web_page", List.of("wp_web_page_sk")),
            Map.entry("web_site", List.of("web_site_sk")));

    public void generate(double scale, Path dataDir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            Session session = Session.getDefaultSession().withScale(scale);
            for (Table table : Table.getBaseTables()) {
                generateTable(storage, session, table);
            }
            storage.close();
        }
    }

    private void generateTable(StorageManager storage, Session session, Table table) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (com.teradata.tpcds.column.Column c : table.getColumns()) {
            columns.add(toColumnMeta(c));
        }
        List<String> pk = PRIMARY_KEYS.getOrDefault(table.getName().toLowerCase(), List.of());
        TableSchema schema = new TableSchema("public", table.getName().toLowerCase(),
                columns, pk, List.of(), List.of());
        SimpleTable target = storage.createTable(schema);

        Results results = Results.constructResults(table, session);
        VectorSchemaRoot batch = target.newBatchRoot();
        batch.allocateNew();
        int rows = 0;
        for (List<List<String>> parentAndChild : results) {
            List<String> row = parentAndChild.get(0);
            for (int c = 0; c < columns.size(); c++) {
                setValue(batch.getVector(c), rows, columns.get(c), row.get(c));
            }
            rows++;
            if (rows >= MAX_BATCH_ROWS) {
                batch.setRowCount(rows);
                target.writePart(batch);
                batch.close();
                batch = target.newBatchRoot();
                batch.allocateNew();
                rows = 0;
            }
        }
        if (rows > 0) {
            batch.setRowCount(rows);
            target.writePart(batch);
        }
        batch.close();
    }

    /** tpcds 列类型 → MiniDB 列类型。IDENTIFIER 是 64 位代理键,落 BIGINT 防溢出。 */
    private static ColumnMeta toColumnMeta(com.teradata.tpcds.column.Column c) {
        com.teradata.tpcds.column.ColumnType.Base base = c.getType().getBase();
        ColumnType type = switch (base) {
            case INTEGER -> ColumnType.INTEGER;
            case IDENTIFIER -> ColumnType.BIGINT;
            case DECIMAL -> ColumnType.DECIMAL;
            case VARCHAR, CHAR -> ColumnType.VARCHAR;
            case DATE -> ColumnType.DATE;
            case TIME -> ColumnType.TIME;
        };
        int precision = ColumnMeta.PRECISION_UNSET;
        int scale = ColumnMeta.SCALE_UNSET;
        if (base == com.teradata.tpcds.column.ColumnType.Base.DECIMAL) {
            precision = c.getType().getPrecision().orElse(10);
            scale = c.getType().getScale().orElse(0);
        }
        return new ColumnMeta(c.getName(), type, precision, scale, true);
    }

    private static void setValue(FieldVector v, int row, ColumnMeta column, String raw) {
        if (raw == null) {
            v.setNull(row);
            return;
        }
        switch (column.type()) {
            case INTEGER -> ((IntVector) v).setSafe(row, Integer.parseInt(raw));
            case BIGINT -> ((BigIntVector) v).setSafe(row, Long.parseLong(raw));
            case DECIMAL -> ((DecimalVector) v).setSafe(row,
                    Kernels.scaleTo((DecimalVector) v, new BigDecimal(raw)));
            case VARCHAR -> ((VarCharVector) v).setSafe(row, raw.getBytes(StandardCharsets.UTF_8));
            case DATE -> ((DateDayVector) v).setSafe(row, new DateString(raw).getDaysSinceEpoch());
            case TIME -> ((TimeMilliVector) v).setSafe(row, timeToMillis(raw));
            default -> throw new IllegalArgumentException("unsupported tpcds type: " + column.type());
        }
    }

    private static int timeToMillis(String raw) {
        String[] parts = raw.split(":");
        int h = Integer.parseInt(parts[0]);
        int m = Integer.parseInt(parts[1]);
        int s = Integer.parseInt(parts[2]);
        return (h * 3600 + m * 60 + s) * 1000;
    }
}
