package com.minidb.server.exec;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class MetadataExecutor {

    private static final ArrowType VARCHAR = ArrowType.Utf8.INSTANCE;

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;

    public MetadataExecutor(MiniDbCatalog catalog, BufferAllocator allocator) {
        this.catalog = catalog;
        this.allocator = allocator;
    }

    public VectorSchemaRoot schemas(String schemaPattern) {
        Pattern like = compileLike(schemaPattern);
        List<String> matched = new ArrayList<>();
        for (String s : catalog.schemaNames()) {
            if (like == null || like.matcher(s).matches()) {
                matched.add(s);
            }
        }
        matched.sort(String::compareTo);
        VarCharVector schem = new VarCharVector("TABLE_SCHEM", allocator);
        VarCharVector cat = new VarCharVector("TABLE_CATALOG", allocator);
        schem.setInitialCapacity(matched.size());
        cat.setInitialCapacity(matched.size());
        schem.allocateNew();
        cat.allocateNew();
        for (int i = 0; i < matched.size(); i++) {
            schem.setSafe(i, matched.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        schem.setValueCount(matched.size());
        cat.setValueCount(matched.size());
        return VectorSchemaRoot.of(schem, cat);
    }

    public VectorSchemaRoot tables(String schemaPattern, String tableNamePattern, String[] types) {
        if (!acceptsType(types)) {
            return emptyRoot(tableFields());
        }
        Pattern schemaLike = compileLike(schemaPattern);
        Pattern tableLike = compileLike(tableNamePattern);
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        List<String[]> rows = new ArrayList<>(); // [schema, table]
        for (String schema : schemas) {
            if (schemaLike != null && !schemaLike.matcher(schema).matches()) continue;
            List<String> tableNames = new ArrayList<>(catalog.tableNames(schema));
            tableNames.sort(String::compareTo);
            for (String table : tableNames) {
                if (tableLike != null && !tableLike.matcher(table).matches()) continue;
                rows.add(new String[]{schema, table});
            }
        }
        return buildTablesRoot(rows);
    }

    private boolean acceptsType(String[] types) {
        if (types == null || types.length == 0) return true;
        for (String t : types) {
            if (t != null && t.equalsIgnoreCase("TABLE")) return true;
        }
        return false;
    }

    private VectorSchemaRoot buildTablesRoot(List<String[]> rows) {
        int n = rows.size();
        VarCharVector cat = vc("TABLE_CAT", n);
        VarCharVector schem = vc("TABLE_SCHEM", n);
        VarCharVector name = vc("TABLE_NAME", n);
        VarCharVector type = vc("TABLE_TYPE", n);
        VarCharVector remarks = vc("REMARKS", n);
        VarCharVector typeCat = vc("TYPE_CAT", n);
        VarCharVector typeSchem = vc("TYPE_SCHEM", n);
        VarCharVector typeName = vc("TYPE_NAME", n);
        VarCharVector selfRef = vc("SELF_REFERENCING_COL_NAME", n);
        VarCharVector refGen = vc("REF_GENERATION", n);
        for (int i = 0; i < n; i++) {
            schem.setSafe(i, rows.get(i)[0].getBytes(java.nio.charset.StandardCharsets.UTF_8));
            name.setSafe(i, rows.get(i)[1].getBytes(java.nio.charset.StandardCharsets.UTF_8));
            type.setSafe(i, "TABLE".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        for (VarCharVector v : new VarCharVector[]{cat, schem, name, type, remarks, typeCat, typeSchem, typeName, selfRef, refGen}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(cat, schem, name, type, remarks, typeCat, typeSchem, typeName, selfRef, refGen);
    }

    private List<Field> tableFields() {
        return java.util.List.of(
                field("TABLE_CAT"), field("TABLE_SCHEM"), field("TABLE_NAME"),
                field("TABLE_TYPE"), field("REMARKS"), field("TYPE_CAT"),
                field("TYPE_SCHEM"), field("TYPE_NAME"),
                field("SELF_REFERENCING_COL_NAME"), field("REF_GENERATION"));
    }

    private VarCharVector vc(String name, int capacity) {
        VarCharVector v = new VarCharVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }

    private static Field field(String name) {
        return new Field(name, FieldType.nullable(VARCHAR), java.util.List.of());
    }

    private VectorSchemaRoot emptyRoot(List<Field> fields) {
        VectorSchemaRoot root = VectorSchemaRoot.of(fields.stream()
                .map(f -> {
                    FieldVector v = f.createVector(allocator);
                    v.allocateNew();
                    return v;
                })
                .toArray(FieldVector[]::new));
        root.setRowCount(0);
        return root;
    }

    public VectorSchemaRoot columns(String schemaPattern, String tableNamePattern, String columnNamePattern) {
        Pattern schemaLike = compileLike(schemaPattern);
        Pattern tableLike = compileLike(tableNamePattern);
        Pattern colLike = compileLike(columnNamePattern);
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        List<Row> rows = new ArrayList<>();
        for (String schema : schemas) {
            if (schemaLike != null && !schemaLike.matcher(schema).matches()) continue;
            List<String> tableNames = new ArrayList<>(catalog.tableNames(schema));
            tableNames.sort(String::compareTo);
            for (String table : tableNames) {
                if (tableLike != null && !tableLike.matcher(table).matches()) continue;
                TableSchema ts = catalog.getTable(schema, table);
                List<ColumnMeta> cols = ts.columns();
                for (int idx = 0; idx < cols.size(); idx++) {
                    ColumnMeta col = cols.get(idx);
                    if (colLike != null && !colLike.matcher(col.name()).matches()) continue;
                    rows.add(new Row(schema, table, col, idx + 1));
                }
            }
        }
        return buildColumnsRoot(rows);
    }

    private record Row(String schema, String table, ColumnMeta column, int ordinal) {}

    private VectorSchemaRoot buildColumnsRoot(List<Row> rows) {
        int n = rows.size();
        VarCharVector tableCat = vc("TABLE_CAT", n);
        VarCharVector tableSchem = vc("TABLE_SCHEM", n);
        VarCharVector tableName = vc("TABLE_NAME", n);
        VarCharVector colName = vc("COLUMN_NAME", n);
        IntVector dataType = intVec("DATA_TYPE", n);
        VarCharVector typeName = vc("TYPE_NAME", n);
        IntVector colSize = intVec("COLUMN_SIZE", n);
        IntVector bufLen = intVec("BUFFER_LENGTH", n);
        IntVector decDigits = intVec("DECIMAL_DIGITS", n);
        IntVector numPrecRadix = intVec("NUM_PREC_RADIX", n);
        IntVector nullable = intVec("NULLABLE", n);
        VarCharVector remarks = vc("REMARKS", n);
        VarCharVector colDef = vc("COLUMN_DEF", n);
        IntVector sqlDataType = intVec("SQL_DATA_TYPE", n);
        IntVector sqlDateTimeSub = intVec("SQL_DATETIME_SUB", n);
        IntVector charOctetLen = intVec("CHAR_OCTET_LENGTH", n);
        IntVector ordinal = intVec("ORDINAL_POSITION", n);
        VarCharVector isNullable = vc("IS_NULLABLE", n);
        VarCharVector scopeCat = vc("SCOPE_CATALOG", n);
        VarCharVector scopeSchem = vc("SCOPE_SCHEMA", n);
        VarCharVector scopeTable = vc("SCOPE_TABLE", n);
        SmallIntVector sourceDataType = smallInt("SOURCE_DATA_TYPE", n);
        VarCharVector isAutoInc = vc("IS_AUTOINCREMENT", n);
        VarCharVector isGenCol = vc("IS_GENERATEDCOLUMN", n);
        for (int i = 0; i < n; i++) {
            Row r = rows.get(i);
            tableSchem.setSafe(i, r.schema().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            tableName.setSafe(i, r.table().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            colName.setSafe(i, r.column().name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            dataType.setSafe(i, sqlType(r.column().type()));
            typeName.setSafe(i, ArrowTypes.toSqlTypeName(r.column().type()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            // DECIMAL/NUMERIC 报精度/小数位(JDBC COLUMN_SIZE/DECIMAL_DIGITS 语义),
            // 其余类型恒 0(ColumnMeta.precision/scale 对非 decimal 类型恒 UNSET=-1)。
            if (isDecimalType(r.column().type())) {
                colSize.setSafe(i, Math.max(r.column().precision(), 0));
                decDigits.setSafe(i, Math.max(r.column().scale(), 0));
            } else {
                colSize.setSafe(i, 0);
                decDigits.setSafe(i, 0);
            }
            bufLen.setSafe(i, 0);
            if (isIntegerType(r.column().type())) numPrecRadix.setSafe(i, 10);
            nullable.setSafe(i, 1); // columnNullable
            ordinal.setSafe(i, r.ordinal());
            isNullable.setSafe(i, "YES".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            isAutoInc.setSafe(i, "NO".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            isGenCol.setSafe(i, "NO".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        for (VarCharVector v : new VarCharVector[]{tableCat, tableSchem, tableName, colName, typeName,
                remarks, colDef, isNullable, scopeCat, scopeSchem, scopeTable, isAutoInc, isGenCol}) {
            v.setValueCount(n);
        }
        for (IntVector v : new IntVector[]{dataType, colSize, bufLen, decDigits, numPrecRadix,
                nullable, sqlDataType, sqlDateTimeSub, charOctetLen, ordinal}) {
            v.setValueCount(n);
        }
        sourceDataType.setValueCount(n);
        return VectorSchemaRoot.of(tableCat, tableSchem, tableName, colName, dataType, typeName,
                colSize, bufLen, decDigits, numPrecRadix, nullable, remarks, colDef,
                sqlDataType, sqlDateTimeSub, charOctetLen, ordinal, isNullable,
                scopeCat, scopeSchem, scopeTable, sourceDataType, isAutoInc, isGenCol);
    }

    private IntVector intVec(String name, int capacity) {
        IntVector v = new IntVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }

    private SmallIntVector smallInt(String name, int capacity) {
        SmallIntVector v = new SmallIntVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }

    private static int sqlType(ColumnType type) {
        return switch (type) {
            case SMALLINT -> Types.SMALLINT;
            case INTEGER -> Types.INTEGER;
            case BIGINT -> Types.BIGINT;
            case REAL, FLOAT -> Types.REAL;
            case DOUBLE -> Types.DOUBLE;
            case DECIMAL, NUMERIC -> Types.DECIMAL;
            case VARCHAR -> Types.VARCHAR;
            case CHAR -> Types.CHAR;
            case NCHAR -> Types.NCHAR;
            case NVARCHAR -> Types.NVARCHAR;
            case BOOLEAN -> Types.BOOLEAN;
            case DATE -> Types.DATE;
            case TIME -> Types.TIME;
            case TIMESTAMP -> Types.TIMESTAMP;
            case BINARY -> Types.BINARY;
            case VARBINARY -> Types.VARBINARY;
        };
    }

    private static boolean isIntegerType(ColumnType type) {
        return type == ColumnType.SMALLINT
                || type == ColumnType.INTEGER
                || type == ColumnType.BIGINT;
    }

    private static boolean isDecimalType(ColumnType type) {
        return type == ColumnType.DECIMAL || type == ColumnType.NUMERIC;
    }

    static Pattern compileLike(String pattern) {
        if (pattern == null) return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < pattern.length()) {
                // JDBC 转义(getSearchStringEscape()="\"):\X 表示字面量 X。
                // DataGrip 等工具会把 schema 名里的 _ 转义为 \_ 以匹配字面下划线。
                appendLiteral(sb, pattern.charAt(i + 1));
                i++;
            } else if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else {
                appendLiteral(sb, c);
            }
        }
        return Pattern.compile(sb.toString(), Pattern.DOTALL);
    }

    private static void appendLiteral(StringBuilder sb, char c) {
        if ("\\.[]{}()*+?^$|".indexOf(c) >= 0) {
            sb.append('\\');
        }
        sb.append(c);
    }
}
