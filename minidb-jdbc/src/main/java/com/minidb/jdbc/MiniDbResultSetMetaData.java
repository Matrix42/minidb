package com.minidb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class MiniDbResultSetMetaData implements ResultSetMetaData {

    // Immutable snapshots taken at construction so this object stays valid after
    // the source root is closed (the result set swaps pages and closes old pages).
    private final List<Field> fields;
    private final Map<String, String> customMetadata;

    public MiniDbResultSetMetaData(VectorSchemaRoot root) {
        List<Field> extracted = new ArrayList<>(root.getFieldVectors().size());
        for (FieldVector v : root.getFieldVectors()) {
            extracted.add(v.getField());
        }
        this.fields = extracted;
        this.customMetadata = root.getSchema().getCustomMetadata();
    }

    @Override
    public int getColumnCount() {
        return fields.size();
    }

    @Override
    public String getColumnName(int column) {
        return fields.get(column - 1).getName();
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumnName(column);
    }

    @Override
    public String getColumnTypeName(int column) {
        Field f = fields.get(column - 1);
        Map<String, String> meta = f.getMetadata();
        if (meta != null && meta.containsKey("minidb.type")) {
            return meta.get("minidb.type");
        }
        switch (f.getType().getTypeID()) {
            case Int:
                return ((ArrowType.Int) f.getType()).getBitWidth() == 16 ? "SMALLINT"
                        : ((ArrowType.Int) f.getType()).getBitWidth() == 32 ? "INTEGER" : "BIGINT";
            case FloatingPoint:
                return "DOUBLE";
            case Decimal:
                return "DECIMAL";
            case Utf8:
                return "VARCHAR";
            case Bool:
                return "BOOLEAN";
            case Date:
                return "DATE";
            case Time:
                return "TIME";
            case Timestamp:
                return "TIMESTAMP";
            case Binary:
                return "VARBINARY";
            default:
                return f.getType().getTypeID().name();
        }
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        Field f = fields.get(column - 1);
        switch (f.getType().getTypeID()) {
            case Int: {
                int bitWidth = ((ArrowType.Int) f.getType()).getBitWidth();
                return bitWidth == 16 ? java.sql.Types.SMALLINT
                        : bitWidth == 32 ? java.sql.Types.INTEGER : java.sql.Types.BIGINT;
            }
            case FloatingPoint:
                return ((ArrowType.FloatingPoint) f.getType()).getPrecision()
                        == FloatingPointPrecision.SINGLE ? java.sql.Types.REAL : java.sql.Types.DOUBLE;
            case Decimal:
                return java.sql.Types.DECIMAL;
            case Utf8:
                return java.sql.Types.VARCHAR;
            case Bool:
                return java.sql.Types.BOOLEAN;
            case Date:
                return java.sql.Types.DATE;
            case Time:
                return java.sql.Types.TIME;
            case Timestamp:
                return java.sql.Types.TIMESTAMP;
            case Binary:
                return java.sql.Types.VARBINARY;
            default:
                return java.sql.Types.OTHER;
        }
    }

    @Override
    public int isNullable(int column) {
        // 依据 Arrow Field 的可空性:NOT NULL/主键列(元数据已把 field 标为 non-nullable)
        // 报 columnNoNulls;否则 columnNullable。
        return fields.get(column - 1).isNullable()
                ? ResultSetMetaData.columnNullable
                : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public String getTableName(int column) {
        return "";
    }

    @Override
    public String getSchemaName(int column) {
        return customMetadata != null ? customMetadata.getOrDefault("schema", "") : "";
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public int getColumnDisplaySize(int column) {
        // 近似显示宽度,供 GUI 工具列宽/排序使用。
        ArrowType type = fields.get(column - 1).getType();
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 16 -> 6;
                case 32 -> 11;
                default -> 20;
            };
        }
        if (type instanceof ArrowType.Decimal d) {
            return d.getPrecision() + 2; // 符号 + 小数点
        }
        if (type instanceof ArrowType.FloatingPoint f) {
            return f.getPrecision() == FloatingPointPrecision.SINGLE ? 13 : 22;
        }
        if (type instanceof ArrowType.Bool) return 5;
        if (type instanceof ArrowType.Date) return 10;
        if (type instanceof ArrowType.Time) return 8;
        if (type instanceof ArrowType.Timestamp) return 23;
        // VARCHAR / Binary 等变长:给出默认列宽
        return 40;
    }

    @Override
    public int getPrecision(int column) {
        ArrowType type = fields.get(column - 1).getType();
        if (type instanceof ArrowType.Decimal d) {
            return d.getPrecision();
        }
        if (type instanceof ArrowType.Int i) {
            return i.getBitWidth() == 16 ? 5 : i.getBitWidth() == 32 ? 10 : 19;
        }
        if (type instanceof ArrowType.FloatingPoint f) {
            return f.getPrecision() == FloatingPointPrecision.SINGLE ? 7 : 15;
        }
        return 0;
    }

    @Override
    public int getScale(int column) {
        ArrowType type = fields.get(column - 1).getType();
        return type instanceof ArrowType.Decimal decimal ? decimal.getScale() : 0;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        // 只有文本列(Utf8)大小写敏感;数值/布尔/日期列不适用。
        return fields.get(column - 1).getType() instanceof ArrowType.Utf8;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public boolean isSigned(int column) {
        ArrowType type = fields.get(column - 1).getType();
        return type instanceof ArrowType.Int
                || type instanceof ArrowType.FloatingPoint
                || type instanceof ArrowType.Decimal;
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        Field f = fields.get(column - 1);
        switch (f.getType().getTypeID()) {
            case Int: {
                int bw = ((ArrowType.Int) f.getType()).getBitWidth();
                return bw == 16 ? Short.class.getName()
                        : bw == 32 ? Integer.class.getName() : Long.class.getName();
            }
            case FloatingPoint:
                return ((ArrowType.FloatingPoint) f.getType()).getPrecision()
                        == FloatingPointPrecision.SINGLE ? Float.class.getName() : Double.class.getName();
            case Decimal:
                return java.math.BigDecimal.class.getName();
            case Utf8:
                return String.class.getName();
            case Bool:
                return Boolean.class.getName();
            case Date:
                return java.sql.Date.class.getName();
            case Time:
                return java.sql.Time.class.getName();
            case Timestamp:
                return java.sql.Timestamp.class.getName();
            case Binary:
                return byte[].class.getName();
            default:
                return Object.class.getName();
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new java.sql.SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
