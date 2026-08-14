package com.minidb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class MiniDbResultSetMetaData implements ResultSetMetaData {

    private final VectorSchemaRoot root;

    public MiniDbResultSetMetaData(VectorSchemaRoot root) {
        this.root = root;
    }

    @Override
    public int getColumnCount() {
        return root.getFieldVectors().size();
    }

    @Override
    public String getColumnName(int column) {
        return root.getFieldVectors().get(column - 1).getName();
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumnName(column);
    }

    @Override
    public String getColumnTypeName(int column) {
        Field f = root.getFieldVectors().get(column - 1).getField();
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
        Field f = root.getFieldVectors().get(column - 1).getField();
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
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public String getTableName(int column) {
        return "";
    }

    @Override
    public String getSchemaName(int column) {
        Map<String, String> meta = root.getSchema().getCustomMetadata();
        return meta != null ? meta.getOrDefault("schema", "") : "";
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 40;
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
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
        return true;
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
        return Object.class.getName();
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
