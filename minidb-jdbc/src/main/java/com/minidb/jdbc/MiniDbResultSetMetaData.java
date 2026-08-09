package com.minidb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

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
        FieldVector v = root.getFieldVectors().get(column - 1);
        switch (v.getMinorType()) {
            case INT:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT8:
                return "DOUBLE";
            case VARCHAR:
                return "VARCHAR";
            case BIT:
                return "BOOLEAN";
            case DATEDAY:
                return "DATE";
            case TIMESTAMPMILLI:
                return "TIMESTAMP";
            default:
                return v.getMinorType().name();
        }
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        FieldVector v = root.getFieldVectors().get(column - 1);
        switch (v.getMinorType()) {
            case INT:
                return java.sql.Types.INTEGER;
            case BIGINT:
                return java.sql.Types.BIGINT;
            case FLOAT8:
                return java.sql.Types.DOUBLE;
            case VARCHAR:
                return java.sql.Types.VARCHAR;
            case BIT:
                return java.sql.Types.BOOLEAN;
            case DATEDAY:
                return java.sql.Types.DATE;
            case TIMESTAMPMILLI:
                return java.sql.Types.TIMESTAMP;
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
        return "";
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
