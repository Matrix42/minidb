package com.minidb.server.catalog;

import java.util.List;
import java.util.Locale;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public final class ArrowTypes {

    private ArrowTypes() {
    }

    public static ColumnType fromSqlTypeName(String name) {
        String upper = name.toUpperCase(Locale.ROOT);
        switch (upper) {
            case "INTEGER":
            case "INT":
                return ColumnType.INTEGER;
            case "BIGINT":
                return ColumnType.BIGINT;
            case "DOUBLE":
                return ColumnType.DOUBLE;
            case "VARCHAR":
                return ColumnType.VARCHAR;
            case "BOOLEAN":
                return ColumnType.BOOLEAN;
            case "DATE":
                return ColumnType.DATE;
            case "TIMESTAMP":
                return ColumnType.TIMESTAMP;
            default:
                throw new IllegalArgumentException(
                        "unsupported column type: " + name);
        }
    }

    public static String toSqlTypeName(ColumnType type) {
        switch (type) {
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case DOUBLE:
                return "DOUBLE";
            case VARCHAR:
                return "VARCHAR";
            case BOOLEAN:
                return "BOOLEAN";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "TIMESTAMP";
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    public static ArrowType arrowType(ColumnType type, BufferAllocator allocator) {
        return arrowTypeOf(type);
    }

    public static Field field(ColumnMeta meta) {
        return new Field(meta.name(), FieldType.nullable(arrowTypeOf(meta.type())),
                List.of());
    }

    private static ArrowType arrowTypeOf(ColumnType type) {
        switch (type) {
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case VARCHAR:
                return ArrowType.Utf8.INSTANCE;
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    public static RelDataType toCalciteType(ColumnType type, RelDataTypeFactory factory) {
        SqlTypeName sqlType;
        switch (type) {
            case INTEGER:
                sqlType = SqlTypeName.INTEGER;
                break;
            case BIGINT:
                sqlType = SqlTypeName.BIGINT;
                break;
            case DOUBLE:
                sqlType = SqlTypeName.DOUBLE;
                break;
            case VARCHAR:
                sqlType = SqlTypeName.VARCHAR;
                break;
            case BOOLEAN:
                sqlType = SqlTypeName.BOOLEAN;
                break;
            case DATE:
                sqlType = SqlTypeName.DATE;
                break;
            case TIMESTAMP:
                sqlType = SqlTypeName.TIMESTAMP;
                break;
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
        if (sqlType == SqlTypeName.VARCHAR) {
            return factory.createSqlType(sqlType, Integer.MAX_VALUE);
        }
        return factory.createSqlType(sqlType);
    }
}
