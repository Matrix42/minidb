package com.minidb.server.catalog;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

public final class ArrowTypes {

    /** 保存到 Arrow Field 元数据里、标识声明类型名的 key(类型名端到端保真的核心)。 */
    public static final String TYPE_NAME_METADATA = "minidb.type";
    private static final int DEFAULT_DECIMAL_PRECISION = 10;
    private static final int DEFAULT_DECIMAL_SCALE = 0;

    private ArrowTypes() {
    }

    public static ColumnType fromSqlTypeName(String name) {
        String upper = name.toUpperCase(Locale.ROOT);
        switch (upper) {
            case "SMALLINT":
                return ColumnType.SMALLINT;
            case "INTEGER":
            case "INT":
                return ColumnType.INTEGER;
            case "BIGINT":
                return ColumnType.BIGINT;
            case "REAL":
                return ColumnType.REAL;
            case "FLOAT":
                return ColumnType.FLOAT;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return ColumnType.DOUBLE;
            case "DECIMAL":
                return ColumnType.DECIMAL;
            case "NUMERIC":
                return ColumnType.NUMERIC;
            case "VARCHAR":
                return ColumnType.VARCHAR;
            case "CHAR":
            case "CHARACTER":
                return ColumnType.CHAR;
            case "NCHAR":
            case "NATIONAL CHARACTER":
                return ColumnType.NCHAR;
            case "NVARCHAR":
            case "NATIONAL CHARACTER VARYING":
                return ColumnType.NVARCHAR;
            case "BOOLEAN":
                return ColumnType.BOOLEAN;
            case "DATE":
                return ColumnType.DATE;
            case "TIME":
                return ColumnType.TIME;
            case "TIMESTAMP":
                return ColumnType.TIMESTAMP;
            case "BINARY":
                return ColumnType.BINARY;
            case "VARBINARY":
            case "BINARY VARYING":
                return ColumnType.VARBINARY;
            default:
                throw new IllegalArgumentException(
                        "unsupported column type: " + name);
        }
    }

    public static String toSqlTypeName(ColumnType type) {
        switch (type) {
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case REAL:
                return "REAL";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL";
            case NUMERIC:
                return "NUMERIC";
            case VARCHAR:
                return "VARCHAR";
            case CHAR:
                return "CHAR";
            case NCHAR:
                return "NCHAR";
            case NVARCHAR:
                return "NVARCHAR";
            case BOOLEAN:
                return "BOOLEAN";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BINARY:
                return "BINARY";
            case VARBINARY:
                return "VARBINARY";
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    public static ArrowType arrowType(ColumnType type, BufferAllocator allocator) {
        return arrowTypeOf(type, DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
    }

    public static Field field(ColumnMeta meta) {
        return new Field(meta.name(),
                new FieldType(true, arrowTypeOf(meta.type(), meta.precision(), meta.scale()),
                        null, Map.of(TYPE_NAME_METADATA, meta.type().name())),
                List.of());
    }

    public static Field field(RelDataTypeField dataTypeField) {
        return new Field(dataTypeField.getName(),
                FieldType.nullable(arrowTypeOf(dataTypeField.getType())),
                List.of());
    }

    public static Field field(RelDataType type, String name) {
        return new Field(name,
                FieldType.nullable(arrowTypeOf(type)),
                List.of());
    }

    private static ArrowType arrowTypeOf(RelDataType type) {
        if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
            int precision = type.getPrecision();
            int scale = type.getScale();
            if (precision < 0) {
                precision = DEFAULT_DECIMAL_PRECISION;
            }
            if (scale < 0) {
                scale = DEFAULT_DECIMAL_SCALE;
            }
            return new ArrowType.Decimal(precision, scale, 128);
        }
        return arrowTypeOf(type.getSqlTypeName());
    }

    private static ArrowType arrowTypeOf(SqlTypeName type) {
        switch (type) {
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case REAL:
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DECIMAL:
                return new ArrowType.Decimal(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE, 128);
            case VARCHAR:
            case CHAR:
                return ArrowType.Utf8.INSTANCE;
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case BINARY:
            case VARBINARY:
                return ArrowType.Binary.INSTANCE;
            default:
                throw new IllegalArgumentException(
                        "unsupported sql type: " + type);
        }
    }

    private static ArrowType arrowTypeOf(ColumnType type, int precision, int scale) {
        switch (type) {
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case REAL:
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DECIMAL:
            case NUMERIC: {
                int p = precision >= 0 ? precision : DEFAULT_DECIMAL_PRECISION;
                int s = scale >= 0 ? scale : DEFAULT_DECIMAL_SCALE;
                return new ArrowType.Decimal(p, s, 128);
            }
            case VARCHAR:
            case CHAR:
            case NCHAR:
            case NVARCHAR:
                return ArrowType.Utf8.INSTANCE;
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case BINARY:
            case VARBINARY:
                return ArrowType.Binary.INSTANCE;
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    public static RelDataType toCalciteType(ColumnMeta meta, RelDataTypeFactory factory) {
        if (meta.type() == ColumnType.DECIMAL || meta.type() == ColumnType.NUMERIC) {
            int precision = meta.precision() >= 0 ? meta.precision() : DEFAULT_DECIMAL_PRECISION;
            int scale = meta.scale() >= 0 ? meta.scale() : DEFAULT_DECIMAL_SCALE;
            return factory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        }
        SqlTypeName sqlType;
        switch (meta.type()) {
            case SMALLINT:
                sqlType = SqlTypeName.SMALLINT;
                break;
            case INTEGER:
                sqlType = SqlTypeName.INTEGER;
                break;
            case BIGINT:
                sqlType = SqlTypeName.BIGINT;
                break;
            case REAL:
            case FLOAT:
                sqlType = SqlTypeName.REAL;
                break;
            case DOUBLE:
                sqlType = SqlTypeName.DOUBLE;
                break;
            case VARCHAR:
            case CHAR:
            case NCHAR:
            case NVARCHAR:
                // CHAR/NCHAR/NVARCHAR 都变长存储、不做定长空格填充(设计简化,见
                // data-types-design「CHAR/NCHAR/NVARCHAR 语义」),故 Calcite 侧统一映射为
                // VARCHAR;若映射为 SqlTypeName.CHAR,Calcite 会把插入值空格填充到声明长度。
                sqlType = SqlTypeName.VARCHAR;
                break;
            case BOOLEAN:
                sqlType = SqlTypeName.BOOLEAN;
                break;
            case DATE:
                sqlType = SqlTypeName.DATE;
                break;
            case TIME:
                sqlType = SqlTypeName.TIME;
                break;
            case TIMESTAMP:
                sqlType = SqlTypeName.TIMESTAMP;
                break;
            case BINARY:
            case VARBINARY:
                // BINARY 与 VARBINARY 同为变长 Binary 存储(设计简化,落 VarBinaryVector);
                // Calcite 侧统一映射为 VARBINARY,若映射 SqlTypeName.BINARY 会触发定长零填充
                // 且其 CAST 路径对 VarBinaryVector 源抛异常。声明名靠 Arrow 元数据保真。
                sqlType = SqlTypeName.VARBINARY;
                break;
            default:
                throw new IllegalArgumentException("unknown type: " + meta.type());
        }
        if (sqlType == SqlTypeName.VARCHAR || sqlType == SqlTypeName.VARBINARY) {
            return factory.createSqlType(sqlType, Integer.MAX_VALUE);
        }
        return factory.createSqlType(sqlType);
    }
}
