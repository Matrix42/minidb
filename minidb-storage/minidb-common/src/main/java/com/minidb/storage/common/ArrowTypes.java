package com.minidb.storage.common;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ArrowTypes {

    /** 保存到 Arrow Field 元数据里、标识声明类型名的 key(类型名端到端保真的核心)。 */
    public static final String TYPE_NAME_METADATA = "minidb.type";

    private static final int DEFAULT_DECIMAL_PRECISION = 10;
    private static final int DEFAULT_DECIMAL_SCALE = 0;

    private ArrowTypes() {}

    public static ColumnType fromSqlTypeName(String name) {
        String upper = name.toUpperCase(Locale.ROOT);
        return switch (upper) {
            case "SMALLINT" -> ColumnType.SMALLINT;
            case "INTEGER", "INT" -> ColumnType.INTEGER;
            case "BIGINT" -> ColumnType.BIGINT;
            case "REAL" -> ColumnType.REAL;
            case "FLOAT" -> ColumnType.FLOAT;
            case "DOUBLE", "DOUBLE PRECISION" -> ColumnType.DOUBLE;
            case "DECIMAL" -> ColumnType.DECIMAL;
            case "NUMERIC" -> ColumnType.NUMERIC;
            case "VARCHAR" -> ColumnType.VARCHAR;
            case "CHAR", "CHARACTER" -> ColumnType.CHAR;
            case "NCHAR", "NATIONAL CHARACTER" -> ColumnType.NCHAR;
            case "NVARCHAR", "NATIONAL CHARACTER VARYING" -> ColumnType.NVARCHAR;
            case "BOOLEAN" -> ColumnType.BOOLEAN;
            case "DATE" -> ColumnType.DATE;
            case "TIME" -> ColumnType.TIME;
            case "TIMESTAMP" -> ColumnType.TIMESTAMP;
            case "BINARY" -> ColumnType.BINARY;
            case "VARBINARY", "BINARY VARYING" -> ColumnType.VARBINARY;
            default -> throw new IllegalArgumentException("unsupported column type: " + name);
        };
    }

    public static String toSqlTypeName(ColumnType type) {
        return switch (type) {
            case SMALLINT -> "SMALLINT";
            case INTEGER -> "INTEGER";
            case BIGINT -> "BIGINT";
            case REAL -> "REAL";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case DECIMAL -> "DECIMAL";
            case NUMERIC -> "NUMERIC";
            case VARCHAR -> "VARCHAR";
            case CHAR -> "CHAR";
            case NCHAR -> "NCHAR";
            case NVARCHAR -> "NVARCHAR";
            case BOOLEAN -> "BOOLEAN";
            case DATE -> "DATE";
            case TIME -> "TIME";
            case TIMESTAMP -> "TIMESTAMP";
            case BINARY -> "BINARY";
            case VARBINARY -> "VARBINARY";
            default -> throw new IllegalArgumentException("unknown type: " + type);
        };
    }

    public static ArrowType arrowType(ColumnType type) {
        return arrowTypeOf(type, DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
    }

    public static Field field(ColumnMeta meta) {
        // field 可空性由 ColumnMeta.nullable 决定:NOT NULL/主键列(TableSchema 已强制)
        // 报 non-nullable FieldType,JDBC getMetaData().isNullable 才能正确反馈可空性。
        return new Field(
                meta.name(),
                new FieldType(
                        Boolean.TRUE.equals(meta.nullable()),
                        arrowTypeOf(meta.type(), meta.precision(), meta.scale()),
                        null,
                        Map.of(TYPE_NAME_METADATA, meta.type().name())),
                List.of());
    }

    public static Schema arrowSchema(TableSchema schema) {
        List<Field> fields = new ArrayList<>();
        for (ColumnMeta column : schema.columns()) {
            fields.add(field(column));
        }
        return new Schema(fields, Map.of("schema", schema.schemaName()));
    }

    public static Field field(RelDataTypeField dataTypeField) {
        return new Field(
                dataTypeField.getName(),
                FieldType.nullable(arrowTypeOf(dataTypeField.getType())),
                List.of());
    }

    public static Field field(RelDataType type, String name) {
        return new Field(name, FieldType.nullable(arrowTypeOf(type)), List.of());
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
        return switch (type) {
            case SMALLINT -> new ArrowType.Int(16, true);
            case INTEGER -> new ArrowType.Int(32, true);
            case BIGINT -> new ArrowType.Int(64, true);
            case REAL, FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DECIMAL ->
                    new ArrowType.Decimal(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE, 128);
            case VARCHAR, CHAR -> ArrowType.Utf8.INSTANCE;
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            case DATE -> new ArrowType.Date(DateUnit.DAY);
            case TIME -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case BINARY, VARBINARY -> ArrowType.Binary.INSTANCE;
            default -> throw new IllegalArgumentException("unsupported sql type: " + type);
        };
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
            case NUMERIC:
                {
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
        SqlTypeName sqlType =
                switch (meta.type()) {
                    case SMALLINT -> SqlTypeName.SMALLINT;
                    case INTEGER -> SqlTypeName.INTEGER;
                    case BIGINT -> SqlTypeName.BIGINT;
                    case REAL, FLOAT -> SqlTypeName.REAL;
                    case DOUBLE -> SqlTypeName.DOUBLE;
                    case VARCHAR, CHAR, NCHAR, NVARCHAR ->
                            // CHAR/NCHAR/NVARCHAR 都变长存储、不做定长空格填充(设计简化,见
                            // data-types-design「CHAR/NCHAR/NVARCHAR 语义」),故 Calcite 侧统一映射为
                            // VARCHAR;若映射为 SqlTypeName.CHAR,Calcite 会把插入值空格填充到声明长度。
                            SqlTypeName.VARCHAR;
                    case BOOLEAN -> SqlTypeName.BOOLEAN;
                    case DATE -> SqlTypeName.DATE;
                    case TIME -> SqlTypeName.TIME;
                    case TIMESTAMP -> SqlTypeName.TIMESTAMP;
                    case BINARY, VARBINARY ->
                            // BINARY 与 VARBINARY 同为变长 Binary 存储(设计简化,落 VarBinaryVector);
                            // Calcite 侧统一映射为 VARBINARY,若映射 SqlTypeName.BINARY 会触发定长零填充
                            // 且其 CAST 路径对 VarBinaryVector 源抛异常。声明名靠 Arrow 元数据保真。
                            SqlTypeName.VARBINARY;
                    default -> throw new IllegalArgumentException("unknown type: " + meta.type());
                };
        if (sqlType == SqlTypeName.VARCHAR || sqlType == SqlTypeName.VARBINARY) {
            return factory.createSqlType(sqlType, Integer.MAX_VALUE);
        }
        return factory.createSqlType(sqlType);
    }
}
