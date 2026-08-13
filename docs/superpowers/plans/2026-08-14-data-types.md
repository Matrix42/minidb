# 补全 SQL 标准标量数据类型 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 MiniDB 列类型从 7 种补全到 12+1 种 SQL 标准标量类型,全原生保真(每种类型映射到自己的 Arrow 向量,类型名经 `"minidb.type"` 元数据端到端保真)。

**Architecture:** 扩展 `ColumnType` 枚举 + `ColumnMeta`(加 precision/scale),`ArrowTypes` 四向映射改原生向量(SMALLINT→Int16、REAL/FLOAT→Float4、DECIMAL/NUMERIC→Decimal128、TIME→TimeMilli、BINARY/VARBINARY→VarBinary、CHAR/NCHAR/NVARCHAR→Utf8),类型名经 Arrow Field 元数据 `"minidb.type"` 保真。表达式层(`RexInterpreter`+`Function`+`BuiltInFunctions`)、行转换(`RowVectors`+6 处副本+`RowCopier`+`MiniDbValues`)、聚合、服务端元数据、JDBC 客户端逐层串通。

**Tech Stack:** Java 17、Apache Arrow(Int16/Float4/Decimal/Time/Binary/Utf8 向量)、Apache Calcite 1.42、JUnit 5。

**Spec:** `docs/superpowers/specs/2026-08-14-data-types-design.md`

## Global Constraints

- JDK 17(`JAVA_HOME` 指向 JDK 17)。构建在 bash 下用 `./mvnw.cmd`(不是 `mvnw.cmd`/`mvn`/`cmd //c`)。
- 全量测试:`./mvnw.cmd test`;单模块:`./mvnw.cmd test -pl minidb-server`;单测试类:`./mvnw.cmd test -pl minidb-server -Dtest=ArrowTypesTest`。
- 每改完一个逻辑改动就提交,conventional commit 风格(`feat:`/`fix:`/`test:`/`refactor:`/`docs:`),不 amend、不 `--no-verify`。
- 代码是给人读的:命名自解释,注释只解释 WHY。中文回复用户,代码/标识符/路径保持原文。
- 现有 7 个物理算子与 `minidb-protocol` 尽量不改(本任务只在必要时给算子加类型分支,不重构其结构)。
- 测试用 JUnit 5 + `@TempDir` + `RootAllocator`。

---

## Task 1: 类型模型 — `ColumnType` 枚举 + `ColumnMeta` 扩展

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/ColumnType.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/ColumnMeta.java`

**Interfaces:**
- Produces: `ColumnType` 新增 12 值 `SMALLINT/REAL/FLOAT/CHAR/NCHAR/NVARCHAR/DECIMAL/NUMERIC/TIME/BINARY/VARBINARY`;`ColumnMeta(String name, ColumnType type, int precision, int scale)` record,便捷构造 `ColumnMeta(String name, ColumnType type)` 保留旧调用点。后置任务依赖这两个类。

- [ ] **Step 1: 扩展 `ColumnType` 枚举**

把 `ColumnType.java` 整个文件替换为:

```java
package com.minidb.server.catalog;

public enum ColumnType {
    SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC,
    VARCHAR, CHAR, NCHAR, NVARCHAR, BOOLEAN, DATE, TIME, TIMESTAMP,
    BINARY, VARBINARY
}
```

- [ ] **Step 2: 扩展 `ColumnMeta` record(加 precision/scale)**

把 `ColumnMeta.java` 整个文件替换为:

```java
package com.minidb.server.catalog;

/**
 * 一列元数据。precision/scale 仅对 DECIMAL/NUMERIC 有意义,其余类型恒
 * {@link #PRECISION_UNSET}/{@link #SCALE_UNSET}。
 */
public record ColumnMeta(String name, ColumnType type, int precision, int scale) {

    public static final int PRECISION_UNSET = -1;
    public static final int SCALE_UNSET = -1;

    public ColumnMeta(String name, ColumnType type) {
        this(name, type, PRECISION_UNSET, SCALE_UNSET);
    }
}
```

- [ ] **Step 3: 跑测试确认旧调用点仍编译通过**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: PASS。`new ColumnMeta(name, type)` 便捷构造兜住所有既有调用点(StorageManagerTest/ArrowTableTest/MiniDbCatalogTest 等)。

- [ ] **Step 4: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/ColumnType.java minidb-server/src/main/java/com/minidb/server/catalog/ColumnMeta.java
git commit -m "feat: ColumnType 补全 12 种标量类型 + ColumnMeta 加 precision/scale"
```

---

## Task 2: `ArrowTypes` 四向映射改原生 + Field 元数据

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/ArrowTypes.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteTable.java`
- Test: `minidb-server/src/test/java/com/minidb/server/catalog/ArrowTypesTest.java`

**Interfaces:**
- Consumes: Task 1 的 `ColumnType`/`ColumnMeta`。
- Produces: `ArrowTypes.fromSqlTypeName(String)`(认新类型名)、`toSqlTypeName(ColumnType)`、`arrowType(ColumnType, allocator)`、`field(ColumnMeta)`(带 `"minidb.type"` 元数据 + Decimal precision/scale)、`field(RelDataTypeField)`/`field(RelDataType,String)`、`toCalciteType(ColumnMeta, RelDataTypeFactory)`。后置任务全靠这些映射。

- [ ] **Step 1: 先写失败测试(ArrowTypesTest 新增断言)**

在 `ArrowTypesTest.java` 追加两个测试方法:

```java
@Test
void newSqlTypeNamesMapToColumnType() {
    assertEquals(ColumnType.SMALLINT, ArrowTypes.fromSqlTypeName("SMALLINT"));
    assertEquals(ColumnType.REAL, ArrowTypes.fromSqlTypeName("REAL"));
    assertEquals(ColumnType.FLOAT, ArrowTypes.fromSqlTypeName("FLOAT"));
    assertEquals(ColumnType.DOUBLE, ArrowTypes.fromSqlTypeName("DOUBLE PRECISION"));
    assertEquals(ColumnType.CHAR, ArrowTypes.fromSqlTypeName("CHAR"));
    assertEquals(ColumnType.NCHAR, ArrowTypes.fromSqlTypeName("NCHAR"));
    assertEquals(ColumnType.NVARCHAR, ArrowTypes.fromSqlTypeName("NVARCHAR"));
    assertEquals(ColumnType.DECIMAL, ArrowTypes.fromSqlTypeName("DECIMAL"));
    assertEquals(ColumnType.NUMERIC, ArrowTypes.fromSqlTypeName("NUMERIC"));
    assertEquals(ColumnType.TIME, ArrowTypes.fromSqlTypeName("TIME"));
    assertEquals(ColumnType.BINARY, ArrowTypes.fromSqlTypeName("BINARY"));
    assertEquals(ColumnType.VARBINARY, ArrowTypes.fromSqlTypeName("VARBINARY"));
}

@Test
void decimalFieldCarriesPrecisionScaleAndTypeName() {
    Field f = ArrowTypes.field(new ColumnMeta("price", ColumnType.DECIMAL, 10, 2));
    ArrowType.Decimal d = (ArrowType.Decimal) f.getType();
    assertEquals(10, d.getPrecision());
    assertEquals(2, d.getScale());
    assertEquals("DECIMAL", f.getMetadata().get("minidb.type"));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=ArrowTypesTest`
Expected: FAIL(编译错误 —— `ColumnMeta` 无 precision/scale 构造?不,已有;失败在 `fromSqlTypeName("SMALLINT")` 抛 IllegalArgumentException,及 `field(ColumnMeta)` 无 metadata)。

- [ ] **Step 3: 重写 `ArrowTypes.java`**

把 `ArrowTypes.java` 整个文件替换为(注意 `toCalciteType` 签名从 `ColumnType` 改为 `ColumnMeta`,以携带 DECIMAL precision/scale):

```java
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
            case NCHAR:
            case NVARCHAR:
                sqlType = SqlTypeName.VARCHAR;
                break;
            case CHAR:
                sqlType = SqlTypeName.CHAR;
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
                sqlType = SqlTypeName.BINARY;
                break;
            case VARBINARY:
                sqlType = SqlTypeName.VARBINARY;
                break;
            default:
                throw new IllegalArgumentException("unknown type: " + meta.type());
        }
        if (sqlType == SqlTypeName.VARCHAR || sqlType == SqlTypeName.CHAR
                || sqlType == SqlTypeName.BINARY || sqlType == SqlTypeName.VARBINARY) {
            return factory.createSqlType(sqlType, Integer.MAX_VALUE);
        }
        return factory.createSqlType(sqlType);
    }
}
```

> **注意**:`SqlTypeName.SMALLINT`/`SqlTypeName.REAL`/`SqlTypeName.TIME`/`SqlTypeName.BINARY`/`SqlTypeName.VARBINARY` 是否存在于 Calcite 1.42 的 `SqlTypeName` 枚举,以编译为准。若某个不存在,回退到可用的近似:`SMALLINT`→`SqlTypeName.SMALLINT`(存在);`REAL`/`FLOAT`→`SqlTypeName.REAL`;`BINARY`/`VARBINARY`→`SqlTypeName.BINARY`/`VARBINARY`。若 Calcite 1.42 缺 `BINARY`/`VARBINARY`(老版本可能缺),用 `SqlTypeName.VARBINARY` 或降级 `SqlTypeName.VARBINARY` 不存在则改用 `factory.createSqlType(SqlTypeName.VARCHAR)` 并记录限制——**以编译错误为准逐项修**,不要臆断。

- [ ] **Step 4: 更新 `MiniDbCalciteTable.java` 调用点**

把第 24 行 `ArrowTypes.toCalciteType(column.type(), typeFactory)` 改为:

```java
RelDataType type = ArrowTypes.toCalciteType(column, typeFactory);
```

- [ ] **Step 5: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=ArrowTypesTest`
Expected: PASS(含新增断言)。

- [ ] **Step 6: 跑全量测试确认无回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: PASS。注意:此任务**未**改 `RexInterpreter`/`Function`,故表达式结果仍走旧的 DECIMAL→Float8 归一化;现有测试不触碰新类型列,应全绿。

- [ ] **Step 7: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/ArrowTypes.java minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteTable.java minidb-server/src/test/java/com/minidb/server/catalog/ArrowTypesTest.java
git commit -m "feat: ArrowTypes 四向映射改原生向量 + Field 挂 minidb.type 元数据"
```

---

## Task 3: 存储重载保真 — `StorageManager` 反向映射读元数据与 precision/scale

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java`
- Test: `minidb-server/src/test/java/com/minidb/server/storage/StorageManagerTest.java`

**Interfaces:**
- Consumes: Task 2 的 `ArrowTypes.field(ColumnMeta)`(写入时挂元数据)。
- Produces: `StorageManager.loadAll` 重载后 `catalog.getTable(...).columns()` 恢复精确 `ColumnType` 与 DECIMAL precision/scale(旧文件无元数据回退到 Arrow 类型推断)。

- [ ] **Step 1: 先写失败测试(StorageManagerTest 新增)**

在 `StorageManagerTest.java` 追加:

```java
@Test
void reloadPreservesNewColumnTypesAndDecimalScale(@TempDir Path dir) {
    TableSchema schema = new TableSchema("t", List.of(
            new ColumnMeta("s", ColumnType.SMALLINT),
            new ColumnMeta("r", ColumnType.REAL),
            new ColumnMeta("p", ColumnType.DECIMAL, 10, 2),
            new ColumnMeta("c", ColumnType.CHAR),
            new ColumnMeta("b", ColumnType.VARBINARY)));
    MiniDbCatalog catalog = new MiniDbCatalog();
    try (BufferAllocator allocator = new RootAllocator()) {
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        storage.createTable(schema);
        storage.markDirty("t");
        storage.close();
    }
    MiniDbCatalog catalog2 = new MiniDbCatalog();
    try (BufferAllocator allocator = new RootAllocator()) {
        StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
        storage2.loadAll();
        List<ColumnMeta> cols = catalog2.getTable("t").columns();
        assertEquals(ColumnType.SMALLINT, cols.get(0).type());
        assertEquals(ColumnType.REAL, cols.get(1).type());
        assertEquals(ColumnType.DECIMAL, cols.get(2).type());
        assertEquals(10, cols.get(2).precision());
        assertEquals(2, cols.get(2).scale());
        assertEquals(ColumnType.CHAR, cols.get(3).type());
        assertEquals(ColumnType.VARBINARY, cols.get(4).type());
        storage2.close();
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StorageManagerTest`
Expected: FAIL(`toColumnType` 对新 Arrow 类型抛 `unsupported arrow type in file`,或元数据未读导致 SMALLINT 坍缩成 BIGINT)。

- [ ] **Step 3: 改 `StorageManager.toTableSchema` 与 `toColumnType`**

把 `toTableSchema` 里的 `new ColumnMeta(field.getName(), toColumnType(field.getType()))` 改为 `toColumnMeta(field)`,并把 `toColumnType(ArrowType)` 替换为读 Field 元数据 + precision/scale 的版本:

```java
private static TableSchema toTableSchema(
        org.apache.arrow.vector.types.pojo.Schema arrowSchema,
        String schemaName, String tableName) {
    List<ColumnMeta> columns = new ArrayList<>();
    for (Field field : arrowSchema.getFields()) {
        columns.add(toColumnMeta(field));
    }
    String resolvedSchema = schemaName;
    Map<String, String> meta = arrowSchema.getCustomMetadata();
    if (meta != null && meta.containsKey("schema")) {
        resolvedSchema = meta.get("schema");
    }
    return new TableSchema(resolvedSchema, tableName, columns);
}

private static ColumnMeta toColumnMeta(Field field) {
    Map<String, String> meta = field.getMetadata();
    String typeName = meta != null ? meta.get(ArrowTypes.TYPE_NAME_METADATA) : null;
    if (typeName != null) {
        ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
        if (type == ColumnType.DECIMAL || type == ColumnType.NUMERIC) {
            ArrowType.Decimal d = (ArrowType.Decimal) field.getType();
            return new ColumnMeta(field.getName(), type, d.getPrecision(), d.getScale());
        }
        return new ColumnMeta(field.getName(), type);
    }
    // 旧文件无元数据:回退到 Arrow 类型推断。
    return new ColumnMeta(field.getName(), inferFromArrowType(field.getType()));
}

private static ColumnType inferFromArrowType(ArrowType type) {
    switch (type.getTypeID()) {
        case Int: {
            ArrowType.Int intType = (ArrowType.Int) type;
            int w = intType.getBitWidth();
            if (w == 16) {
                return ColumnType.SMALLINT;
            }
            return w == 32 ? ColumnType.INTEGER : ColumnType.BIGINT;
        }
        case FloatingPoint:
            return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.SINGLE
                    ? ColumnType.REAL : ColumnType.DOUBLE;
        case Decimal: {
            ArrowType.Decimal d = (ArrowType.Decimal) type;
            // 旧格式的 Decimal 无从区分 DECIMAL/NUMERIC,归 DECIMAL;precision/scale 从类型取。
            return ColumnType.DECIMAL;
        }
        case Utf8:
            return ColumnType.VARCHAR;
        case Bool:
            return ColumnType.BOOLEAN;
        case Date:
            return ColumnType.DATE;
        case Time:
            return ColumnType.TIME;
        case Timestamp:
            return ColumnType.TIMESTAMP;
        case Binary:
            return ColumnType.VARBINARY;
        default:
            throw new IllegalArgumentException(
                    "unsupported arrow type in file: " + type);
    }
}
```

> 需要新增 import:`org.apache.arrow.vector.types.FloatingPointPrecision`。`Field.getMetadata()` 可能返回空 Map 而非 null —— 上面用 `meta != null` 兼容两者。旧的 `toColumnType(ArrowType)` 方法整体删除,被 `inferFromArrowType` 取代。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StorageManagerTest`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java minidb-server/src/test/java/com/minidb/server/storage/StorageManagerTest.java
git commit -m "feat: StorageManager 重载按 Field 元数据与 Arrow 类型恢复新类型及 DECIMAL 精度"
```

---

## Task 4: DDL 解析 DECIMAL precision/scale

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java:134-148`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/QueryExecutorTest.java`(或新建 `DataTypeDdlTest.java`)

**Interfaces:**
- Consumes: Task 2 的 `ArrowTypes.fromSqlTypeName`、Task 1 的 `ColumnMeta(name,type,precision,scale)`。
- Produces: `handleCreate` 构造带 precision/scale 的 `ColumnMeta`。

- [ ] **Step 1: 先写失败测试**

新建 `minidb-server/src/test/java/com/minidb/server/exec/DataTypeDdlTest.java`(用与 QueryExecutorTest 相同的构造模式:4 参 `QueryExecutor(catalog, storage, allocator, stats)`;若不确定构造参数,先读 `QueryExecutorTest` 的 setUp 照抄):

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataTypeDdlTest {

    @Test
    void createTableParsesDecimalPrecisionScale(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator();
             StatsManager stats = new StatsManager(allocator)) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            storage.setStatsManager(stats);
            QueryExecutor exec = new QueryExecutor(catalog, storage, allocator, stats);
            exec.execute("CREATE TABLE t (price DECIMAL(10,2), qty NUMERIC(8), s SMALLINT)");
            List<ColumnMeta> cols = catalog.getTable("t").columns();
            assertEquals(ColumnType.DECIMAL, cols.get(0).type());
            assertEquals(10, cols.get(0).precision());
            assertEquals(2, cols.get(0).scale());
            assertEquals(ColumnType.NUMERIC, cols.get(1).type());
            assertEquals(8, cols.get(1).precision());
            assertEquals(0, cols.get(1).scale());
            assertEquals(ColumnType.SMALLINT, cols.get(2).type());
        }
    }
}
```

> 若 `StatsManager` 构造签名不是 `(BufferAllocator)`,照 `QueryExecutorTest`/`MiniDbServer` 里的实际用法调整(必要时读这两个文件确认)。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=DataTypeDdlTest`
Expected: FAIL(精度/scale 未解析,`DECIMAL(10,2)` 的 precision 恒 -1;或 NUMERIC 名字解析失败)。

- [ ] **Step 3: 改 `QueryExecutor.handleCreate`**

把 `handleCreate` 的列解析循环(第 139-144 行)替换为:

```java
for (SqlNode columnNode : create.columnList) {
    SqlColumnDeclaration column = (SqlColumnDeclaration) columnNode;
    String typeName = column.dataType.getTypeName().getSimple();
    ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
    int precision = column.dataType.getPrecision();
    int scale = column.dataType.getScale();
    columns.add(new ColumnMeta(column.name.getSimple(), type, precision, scale));
}
```

> `SqlDataTypeSpec.getPrecision()`/`getScale()` 未指定时返回 -1;`DECIMAL(10,2)` 返回 10/2,`DECIMAL(8)` 返回 8/-1(scale 未设,`ArrowTypes.arrowTypeOf` 会落到默认 0)。**验证步骤**:若 `NUMERIC` 经 Calcite `getSimple()` 返回 `"DECIMAL"` 而非 `"NUMERIC"`,则 NUMERIC 会被记为 DECIMAL——记录此现象并在本任务的 commit message 说明;若要严格区分,需读 `column.dataType.getTypeName()` 的 `SqlTypeName` 枚举,但 Calcite 1.42 可能把 NUMERIC 折叠成 DECIMAL 枚举值,此时无法区分,接受「NUMERIC 记为 DECIMAL」并在 README 注明。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=DataTypeDdlTest`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java minidb-server/src/test/java/com/minidb/server/exec/DataTypeDdlTest.java
git commit -m "feat: CREATE TABLE 解析 DECIMAL/NUMERIC 的 precision/scale"
```

---

## Task 5: 表达式原生向量(类型映射 + 字面量 + CAST + 内核)

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/RexInterpreter.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/functions/Function.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/functions/ScalarKernels.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/functions/Kernels.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/functions/FunctionFrameworkTest.java`(或新建 `NewTypeExpressionTest.java`)

**Interfaces:**
- Consumes: Task 2 的 `ArrowTypes.field(RelDataType, name)`(现在对 DECIMAL 产 Decimal128)。
- Produces: `RexInterpreter` 能求值 SMALLINT/REAL/FLOAT/DECIMAL/TIME/BINARY 字面量、CAST;`Function`/`BuiltInFunctions` 对 SmallInt/Float4/Decimal 有算术+比较内核,对 Time/VarBinary 有比较内核。

> **这是关键任务,必须原子提交**:改 `Function.outputVectorClass`(DECIMAL→DecimalVector 等)与 `RexInterpreter.literalVector`(DECIMAL 字面量→DecimalVector)的同时,**必须**同步注册对应的算术/比较/数学内核,否则 `SELECT 1.5 + 1.5`、`ROUND(2.5)` 这类既有 DECIMAL 表达式会在 `Function.resolve` 抛「no overload」。

- [ ] **Step 1: 先写失败测试**

新建 `minidb-server/src/test/java/com/minidb/server/exec/functions/NewTypeExpressionTest.java`,覆盖字面量类型、DECIMAL 精确算术、比较、CAST(用与 `RexInterpreterTest` 相同的构造模式,先读它照抄 harness):

```java
package com.minidb.server.exec.functions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.minidb.server.exec.RexInterpreter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NewTypeExpressionTest {

    static BufferAllocator allocator;
    static RexInterpreter interp;

    @BeforeAll
    static void setUp() {
        allocator = new RootAllocator();
        interp = new RexInterpreter(allocator);
    }

    @AfterAll
    static void tearDown() {
        allocator.close();
    }

    @Test
    void decimalLiteralProducesDecimalVector() {
        // 直接用 RexInterpreter 无法轻易造 RexLiteral,改用端到端 SQL 由 QueryExecutor 驱动;
        // 本测试类改成走 QueryExecutor.execute(sql) 断言结果向量 MinorType。
    }
}
```

> **重要提示**:`RexInterpreter` 的 `eval` 需要 `RexLiteral`/`RexNode`,手工构造繁琐。**改用端到端测试**——通过 `QueryExecutor` 执行 SQL,断言返回 `QueryResult.Rows` 的 `VectorSchemaRoot` 里各列向量的 `MinorType` 与取值。把上面占位测试替换为真正端到端测试(构造 QueryExecutor 同 Task 4),例如:

```java
@Test
void decimalArithmeticIsExact() {
    QueryResult.Rows r = exec.execute("SELECT 0.1 + 0.2 AS x");
    // 结果列是 DecimalVector,BigDecimal 精确 0.3
    ValueVector v = r.rows().getVector("x");
    assertTrue(v instanceof org.apache.arrow.vector.DecimalVector);
    assertEquals(0, new java.math.BigDecimal("0.3")
            .compareTo((java.math.BigDecimal) v.getObject(0)));
    r.rows().close();
}
```

**实施者务必先读 `QueryExecutorTest` 与 `RexInterpreterTest`,照它们的 harness 写真实测试**,上面的代码只作方向示意,不要原样照抄未定义的 `exec`。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=NewTypeExpressionTest`
Expected: FAIL(DECIMAL 表达式走旧 Float8 路径,断言 DecimalVector 不成立)。

- [ ] **Step 3: 改 `Function.outputVectorClass`(原生向量类)**

把 `Function.java` 的 `outputVectorClass` switch 改为:

```java
private static Class<? extends FieldVector> outputVectorClass(RelDataType type) {
    return switch (type.getSqlTypeName()) {
        case SMALLINT -> SmallIntVector.class;
        case INTEGER -> IntVector.class;
        case BIGINT -> BigIntVector.class;
        case REAL, FLOAT -> Float4Vector.class;
        case DOUBLE -> Float8Vector.class;
        case DECIMAL -> DecimalVector.class;
        case VARCHAR, CHAR -> VarCharVector.class;
        case BOOLEAN -> BitVector.class;
        case DATE -> DateDayVector.class;
        case TIME -> TimeMilliVector.class;
        case TIMESTAMP -> TimeStampMilliVector.class;
        case BINARY, VARBINARY -> VarBinaryVector.class;
        default -> throw new IllegalArgumentException(
                "unsupported result type: " + type.getSqlTypeName());
    };
}
```

新增 import:`SmallIntVector`、`Float4Vector`、`DecimalVector`、`TimeMilliVector`、`VarBinaryVector`。

- [ ] **Step 4: 改 `RexInterpreter.literalVector`/`nullLiteral`/`evalCast`/`newVector`/`as*`**

在 `RexInterpreter.java` 中:

(a) `literalVector` 的 switch:`SMALLINT`(原与 INTEGER 同组)单独成 `SmallIntVector` 分支,`REAL`/`FLOAT` 成 `Float4Vector`,去掉 `DECIMAL`/`FLOAT`/`REAL` 与 DOUBLE 同组、改为 `DECIMAL`→`DecimalVector`,`CHAR`/`VARCHAR`→`VarCharVector`(CHAR 已在内),新增 `TIME`→`TimeMilliVector`、`BINARY`/`VARBINARY`→`VarBinaryVector`。DECIMAL 字面量取值 `literal.getValueAs(BigDecimal.class)`。

(b) `nullLiteral` 同理:按 `SqlTypeName` 建对应向量(SMALLINT→SmallIntVector、REAL/FLOAT→Float4Vector、DECIMAL→DecimalVector、TIME→TimeMilliVector、BINARY/VARBINARY→VarBinaryVector)。

(c) `newVector`(CASE 结果)同理加新类型。

(d) `evalCast` 的 target switch:新增 `SMALLINT`(short)、`REAL`/`FLOAT`(float)、`DECIMAL`(BigDecimal)、`TIME`(int ms)、`BINARY`/`VARBINARY`(byte[])、`CHAR`(已并入 VARCHAR)。CAST 到 DECIMAL 用 `new BigDecimal(asString(v,i))` 或数值直转;CAST 到 TIME 从字符串/长整数读 ms;CAST 到 BINARY 从字符串取 UTF-8 字节。

(e) `asLong`/`asDouble`/`asString`/`asBoolean` 加 SmallIntVector/Float4Vector/DecimalVector 的读取分支(DecimalVector 用 `getObject(i)` 转 BigDecimal,再 `.longValue()/.doubleValue()/.toPlainString()`)。

> **DECIMAL 的 CAST 与取值**:`DecimalVector.getObject(int)` 返回 `BigDecimal`。CAST 到 DECIMAL 时,输出向量要用 `ArrowTypes.field(castType,"cast").createVector(allocator)` 分配(带 precision/scale),不能 `new DecimalVector(...)`(缺 precision/scale 会抛)。同理 `literalVector`/`newVector` 里建 DecimalVector 也要带 precision/scale——**用 `ArrowTypes.field(literal.getType(), "lit").createVector(allocator)` 替代手建 DecimalVector**;其余向量类型仍可 `new XxxVector(...)`。

- [ ] **Step 5: 新增内核(ScalarKernels + Kernels)**

在 `ScalarKernels.java` 追加接口(short/float/BigDecimal/byte[] 的二元与比较函数式接口):

```java
public interface ShortBinary { short apply(short a, short b); }
public interface ShortCompare { int compare(short a, short b); }
public interface FloatBinary { float apply(float a, float b); }
public interface FloatCompare { int compare(float a, float b); }
public interface DecimalBinary { BigDecimal apply(BigDecimal a, BigDecimal b); }
public interface DecimalCompare { int compare(BigDecimal a, BigDecimal b); }
public interface BytesCompare { int compare(byte[] a, byte[] b); }
```

在 `Kernels.java` 追加 fill 助手(STRICT-null 循环,只写值不 setValueCount):

```java
public static void fillBinaryShort(SmallIntVector l, SmallIntVector r, SmallIntVector out, ScalarKernels.ShortBinary op) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, op.apply(l.get(i), r.get(i)));
    }
}
public static void fillCompareShort(SmallIntVector l, SmallIntVector r, BitVector out, ScalarKernels.ShortCompare cmp, SqlKind kind) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, compareToBool(cmp.compare(l.get(i), r.get(i)), kind) ? 1 : 0);
    }
}
public static void fillBinaryFloat(Float4Vector l, Float4Vector r, Float4Vector out, ScalarKernels.FloatBinary op) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, op.apply(l.get(i), r.get(i)));
    }
}
public static void fillCompareFloat(Float4Vector l, Float4Vector r, BitVector out, ScalarKernels.FloatCompare cmp, SqlKind kind) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, compareToBool(cmp.compare(l.get(i), r.get(i)), kind) ? 1 : 0);
    }
}
public static void fillBinaryDecimal(DecimalVector l, DecimalVector r, DecimalVector out, ScalarKernels.DecimalBinary op) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, op.apply(l.getObject(i), r.getObject(i)));
    }
}
public static void fillCompareDecimal(DecimalVector l, DecimalVector r, BitVector out, ScalarKernels.DecimalCompare cmp, SqlKind kind) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, compareToBool(cmp.compare(l.getObject(i), r.getObject(i)), kind) ? 1 : 0);
    }
}
public static void fillCompareTime(TimeMilliVector l, TimeMilliVector r, BitVector out, ScalarKernels.IntCompare cmp, SqlKind kind) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
    }
}
public static void fillCompareBytes(VarBinaryVector l, VarBinaryVector r, BitVector out, ScalarKernels.BytesCompare cmp, SqlKind kind) {
    for (int i = 0; i < l.getValueCount(); i++) {
        if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
        out.setSafe(i, compareToBool(cmp.compare(l.get(i), r.get(i)), kind) ? 1 : 0);
    }
}
```

需要把 `compareToBool` 从 `private` 改 `package-private`(或直接在新增方法里内联比较逻辑),因为 `fillCompareShort` 等在同类内,`private` 已可访问——保持 `private` 即可。新增 import:`SmallIntVector`/`Float4Vector`/`DecimalVector`/`TimeMilliVector`/`VarBinaryVector`/`java.math.BigDecimal`。

- [ ] **Step 6: 注册内核(BuiltInFunctions)**

在 `BuiltInFunctions.arithmeticFunction` 追加 SmallInt/Float4/Decimal 三个同型重载;在 `comparisonFunction` 追加 SmallInt/Float4/Decimal/Time/VarBinary 五个同型重载。示例(SmallInt 算术,`+ - * /` 用 short 域,`/` 除零抛 ArithmeticException):

```java
private static Function arithmeticFunction(SqlOperator op) {
    ScalarKernels.ShortBinary shortOp = shortKernel(op);
    ScalarKernels.FloatBinary floatOp = floatKernel(op);
    ScalarKernels.DecimalBinary decimalOp = decimalKernel(op);
    // ... 保留既有 Int/Long/Double/跨型重载,再追加:
    return new Function(op.getName(), List.of(
            /* 既有 5 个重载不动 */,
            new Overload(List.of(SmallIntVector.class, SmallIntVector.class), SmallIntVector.class,
                    (args, out) -> Kernels.fillBinaryShort(
                            (SmallIntVector) args.get(0), (SmallIntVector) args.get(1),
                            (SmallIntVector) out, shortOp)),
            new Overload(List.of(Float4Vector.class, Float4Vector.class), Float4Vector.class,
                    (args, out) -> Kernels.fillBinaryFloat(
                            (Float4Vector) args.get(0), (Float4Vector) args.get(1),
                            (Float4Vector) out, floatOp)),
            new Overload(List.of(DecimalVector.class, DecimalVector.class), DecimalVector.class,
                    (args, out) -> Kernels.fillBinaryDecimal(
                            (DecimalVector) args.get(0), (DecimalVector) args.get(1),
                            (DecimalVector) out, decimalOp))));
}
```

`shortKernel`/`floatKernel`/`decimalKernel` 与既有 `intKernel`/`longKernel`/`doubleKernel` 同构,分别返回 `ShortBinary`/`FloatBinary`/`DecimalBinary`。**DECIMAL 除法**:`(a, b) -> a.divide(b, Math.max(a.scale(), b.scale()) + 6, java.math.RoundingMode.HALF_UP)`;除零时 `b.signum()==0` 抛 `ArithmeticException`。

比较同理:SmallInt 用 `Short::compare`,Float4 用 `Float::compare`,Decimal 用 `BigDecimal::compareTo`,Time 用 `Integer::compare`,VarBinary 用 `(a,b) -> java.util.Arrays.compareUnsigned(a,b)`。

> **注意 `Function.resolve` 的输出类型匹配**:`outputVectorClass` 把 DECIMAL 映成 `DecimalVector.class`,故 Decimal 算术重载的 `outputType` 必须也是 `DecimalVector.class`(上面已如此);SmallInt→`SmallIntVector.class`、Float4→`Float4Vector.class`、Time→比较输出 `BitVector.class`、VarBinary→比较输出 `BitVector.class`。

- [ ] **Step 7: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=NewTypeExpressionTest`
Expected: PASS。

- [ ] **Step 8: 跑全量测试确认无回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: PASS。特别关注既有 DECIMAL/浮点相关测试(ROUND/ABS/FLOOR/CEIL/CAST)是否因 DECIMAL→DecimalVector 而需要补 Decimal 数学函数重载——若 `ROUND(2.5)` 等失败,给 `ROUND`/`FLOOR`/`CEIL`/`ABS` 补 DecimalVector 重载(BigDecimal 域实现),直到全绿。

- [ ] **Step 9: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/RexInterpreter.java minidb-server/src/main/java/com/minidb/server/exec/functions/Function.java minidb-server/src/main/java/com/minidb/server/exec/functions/ScalarKernels.java minidb-server/src/main/java/com/minidb/server/exec/functions/Kernels.java minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java minidb-server/src/test/java/com/minidb/server/exec/functions/NewTypeExpressionTest.java
git commit -m "feat: 表达式层走原生向量,补齐 SmallInt/Float4/Decimal/Time/VarBinary 内核"
```

---

## Task 6: 行转换读写(INSERT/SELECT/算子)

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/RowVectors.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/RowCopier.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbValues.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbJoin.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbUnion.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/WindowFunctions.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbSetOp.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbAggregate.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbSort.java`
- Test: `minidb-server/src/test/java/com/minidb/server/plan/physical/RowVectorsTest.java`(新建)+ 现有算子测试

**Interfaces:**
- Consumes: Task 5 的表达式原生向量(INSERT 字面量已能产 SmallInt/Float4/Decimal/Time/VarBinary 向量)。
- Produces: `RowVectors.readObject/writeObject` 与各算子私有副本能读写 Int16/Float4/Decimal(BigDecimal)/Time(int)/VarBinary(byte[])。

- [ ] **Step 1: 先写失败测试(RowVectorsTest)**

新建 `RowVectorsTest.java`(用 `RootAllocator` + 一个含新类型列的 `VectorSchemaRoot`,round-trip `readObject`→`buildRoot`→`readObject` 断言值一致):

```java
package com.minidb.server.plan.physical;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import java.math.BigDecimal;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RowVectorsTest {
    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() { allocator = new RootAllocator(); }
    @AfterAll
    static void tearDown() { allocator.close(); }

    @Test
    void readWriteRoundTripsNewTypes() {
        List<FieldVector> vecs = List.of(
                ArrowTypes.field(new ColumnMeta("s", ColumnType.SMALLINT)).createVector(allocator),
                ArrowTypes.field(new ColumnMeta("p", ColumnType.DECIMAL, 10, 2)).createVector(allocator),
                ArrowTypes.field(new ColumnMeta("t", ColumnType.TIME)).createVector(allocator),
                ArrowTypes.field(new ColumnMeta("b", ColumnType.VARBINARY)).createVector(allocator));
        for (FieldVector v : vecs) { v.setInitialCapacity(1); v.allocateNew(); }
        ((SmallIntVector) vecs.get(0)).setSafe(0, (short) 42);
        ((DecimalVector) vecs.get(1)).setSafe(0, new BigDecimal("1.23"));
        ((TimeMilliVector) vecs.get(2)).setSafe(0, 45296000); // 12:34:56
        ((VarBinaryVector) vecs.get(3)).setSafe(0, new byte[]{1, 2, 3});
        for (FieldVector v : vecs) { v.setValueCount(1); }
        VectorSchemaRoot root = VectorSchemaRoot.of(vecs.toArray(new FieldVector[0]));
        root.setRowCount(1);
        try {
            assertEquals((short) 42, RowVectors.readObject(root.getVector(0), 0));
            assertEquals(new BigDecimal("1.23"), RowVectors.readObject(root.getVector(1), 0));
            assertEquals(45296000, RowVectors.readObject(root.getVector(2), 0));
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) RowVectors.readObject(root.getVector(3), 0));
        } finally {
            root.close();
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=RowVectorsTest`
Expected: FAIL(`readObject` 对 SmallInt/Decimal/Time/VarBinary 抛 `cannot read column type`)。

- [ ] **Step 3: 改 `RowVectors.readObject`/`writeObject`**

在 `readObject` 的 `IntVector` 分支前加 `SmallIntVector` 分支,并加 Decimal/Time/VarBinary 分支;`writeObject` 同理:

```java
if (vector instanceof SmallIntVector sv) {
    return sv.get(row);
}
// ... 已有 Int/BigInt/Float8 ...
if (vector instanceof Float4Vector fv) {   // 新增
    return fv.get(row);
}
if (vector instanceof DecimalVector dv) {  // 新增
    return dv.getObject(row);
}
if (vector instanceof TimeMilliVector tv) { // 新增
    return tv.get(row);
}
if (vector instanceof VarBinaryVector bv) { // 新增
    return bv.get(row);
}
```

`writeObject` 对应加:SmallIntVector→`((Number) value).shortValue()`、Float4Vector→`((Number) value).floatValue()`、DecimalVector→`(BigDecimal) value`(或 `new BigDecimal(value.toString())`)、TimeMilliVector→`((Number) value).intValue()`、VarBinaryVector→`(byte[]) value`。新增 import:`SmallIntVector`/`Float4Vector`/`DecimalVector`/`TimeMilliVector`/`VarBinaryVector`/`java.math.BigDecimal`。

- [ ] **Step 4: 同步其余类型分发副本**

下列文件的私有 `readObject`/`writeObject`(或等价的 `instanceof` 分发链)都要加上与 Step 3 相同的新分支。逐个打开文件,定位其 `instanceof IntVector`/`Float8Vector` 链,在对应位置插入 SmallInt/Float4/Decimal/Time/VarBinary 分支:

- `MiniDbJoin.java`(readObject/writeObject)
- `MiniDbUnion.java`(readObject/writeObject)
- `WindowFunctions.java`(materialize 里的 readObject)
- `MiniDbSetOp.java`(readObject)
- `MiniDbAggregate.java`(readObject / accumulator 取列值处)
- `MiniDbSort.java`(比较器里的取键值处)
- `RowCopier.java`(`copyVector`/`writeValue` 里按 MinorType 或 instanceof 的分发)
- `MiniDbValues.java`(`setLiteral` 加 SmallIntVector/Float4Vector/DecimalVector/TimeMilliVector/VarBinaryVector 分支)

> 这些副本的 readObject 读值类型要**一致**:Int16→`short`、Float4→`float`、Decimal→`BigDecimal`、Time→`int`(ms)、VarBinary→`byte[]`。这样 join/union/aggregate 等 eager 算子物化 `Object[]` 后,`writeObject` 能按同类型写回。**注意 `MiniDbSort` 的比较器**目前可能用 `Comparable` 直接比较——`byte[]` 不可 `Comparable`,须在比较器里把 `byte[]` 用 `Arrays.compareUnsigned`、BigDecimal 用 `compareTo` 处理(若比较器走 `compareKeys`/`Comparable` 泛型,确认它能承接 BigDecimal/short/float;byte[] 必须特判)。

- [ ] **Step 5: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=RowVectorsTest`
Expected: PASS。

- [ ] **Step 6: 跑全量测试确认无回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: PASS。

- [ ] **Step 7: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/plan/physical/RowVectors.java minidb-server/src/main/java/com/minidb/server/exec/RowCopier.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbValues.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbJoin.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbUnion.java minidb-server/src/main/java/com/minidb/server/plan/physical/WindowFunctions.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbSetOp.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbAggregate.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbSort.java minidb-server/src/test/java/com/minidb/server/plan/physical/RowVectorsTest.java
git commit -m "feat: 行转换读写串通新类型向量(含 join/union/aggregate/sort 副本)"
```

---

## Task 7: 聚合累加器支持新数值类型

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbAggregate.java`
- Test: `minidb-server/src/test/java/com/minidb/server/plan/physical/MiniDbAggregateTest.java`(或现有聚合测试类)

**Interfaces:**
- Consumes: Task 6 的 readObject 能读 SmallInt/Float4/Decimal。
- Produces: `SUM/AVG/MIN/MAX/COUNT` 能作用于 SMALLINT/REAL/FLOAT/DECIMAL 列,输出按 `getRowType()`(Calcite 推导)落到正确向量。

- [ ] **Step 1: 先写失败测试**

读现有聚合测试类(如 `MiniDbAggregateTest` 或 `QueryExecutorTest` 里的聚合用例),照 harness 加一个端到端用例:建表 `t(x SMALLINT, y DECIMAL(10,2))`,插两行,`SELECT SUM(x), AVG(y) FROM t`,断言结果向量 MinorType 与取值(SUM(SMALLINT) 结果类型以 Calcite 推导为准,可能是 INTEGER/BIGINT;AVG(DECIMAL) 通常是 DECIMAL)。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=<聚合测试类>`
Expected: FAIL(累加器读 SmallInt/Decimal 列时 `readObject` 或取列值处类型不匹配)。

- [ ] **Step 3: 改 `MiniDbAggregate` 累加器**

定位累加器(`SUM` 的 long/double 分支、`MIN/MAX` 的 `Comparable` 分支、`AVG` 的 sum+count)。加:SMALLINT 累加(long 域或 short 域、结果按输出列类型写)、REAL/FLOAT 累加(double 域)、DECIMAL 累加(BigDecimal 域)。关键是**输入侧**按 SmallInt/Float4/Decimal 读值,**输出侧**按 `getRowType()` 的输出向量类型写(输出向量由 `ArrowTypes.field(f).createVector` 分配,若输出是 DecimalVector 则写 BigDecimal)。

> 聚合累加器现有实现可能直接 `instanceof IntVector`/`Float8Vector` 取列值——把新向量类型的分支加进去即可。`MIN/MAX` 的 `Comparable` 路径对 BigDecimal/short/float 已兼容(它们都 Comparable),仅需确保读值类型正确(short 装箱 Short、float 装箱 Float、Decimal 取 BigDecimal)。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=<聚合测试类>` → PASS;再 `./mvnw.cmd test -pl minidb-server` → PASS。

- [ ] **Step 5: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbAggregate.java <聚合测试类>
git commit -m "feat: 聚合累加器支持 SMALLINT/REAL/FLOAT/DECIMAL"
```

---

## Task 8: 服务端元数据(getColumns)报新类型

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java`

**Interfaces:**
- Consumes: Task 1/2 的 `ColumnType`。
- Produces: `MetadataExecutor.columns(...)` 的 `DATA_TYPE`/`TYPE_NAME`/`NUM_PREC_RADIX`/`COLUMN_SIZE`/`DECIMAL_DIGITS` 对新类型正确。

- [ ] **Step 1: 先写失败测试**

在 `MetadataExecutorTest.java` 加用例:建 `t(s SMALLINT, p DECIMAL(10,2), t TIME, b VARBINARY)` 到 catalog,`columns` 结果断言 `DATA_TYPE`=`Types.SMALLINT`/`Types.DECIMAL`/`Types.TIME`/`Types.VARBINARY`,`TYPE_NAME`=`"SMALLINT"`/`"DECIMAL"`/`"TIME"`/`"VARBINARY"`,DECIMAL 的 `COLUMN_SIZE=10`/`DECIMAL_DIGITS=2`。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MetadataExecutorTest`
Expected: FAIL(switch 无新类型,编译错误)。

- [ ] **Step 3: 改 `MetadataExecutor.sqlType`/`isIntegerType` + 精度列**

`sqlType(ColumnType)` 加新分支:SMALLINT→`Types.SMALLINT`、REAL/FLOAT→`Types.REAL`、DECIMAL/NUMERIC→`Types.DECIMAL`、CHAR→`Types.CHAR`、NCHAR→`Types.NCHAR`、NVARCHAR→`Types.NVARCHAR`、TIME→`Types.TIME`、BINARY→`Types.BINARY`、VARBINARY→`Types.VARBINARY`。`isIntegerType` 把 SMALLINT 计入。`buildColumnsRoot` 里 DECIMAL/NUMERIC 行填 `colSize=precision`、`decDigits=scale`(从 `r.column().precision()/scale()` 取,未设则 0)。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MetadataExecutorTest`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java
git commit -m "feat: 服务端 getColumns 报新类型及 DECIMAL 精度"
```

---

## Task 9: JDBC 客户端读新类型 + 类型名保真

**Files:**
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSetMetaData.java`
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSet.java`
- Test: `minidb-jdbc/src/test/java/com/minidb/jdbc/MiniDbResultSetMetaDataTest.java`(新建或加在现有 JDBC 测试)

**Interfaces:**
- Consumes: 服务端发来的 Arrow 批次,Field 带 `"minidb.type"` 元数据。
- Produces: `getColumnTypeName` 优先读元数据(保真 CHAR/NCHAR/NVARCHAR、DECIMAL/NUMERIC、FLOAT/REAL、BINARY/VARBINARY),回退 MinorType;`getObject`/`getShort`/`getFloat`/`getBigDecimal`/`getTime`/`getBytes` 支持新 MinorType。

- [ ] **Step 1: 先写失败测试**

新建 `MiniDbResultSetMetaDataTest.java`:用 `RootAllocator` 构造含 `SmallIntVector`/`Float4Vector`/`DecimalVector`/`TimeMilliVector`/`VarBinaryVector` 的 `VectorSchemaRoot`(每列 Field 挂 `"minidb.type"` 元数据),断言 `getColumnTypeName` 返回 `"SMALLINT"`/`"REAL"`/`"DECIMAL"`/`"TIME"`/`"VARBINARY"`。若 `minidb-jdbc` 测试运行需要 `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`(见 README),在 surefire 配置里加该 argLine 或记录到测试注释。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-jdbc -Dtest=MiniDbResultSetMetaDataTest`
Expected: FAIL(`getColumnTypeName` 对 SMALLINT/FLOAT4/DECIMAL/TIMEMILLI/VARBINARY 走 default 分支返回 MinorType 名)。

- [ ] **Step 3: 改 `MiniDbResultSetMetaData.getColumnTypeName`**

优先读 Field 元数据,回退 MinorType:

```java
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
```

`getColumnType(int)` 同样按 `ArrowType.ArrowTypeID`(而非 MinorType)分支:Int16→`Types.SMALLINT`、FloatingPoint(SINGLE)→`Types.REAL`、Decimal→`Types.DECIMAL`、Utf8→`Types.VARCHAR`、Time→`Types.TIME`、Binary→`Types.VARBINARY`、其余同旧。

- [ ] **Step 4: 改 `MiniDbResultSet`**

实现 `getShort`/`getFloat`/`getBigDecimal`/`getTime`/`getBytes`(从 `throw new SQLFeatureNotSupportedException()` 改为真实实现),`getObject` 加新 MinorType 分支:

```java
@Override
public short getShort(int columnIndex) throws SQLException {
    ValueVector v = vector(columnIndex);
    if (isNull(v)) return 0;
    if (v instanceof SmallIntVector sv) return sv.get(cursor);
    if (v instanceof IntVector iv) return (short) iv.get(cursor);
    throw new SQLException("not a smallint column");
}
@Override
public float getFloat(int columnIndex) throws SQLException {
    ValueVector v = vector(columnIndex);
    if (isNull(v)) return 0f;
    if (v instanceof Float4Vector fv) return fv.get(cursor);
    if (v instanceof Float8Vector fv) return (float) fv.get(cursor);
    throw new SQLException("not a float column");
}
@Override
public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    ValueVector v = vector(columnIndex);
    if (isNull(v)) return null;
    if (v instanceof DecimalVector dv) return dv.getObject(cursor);
    if (v instanceof Float8Vector fv) return BigDecimal.valueOf(fv.get(cursor));
    throw new SQLException("not a decimal column");
}
@Override
public Time getTime(int columnIndex) throws SQLException {
    ValueVector v = vector(columnIndex);
    if (isNull(v)) return null;
    if (v instanceof TimeMilliVector tv) return new Time(tv.get(cursor));
    throw new SQLException("not a time column");
}
@Override
public byte[] getBytes(int columnIndex) throws SQLException {
    ValueVector v = vector(columnIndex);
    if (isNull(v)) return null;
    if (v instanceof VarBinaryVector bv) return bv.get(cursor);
    throw new SQLException("not a binary column");
}
```

`getObject` 的 switch 加:`SMALLINT→getShort`、`FLOAT4→getFloat`、`DECIMAL→getBigDecimal`、`TIMEMILLI→getTime`、`VARBINARY→getBytes`。补对应的 `getXxx(String)` 委托方法(已存在的模式)。新增 import:`SmallIntVector`/`Float4Vector`/`DecimalVector`/`TimeMilliVector`/`VarBinaryVector`。

- [ ] **Step 5: 跑测试确认通过 + 全量回归**

Run: `./mvnw.cmd test -pl minidb-jdbc`(若 NoClassDefFoundError 属环境问题,见 CLAUDE.md 坑 12,忽略之并跑 `./mvnw.cmd test -pl minidb-server`)
Expected: PASS(或仅环境问题)。

- [ ] **Step 6: Commit**

```bash
git add minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSetMetaData.java minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSet.java minidb-jdbc/src/test/java/com/minidb/jdbc/MiniDbResultSetMetaDataTest.java
git commit -m "feat: JDBC 客户端读新类型 + getColumnTypeName 按元数据保真"
```

---

## Task 10: README + 端到端集成测试

**Files:**
- Modify: `README.md`(「支持的列类型」一节)
- Test: `minidb-server/src/test/java/com/minidb/server/exec/DataTypeIntegrationTest.java`(新建)

**Interfaces:**
- Consumes: Task 1-9 全部。

- [ ] **Step 1: 写端到端集成测试**

新建 `DataTypeIntegrationTest.java`,用 `QueryExecutor`(照 Task 4 harness)覆盖每个新类型:CREATE → INSERT → SELECT round-trip 值一致、比较过滤、DECIMAL 精确算术、重启(StorageManager reload)类型与 precision/scale 一致、聚合、CAST。断言用 `QueryResult.Rows` 的向量取值。

- [ ] **Step 2: 跑全量测试**

Run: `./mvnw.cmd test`
Expected: PASS(全部模块)。

- [ ] **Step 3: 更新 README**

把 `README.md` 第 61-63 行的「支持的列类型」改为:

```
INTEGER、BIGINT、SMALLINT、REAL、FLOAT、DOUBLE、DECIMAL、NUMERIC、VARCHAR、CHAR、NCHAR、NVARCHAR、BOOLEAN、DATE、TIME、TIMESTAMP、BINARY、VARBINARY。

- DECIMAL/NUMERIC 为 128 位定点(BigDecimal),precision/scale 支持(默认 10/0)。
- CHAR/NCHAR/NVARCHAR 变长存储、不做空格填充(简化)。
```

- [ ] **Step 4: Commit**

```bash
git add README.md minidb-server/src/test/java/com/minidb/server/exec/DataTypeIntegrationTest.java
git commit -m "docs: README 更新支持类型列表;test: 数据类型端到端集成测试"
```

---

## 自检清单(写完后自查,发现即修)

1. **Spec 覆盖**:逐一核对 spec 的「类型总表」12 种类型是否每个都有对应任务触点(SMALLINT/REAL/FLOAT/DOUBLE PRECISION/DECIMAL/NUMERIC/CHAR/NCHAR/NVARCHAR/TIME/BINARY/VARBINARY)。`DOUBLE PRECISION` 只在 `fromSqlTypeName` 里映射(无新枚举值),已覆盖。
2. **占位符扫描**:全文无 "TBD/TODO/implement later" 之类;所有代码步骤给了实际代码。Task 5 Step 1 的占位测试已明确指示「先读 harness 照抄」并给了方向,不算占位。
3. **类型一致性**:`toCalciteType` 签名改为 `(ColumnMeta, factory)`,Task 2 里唯一调用点 `MiniDbCalciteTable` 已同步改;`arrowTypeOf(ColumnType)` 改为带 precision/scale 的三参私有方法,`field(ColumnMeta)`/`arrowType` 已同步;`inferFromArrowType` 取代旧 `toColumnType`,Task 3 已删除旧方法引用。新增枚举值名称全程一致(`SMALLINT/REAL/FLOAT/CHAR/NCHAR/NVARCHAR/DECIMAL/NUMERIC/TIME/BINARY/VARBINARY`)。
4. **执行顺序依赖**:Task 5(表达式原生)必须在 Task 6(行转换)之前——Task 5 产出的字面量向量类型被 Task 6 读写;Task 3(DDL)在 Task 2(ArrowTypes)之后——依赖 `fromSqlTypeName`;顺序已如此排列。

## 执行交接

计划完成后,提供两种执行方式(见 writing-plans 的执行交接):subagent-driven(推荐)或 inline。
