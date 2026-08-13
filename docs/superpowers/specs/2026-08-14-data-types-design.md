# 2026-08-14 — 补全 SQL 标准标量数据类型

## 目标

把 MiniDB 支持的列类型从 7 种(`INTEGER/BIGINT/DOUBLE/VARCHAR/BOOLEAN/DATE/TIMESTAMP`)补全到 SQL 标准(ISO/IEC 9075)的实用标量集合,采用**全原生保真**方案——每种类型映射到自己的 Arrow 向量,类型名端到端(DDL → 存储 → 持久化 → JDBC)保真。

新增 12 个 `ColumnType` 枚举值 + 1 个别名(见下表)。不含大对象(CLOB/BLOB)、时区类型(TIME/TIMESTAMP WITH TIME ZONE)、INTERVAL、集合类型(ARRAY/MULTISET/ROW)、JSON——这些是独立子系统,超出玩具库范围(见「不做」)。

## 现状

- `ColumnType` 是裸枚举 7 值;`ColumnMeta` 是 `record(name, type)`,无 precision/scale。
- `ArrowTypes` 做四向映射(`fromSqlTypeName` / `toSqlTypeName` / `arrowTypeOf(ColumnType)` / `toCalciteType`)。**表达式层早已内置归一化**:`RexInterpreter.literalVector/nullLiteral` 把 `SMALLINT`/`TINYINT`→IntVector、`FLOAT`/`REAL`/`DECIMAL`→Float8、`CHAR`→VarChar;`Function.outputVectorClass` 与 `MiniDbValues` 同样。唯一不认这些类型的是 DDL 入口 `fromSqlTypeName` 与 `ColumnType` 枚举。
- `StorageManager.toColumnType` 从 Arrow 类型**反推** ColumnType(Int32→INTEGER、Float8→DOUBLE、Utf8→VARCHAR),重启后声明类型坍缩成标准类型。
- 类型分发链有 6+ 处副本:`RowVectors` + `MiniDbJoin`/`MiniDbUnion`/`WindowFunctions`/`MiniDbSetOp`/`MiniDbAggregate`/`MiniDbSort` 私有副本 + `RowCopier` + `MiniDbValues` + JDBC 客户端(`MiniDbResultSetMetaData`/`MiniDbResultSet`)。

## 实现

### 类型总表

| SQL 类型 | ColumnType | Arrow 向量 | Arrow 类型 | JDBC Types |
|---|---|---|---|---|
| SMALLINT | SMALLINT | SmallIntVector | Int(16, true) | SMALLINT |
| REAL | REAL | Float4Vector | FloatingPoint(SINGLE) | REAL |
| FLOAT | FLOAT | Float4Vector | FloatingPoint(SINGLE) | FLOAT |
| DOUBLE PRECISION | DOUBLE(别名) | Float8Vector | Double | DOUBLE |
| DECIMAL(p,s) | DECIMAL | DecimalVector | Decimal(p,s,128) | DECIMAL |
| NUMERIC(p,s) | NUMERIC | DecimalVector | Decimal(p,s,128) | NUMERIC |
| CHAR | CHAR | VarCharVector | Utf8 | CHAR |
| NCHAR | NCHAR | VarCharVector | Utf8 | NCHAR |
| NVARCHAR | NVARCHAR | VarCharVector | Utf8 | NVARCHAR |
| TIME | TIME | TimeMilliVector | Time(MILLISECOND,32) | TIME |
| BINARY | BINARY | VarBinaryVector | Binary | BINARY |
| VARBINARY | VARBINARY | VarBinaryVector | Binary | VARBINARY |

- `DOUBLE PRECISION` 与 `DOUBLE` 是同一语义类型,别名到 `ColumnType.DOUBLE`(不新增枚举值)。
- `FLOAT`/`REAL` 物理同为 Float4,但保留两个枚举值 + 元数据区分类型名(见下)。
- `NUMERIC` 与 `DECIMAL` 同为 Decimal128,两个枚举值。

### 类型名端到端保真(核心机制)

多组类型共享同一 Arrow 物理类型(MinorType 分不开):CHAR/NCHAR/NVARCHAR/VARCHAR 都是 Utf8,DECIMAL/NUMERIC 都是 Decimal,FLOAT/REAL 都是 Float4,BINARY/VARBINARY 都是 Binary。为保真,在**每个 Arrow Field 上挂自定义元数据 `"minidb.type" → ColumnType 名`**,复用现有 `"schema"` 元数据机制:

- `ArrowTable` 构造 Arrow `Schema` 时,每个 field 带 `"minidb.type"` 元数据(IPC 会保留 field 级元数据)。
- JDBC 客户端 `MiniDbResultSetMetaData.getColumnTypeName` 优先读 `field.getMetadata().get("minidb.type")`,回退到 MinorType。
- `StorageManager.toColumnType` 优先读元数据,旧文件无元数据时回退到 Arrow 类型推断。

DECIMAL/NUMERIC 的 precision/scale 不另存元数据,直接存 Arrow `Decimal` 类型本身(ArrowType.Decimal 自带 precision/scale)。

### 元数据模型

`ColumnMeta` 从 `record(name, type)` 扩为:

```java
public record ColumnMeta(String name, ColumnType type, int precision, int scale) {
    public ColumnMeta(String name, ColumnType type) { this(name, type, -1, -1); }
}
```

- precision/scale 仅 DECIMAL/NUMERIC 有意义,其余类型恒 -1。`(name, type)` 便捷构造保现有调用点不改。
- `toCalciteType(ColumnMeta)`:DECIMAL/NUMERIC → `factory.createSqlType(DECIMAL, precision, scale)`(precision/scale 未设时用默认 10/0);其余同旧。

### DECIMAL 语义

- `DECIMAL` → 默认 precision 10、scale 0;`DECIMAL(p)` → scale 0;`DECIMAL(p,s)` 照用。
- `QueryExecutor.handleCreate` 从 `SqlColumnDeclaration.dataType`(`SqlDataTypeSpec.getPrecision()/getScale()`)取 precision/scale,未指定返回 -1,映射到默认。
- 算术/比较/聚合走 `BigDecimal`;`DecimalVector.getObject()` 返回 `BigDecimal`。

### TIME 语义

- ms 自午夜(TimeMilliVector,32-bit),与现有 TIMESTAMP 的 millis 约定一致。
- 无算术,仅比较 + CAST。JDBC `getTime` 返回 `java.sql.Time`。

### CHAR/NCHAR/NVARCHAR 语义

- 变长存储(Utf8),不做空格填充/截断(与现有 VARCHAR 一致,记为已知简化)。NCHAR/NVARCHAR 同 Utf8 物理,类型名靠元数据区分。

### 算子覆盖范围

- **SMALLINT / REAL / FLOAT / DECIMAL / NUMERIC**:比较(= != < <= > >=)+ 四则(+ - * /)+ 聚合(COUNT/SUM/AVG/MIN/MAX)+ CAST。
- **TIME / BINARY / VARBINARY**:比较 + CAST(无算术)。
- **CHAR / NCHAR / NVARCHAR**:复用现有字符串函数(物理是 VarCharVector,已走 `BuiltInFunctions` 的字符串核)。

### 各层改动

**第 1 层 类型模型 + 目录/存储:**
- `ColumnType`:加 12 值(SMALLINT/REAL/FLOAT/CHAR/NCHAR/NVARCHAR/DECIMAL/NUMERIC/TIME/BINARY/VARBINARY)。
- `ArrowTypes`:`fromSqlTypeName`(认新名,含 `NUMERIC`/`REAL`/`FLOAT`/`DOUBLE PRECISION` 等)、`toSqlTypeName`、`arrowTypeOf(ColumnType)`(含 Decimal precision/scale)、`toCalciteType(ColumnMeta)`、`field(ColumnMeta)`(带 `"minidb.type"` 元数据 + Decimal 的 precision/scale)。
- `QueryExecutor.handleCreate`:解析 precision/scale 构造 `ColumnMeta`。
- `ArrowTable`:field 挂 `"minidb.type"` 元数据。
- `StorageManager.toColumnType`:反向映射新 Arrow 类型 + 读元数据;`toTableSchema` 构造带 precision/scale 的 `ColumnMeta`。

**第 2 层 表达式(`exec/` + `exec/functions`):**
- `RexInterpreter.literalVector/nullLiteral`:SMALLINT→Int16、REAL/FLOAT→Float4、DECIMAL/NUMERIC→Decimal(BigDecimal)、TIME→Time、BINARY/VARBINARY→VarBinary、CHAR/NCHAR/NVARCHAR→VarChar(**替换**现有 SMALLINT→Int32、FLOAT/REAL/DECIMAL→Float8 的归一化)。
- `RexInterpreter.evalCast`:新增 SMALLINT/REAL/DECIMAL/TIME/BINARY/VARBINARY/CHAR 目标;`asLong/asDouble/asString/asBoolean` 扩展读新向量;`newVector`(CASE 结果)加新类型。
- `Function.outputVectorClass`:加 SMALLINT→SmallIntVector、REAL/FLOAT→Float4Vector、DECIMAL/NUMERIC→DecimalVector、TIME→TimeMilliVector、BINARY/VARBINARY→VarBinaryVector、CHAR/NCHAR/NVARCHAR→VarCharVector。
- `BuiltInFunctions` + `Kernels`/`ScalarKernels`:新增 SmallInt(short)、Float4(float)、Decimal(BigDecimal) 的算术/比较内核;Time(int)/VarBinary(byte[]) 的比较内核。

**第 3 层 行转换(算子):**
- `RowVectors.readObject/writeObject`:加 Int16/Float4/Decimal(BigDecimal)/Time(int)/VarBinary(byte[])。
- 同步 6 处私有副本:`MiniDbJoin`/`MiniDbUnion`/`WindowFunctions`/`MiniDbSetOp`/`MiniDbAggregate`/`MiniDbSort`。
- `RowCopier`、`MiniDbValues.setLiteral`:加新向量分支。
- `MiniDbAggregate`:累加器支持 DECIMAL(BigDecimal sum/avg)、REAL(float)、SMALLINT(short)。

**第 4 层 JDBC(`minidb-jdbc`):**
- `MiniDbResultSetMetaData`:MinorType + `"minidb.type"` 元数据 → type name / `java.sql.Types`(SMALLINT/FLOAT4/DECIMAL/TIMEMILLI/VARBINARY)。
- `MiniDbResultSet`:实现 `getShort`/`getFloat`/`getBigDecimal`/`getTime`/`getBytes`/`getObject` 的新类型分支。

**第 5 层 元数据(`MetadataExecutor`):**
- `sqlType(ColumnType)`/`isIntegerType`:加新类型;DECIMAL 填 COLUMN_SIZE=precision、DECIMAL_DIGITS=scale。

## 波及文件

- `catalog/`:ColumnType、ColumnMeta、ArrowTypes。
- `exec/`:QueryExecutor、RexInterpreter、MetadataExecutor、RowCopier;`exec/functions/`:Function、BuiltInFunctions、Kernels、ScalarKernels。
- `storage/`:StorageManager、ArrowTable。
- `plan/physical/`:RowVectors、MiniDbValues、MiniDbJoin、MiniDbUnion、MiniDbSetOp、MiniDbAggregate、MiniDbSort、WindowFunctions。
- `minidb-jdbc/`:MiniDbResultSetMetaData、MiniDbResultSet。
- `README.md` 支持类型列表 + CHAR/DECIMAL 简化说明。

## 测试

- **每类型 round-trip**:`CREATE TABLE t(c <type>)` → `INSERT` → `SELECT` 值一致;`DECIMAL(10,2)` 精度/scale 保留;重启(`StorageManager` reload)后类型名与 precision/scale 一致。
- **类型名保真**:`MiniDbResultSetMetaData.getColumnTypeName` 对 CHAR/NCHAR/NVARCHAR、DECIMAL/NUMERIC、FLOAT/REAL、BINARY/VARBINARY 返回声明名(经元数据)。
- **比较/算术/CAST**:新数值类型 `WHERE`/四则/`CAST`;TIME/BINARY 比较;DECIMAL 的 BigDecimal 精确算术(如 `0.1+0.2` 精确)。
- **聚合**:`SUM/AVG/MIN/MAX` over SMALLINT/REAL/DECIMAL。
- **`ArrowTypesTest`**:新增各类型四向映射断言。

## 坑(实施时注意,最终汇总进 CLAUDE.md)

1. `ColumnMeta` 加字段会让所有 `new ColumnMeta(name, type)` 调用点编译——用便捷构造 `(name, type)` 兜住。
2. `DecimalVector.getObject()` 返回 `BigDecimal`;`setSafe(row, BigDecimal)` 直接可用;Field 构造需 `new ArrowType.Decimal(precision, scale, 128)`(bitWidth 必须 128)。
3. `SmallIntVector` get/set 是 `short`;`Float4Vector` 是 `float`;`TimeMilliVector` get/set 是 `int`(ms);`VarBinaryVector` 是 `byte[]`。
4. `RexInterpreter.literalVector` 现在把 SMALLINT/TINYINT 归一成 IntVector、DECIMAL/FLOAT/REAL 归一成 Float8——改原生后要同步 `Function.outputVectorClass`,否则字面量与列的向量类型不一致,`Function.resolve` 找不到重载。
5. Calcite 可能把 SMALLINT/FLOAT/REAL 在表达式中自动提升为 INTEGER/DOUBLE(结果类型按 `getRowType()` 落地),原生 Int16/Float4 向量主要出现在**列存储**与 round-trip;表达式核仍需按 Calcite 推导类型注册,别假设结果恒为 SmallInt/Float4。
6. `QueryExecutor.handleCreate` 用 `dataType.getTypeName().getSimple()` 取类型名——`DOUBLE PRECISION` 的 getSimple 可能只回 "PRECISION",需验证并改用 `SqlDataTypeSpec.getTypeName()` 的 `SqlTypeName` 枚举或处理复合名。
7. `StorageManager.toColumnType` 目前 Int 宽度 `==32? INTEGER:BIGINT`——新增 Int16 后必须显式分支,否则 16-bit 会误判成 BIGINT。
8. DECIMAL 输出 `Field` 时,`ArrowTypes.field(RelDataTypeField)`(无 precision/scale 的 Calcite 类型路径)要能从 `RelDataType.getPrecision()/getScale()` 取,否则 Decimal Field 精度丢失。

## 不做

- CLOB/BLOB、NCLOB(大对象)。
- TIME/TIMESTAMP WITH TIME ZONE(时区)。
- INTERVAL(两类区间 + 算术语义)。
- ARRAY/MULTISET/ROW(集合/用户定义类型)。
- JSON(SQL:2016+)。
- TINYINT(非标准,MySQL 扩展)。
- CHAR 定长填充/截断语义(记为简化)。
- DECIMAL 的溢出/舍入规则精确实现(依赖 Calcite 类型推导 + BigDecimal,不额外手写)。
