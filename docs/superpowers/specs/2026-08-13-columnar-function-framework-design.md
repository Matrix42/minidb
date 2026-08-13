# 列式函数框架设计

> 日期:2026-08-13
> 状态:设计定稿,待写实现计划

## 目标

把 `RexInterpreter` 从一个「大 switch + 手写向量化循环」的单体类,重构为一个**列式函数框架**:标量函数的语义内核与「null 传播 / 类型分发 / 输出向量分配 / 批循环」解耦,新增一个函数只需写一个 typed 标量 lambda 并注册。本次迁移现有函数(算术/比较),并新增一小批标量函数作为框架的验收,同时为后续 UDF 预留扩展点。

**非目标(YAGNI)**:不实现 UDF、不做代码生成/算子融合、不补全数据类型(DECIMAL 精确存储等)、不引入 CPU SIMD。

## 背景与现状

- `RexInterpreter`(当前)手写支持:AND/OR/NOT、CAST、CASE、比较(`= != < <= > >=`)、算术(`+ - * /`)。每个都自己写 `for i in rows` 循环 + null 判断,逻辑重复。
- 入口签名稳定:`ValueVector eval(RexNode, VectorSchemaRoot)`,被 `MiniDbFilter`/`MiniDbProject`/`MiniDbCalc`/`MiniDbAggregate`(聚合表达式参数)/`MiniDbNestedLoopJoin`(join 条件)五个调用方使用。**本设计不改这个入口签名,调用方无感。**
- 已有类型映射 `ArrowTypes.arrowTypeOf(SqlTypeName)` 把 Calcite 类型映射到 Arrow 向量(`INT`→IntVector、`BIGINT`→BigIntVector、`DOUBLE/FLOAT/REAL/DECIMAL`→Float8Vector、`VARCHAR/CHAR`→VarCharVector、`BOOLEAN`→BitVector、`DATE`→DateDayVector、`TIMESTAMP`→TimeStampMilliVector)。框架复用此映射,不另建类型表。

## 技术方向(已与用户对齐)

- **纯 Java 手写 typed-loop 内核**。分发每 batch 只做一次,热循环体是单态 typed 调用,循环内零 `instanceof`/装箱/反射。
- **不用 CPU SIMD**。Arrow Java 无 compute kernels/SIMD;JDK 17 Vector API 是 incubator 且与 Arrow `ArrowBuf` 内存布局不契合。所谓「向量化」指列式批处理 + 无装箱 + 数据局部性,这层纯 Java 已拿到。
- **不用 Janino 代码生成**(见「代码生成备忘」)。

## 架构总览

新包 `com.minidb.server.exec.functions`。`RexInterpreter` 退化为**薄壳**:递归 eval 操作数(RexInputRef / RexLiteral)→ 命中特殊表达式走专用 handler(AND/OR/NOT/CAST/CASE)→ 其余标量函数走 `FunctionRegistry`。

```
RexInterpreter.eval(rexNode, batch)
  ├─ RexInputRef  → RowCopier.copyVector(...)
  ├─ RexLiteral   → literalVector(...)          // 现有逻辑保留
  └─ RexCall
       ├─ AND/OR/NOT → logic/not                // 3VL,保留为专用 handler
       ├─ CAST       → evalCast                 // 类型转换,保留为专用 handler
       ├─ CASE       → caseExpr                 // 控制流,保留为专用 handler
       └─ 其余       → registry.lookup(op).evaluate(args, resultType, allocator)
```

## 内核抽象(两层)

### Tier-1 标量核(零样板、无装箱)

typed 函数式接口(全部 `@FunctionalInterface`,包内顶层):

```java
// 一元
interface IntUnary     { int    apply(int v); }
interface LongUnary    { long   apply(long v); }
interface DoubleUnary  { double apply(double v); }
interface StringUnary  { String apply(String v); }
interface StringToInt  { int    apply(String v); }   // LENGTH 这类 varchar→int

// 二元
interface IntBinary    { int    apply(int a, int b); }
interface LongBinary   { long   apply(long a, long b); }
interface DoubleBinary { double apply(double a, double b); }
interface StringBinary { String apply(String a, String b); }

// 比较核:返回 -1/0/1,框架按 SqlKind 转 bool
interface IntCompare    { int apply(int a, int b); }
interface LongCompare   { int apply(long a, long b); }
interface DoubleCompare { int apply(double a, double b); }
interface StringCompare { int apply(String a, String b); }
```

### Tier-2 完整核(逃生舱)

给三值逻辑、跨列、以及将来需要完整控制的 UDF / 复杂函数:

```java
@FunctionalInterface
interface Kernel {
    // out 已按 resultType 分配好、未 setValueCount;args 与 out 长度一致
    void execute(List<ValueVector> args, FieldVector out);
}
```

## 共享循环 `Kernels`

`Kernels` 静态工具提供 STRICT null 语义 + 分配后的写回循环,让每个重载是一行方法引用:

```java
static void fillUnaryInt   (IntVector in,    IntVector out,    IntUnary op)
static void fillUnaryLong  (BigIntVector in, BigIntVector out, LongUnary op)
static void fillUnaryDouble(Float8Vector in, Float8Vector out, DoubleUnary op)
static void fillUnaryString(VarCharVector in, VarCharVector out, StringUnary op)
static void fillStringToInt(VarCharVector in, IntVector out,    StringToInt op)

static void fillBinaryInt   (IntVector l, IntVector r, IntVector out, IntBinary op)
static void fillBinaryLong  (BigIntVector l, BigIntVector r, BigIntVector out, LongBinary op)
static void fillBinaryDouble(Float8Vector l, Float8Vector r, Float8Vector out, DoubleBinary op)
static void fillBinaryString(VarCharVector l, VarCharVector r, VarCharVector out, StringBinary op)

static void fillCompareInt   (IntVector l, IntVector r, BitVector out, IntCompare cmp, SqlKind kind)
static void fillCompareLong  (BigIntVector l, BigIntVector r, BitVector out, LongCompare cmp, SqlKind kind)
static void fillCompareDouble(Float8Vector l, Float8Vector r, BitVector out, DoubleCompare cmp, SqlKind kind)
static void fillCompareString(VarCharVector l, VarCharVector r, BitVector out, StringCompare cmp, SqlKind kind)
```

每个 `fill*` 循环体:任一输入为 null → `out.setNull(i)`,跳过运算;否则取原语值 → 调用 op → 写回。`fill*String` 结果用 UTF-8 编码写 `VarCharVector`。

例:`ABS` 的 int 重载即 `(args, out) -> Kernels.fillUnaryInt((IntVector) args.get(0), (IntVector) out, Math::abs)`。

## Function / Overload / FunctionRegistry

```java
record Overload(List<Class<? extends ValueVector>> inputTypes,
                Class<? extends FieldVector> outputType, Kernel kernel) {}

final class Function {
    final String name;
    final List<Overload> overloads;

    ValueVector evaluate(List<ValueVector> args, RelDataType resultType, BufferAllocator allocator) {
        Class<? extends FieldVector> outputClass = outputVectorClass(resultType);
        Kernel kernel = resolve(args, outputClass);        // 按输入 + 输出类型匹配
        FieldVector out = ArrowTypes.field(resultType, "expr").createVector(allocator);
        out.setInitialCapacity(args.get(0).getValueCount());
        out.allocateNew();
        try { kernel.execute(args, out); } finally { for (ValueVector a : args) a.close(); }
        out.setValueCount(args.get(0).getValueCount());
        return out;
    }
}
```

- **分发键是 `SqlOperator`**(`SqlStdOperatorTable.PLUS`、`UPPER`、`EQUALS` 等单例),不是 `SqlKind`——算术、比较、字符串/数学函数统一走这一张表。
- `resolve` 按**输入向量类型 + 输出向量类型**匹配。输出类型必须参与分发:同一输入类型可能映射多种输出(ABS 的 `[BigIntVector]` 输入对 INTEGER 字面量产 IntVector、对 BIGINT 列产 BigIntVector),仅按输入类型无法区分。输出向量由 `resultType`(即 `call.getType()`)经 `outputVectorClass` 映成箭头向量类、再经 `ArrowTypes` 分配。
- `BuiltInFunctions` 静态注册全部内置,`FunctionRegistry` 可变——为 UDF 预留。

## null 语义

- 框架默认 **STRICT**:任一操作数为 null → 结果为 null。覆盖算术、比较、所有新标量函数。
- **AND/OR/NOT(三值逻辑)、CAST(类型转换)、CASE(短路)** 不塞进 STRICT 标量模型,保留为 `RexInterpreter` 的专用 handler(现有 `logic`/`not`/`evalCast`/`caseExpr` 清理后原位保留)。

## 迁移清单

- **算术** `+ - * /`:各注册 int / long / double 三个重载(结果类型分别 INTEGER / BIGINT / DOUBLE)。除零保持现有 `ArithmeticException` 语义。
- **比较** `= != < <= > >=`:各注册 int / long / double / varchar 四个重载,`fillCompare*` 按 `SqlKind` 把 -1/0/1 转 bool。六个算子各自一个 `Function`(核内闭合自己的 kind)。

## 新增函数清单(验收)

- **字符串**:`UPPER(varchar)`、`LOWER(varchar)`(`StringUnary`);`LENGTH(varchar)→int`、`CHAR_LENGTH(varchar)→int`(`StringToInt`,LENGTH 是库函数、需开 POSTGRESQL 库);`CONCAT(varchar, ...)`(变参 1/2/3 参,Tier-2 核)、`||`(二元 `StringBinary`);`SUBSTRING(varchar, int, int)`(走 **Tier-2 完整核**);`TRIM(varchar)`(专用 handler:Calcite 解析期把 `TRIM(s)` 重写为 3 参 `TRIM(Flag,' ',s)`,Flag 是 SYMBOL 字面量,由 `RexInterpreter` 专 `case TRIM` 从 RexLiteral 取 Flag 后求值)。
- **数学**:`ABS(int)`、`ABS(bigint)`、`ABS(double)`;`ROUND(double)`、`FLOOR(double)`、`CEIL(double)`(`DoubleUnary`)。

## 类型边界

`ArrowTypes` 现有把 `FLOAT/REAL/DECIMAL` 都坍缩成 `Float8Vector`。因此**返回 DECIMAL 的函数(如对 decimal 入参的 ROUND/FLOOR/CEIL)得到 double 结果**——这是既有简化,非本框架引入,本次不修。数据类型的真正补全(DECIMAL 精确存储等)是独立后续任务。

## UDF 路径(仅预留,不实现)

UDF 由两半组成:

1. **校验侧**:向 Calcite 注册一个 `SqlFunction`(让 SQL 能解析到函数名)。
2. **执行侧**:向 `FunctionRegistry` 注册一个 `Function`(typed lambda 或 Tier-2 完整核)。

`FunctionRegistry` 是可变 `Map`,将来从 catalog/DDL 挂载即可;非 STRICT 的 UDF 用 Tier-2 完整核兜底。本迭代只保证这个扩展点存在(注册表可变、内核两层),不写注册流程。

## 代码生成备忘(记录在案,暂不做)

将来若要做 **算子融合**(把 `ABS(a+b)` 熔成一个循环)或消掉最后一点分发开销:Janino 已在 classpath(随 Calcite 传递依赖),内核小而可组合,能加而不推翻现有内核。**当前不做**——typed-loop 已消除逐行装箱与逐行分发,代码生成只剩 fusion 收益,对玩具 DB 不值得那份复杂度与调试成本。

## 文件结构

新建 `exec/functions/`:

- `Kernel.java` — Tier-2 接口
- `ScalarKernels.java` — Tier-1 typed 函数式接口(IntUnary/…/StringCompare)
- `Kernels.java` — 共享 `fill*` 循环 + STRICT null 处理
- `Overload.java` — record
- `Function.java` — 名称 + 重载 + `evaluate`
- `FunctionRegistry.java` — `Map<SqlOperator, Function>`
- `BuiltInFunctions.java` — 静态注册全部内置

改动:

- `RexInterpreter.java` — 薄壳 + 保留 AND/OR/NOT/CAST/CASE 专用 handler;持 `FunctionRegistry`(默认 `BuiltInFunctions`)
- `ExecContext.java` — `RexInterpreter` 构造处(如有需要注入 registry)

## 测试

- **回归**:现有 199 测全绿(迁移不改变语义)。
- **新增**:每个新函数正反向断言——正常值、null 入参(STRICT → null)、空串(`LENGTH('')`、`TRIM`)、`SUBSTRING` 越界/负 offset、`CONCAT` null 传播、`ABS` 负数、除零仍抛异常。
- **类型**:`ABS` 对 int/long/double 三路都验证;`LENGTH` 返回 INTEGER 不是 BIGINT。

## 设计权衡回顾

- 为什么「typed lambda + 共享循环」而不是每个函数写完整向量化内核:消除样板、UDF 门槛低;同时保留 Tier-2 完整核作为逃生舱(SUBSTRING、将来复杂函数)。
- 为什么 AND/OR/NOT/CAST/CASE 不进框架:三值逻辑/短路/类型转换不是「严格 null 的标量函数」,硬塞会扭曲内核模型。
- 为什么按 `SqlOperator` 分发而非 `SqlKind`:算术、比较、函数三类统一走一张表,且 operator 是精确键(比较六算子各自独立)。
