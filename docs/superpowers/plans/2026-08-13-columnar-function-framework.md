# 列式函数框架实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `RexInterpreter` 从「大 switch + 手写向量化循环」重构为「typed-lambda 内核 + 共享循环 + 注册表」的列式函数框架,迁移算术/比较,新增字符串/数学一小批函数。

**Architecture:** 新包 `com.minidb.server.exec.functions` 承载内核两层抽象(Tier-1 typed 标量 lambda、Tier-2 完整 Kernel)、共享 `Kernels.fill*` 循环、`Function`/`Overload`/`FunctionRegistry`、`BuiltInFunctions`。`RexInterpreter` 退成薄壳:AND/OR/NOT/CAST/CASE 保留为专用 handler,其余标量函数走注册表。入口签名 `ValueVector eval(RexNode, VectorSchemaRoot)` 不变。

**Tech Stack:** Java 17、Apache Arrow(ValueVector/FieldVector)、Apache Calcite(SqlOperator/SqlKind/RexNode)。

**Spec:** `docs/superpowers/specs/2026-08-13-columnar-function-framework-design.md`

## Global Constraints

- JDK 17 必须;构建:`./mvnw.cmd test`、单测:`./mvnw.cmd test -pl minidb-server -Dtest=XxxTest`(bash 下直接跑 `./mvnw.cmd`)。
- **入口签名 `RexInterpreter.eval(RexNode, VectorSchemaRoot)` 不得改动**——五个调用方(`MiniDbFilter`/`MiniDbProject`/`MiniDbCalc`/`MiniDbAggregate`/`MiniDbNestedLoopJoin`)无感。
- **不改** minidb-protocol、7 个稳定算子(MiniDbScan/Filter/Project/Sort/Values/Modify/Aggregate)、`ArrowTypes`/`RowCopier`(只读其 API)。
- 命名自解释,注释解释 WHY 不解释 WHAT;代码是给人读的。
- 测试 JUnit 5 + `RootAllocator`;`@TempDir` 仅 QueryExecutor 集成测试用。
- conventional commit(`feat:`/`test:`/`refactor:`/`docs:`),在 `master` 工作,小步提交,不 amend、不 `--no-verify`。
- 纯 Java 手写 typed-loop;**不用** SIMD、**不用** Janino 代码生成(本迭代)。

---

## Task 1: 内核两层抽象 + 共享循环(Kernel / ScalarKernels / Kernels)

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/functions/Kernel.java`
- Create: `minidb-server/src/main/java/com/minidb/server/exec/functions/ScalarKernels.java`
- Create: `minidb-server/src/main/java/com/minidb/server/exec/functions/Kernels.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/FunctionFrameworkTest.java`

**Interfaces:**
- Produces: `Kernel.execute(List<ValueVector> args, FieldVector out)`;`ScalarKernels.IntUnary` 等 typed 接口;`Kernels.fill*` 共享循环。

- [ ] **Step 1: 写失败测试** — `FunctionFrameworkTest` 只测 `Kernels.fill*`(纯 Arrow,无 Calcite)。测试直接构造 `IntVector`/`BigIntVector`/`Float8Vector`/`VarCharVector`/`BitVector`,填数据 + null,调用 `Kernels.fillUnaryInt(...)` 等,断言结果与 null 传播(STRICT:任一入参 null → 结果 null)。先写 2 个测试:`fillUnaryInt`(含 null 行)、`fillCompareInt`(三种 SqlKind:EQUALS/GREATER_THAN/LESS_THAN)。

- [ ] **Step 2: 跑测试确认失败** — `./mvnw.cmd test -pl minidb-server -Dtest=FunctionFrameworkTest`,预期编译失败(类不存在)。

- [ ] **Step 3: 实现** — 写三个文件:

`Kernel.java`:
```java
package com.minidb.server.exec.functions;

import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

/** 完整内核:args 为已求值的操作数向量(与 out 等长),out 已按结果类型分配、未 setValueCount。 */
@FunctionalInterface
public interface Kernel {
    void execute(List<ValueVector> args, FieldVector out);
}
```

`ScalarKernels.java`(typed 函数式接口,嵌套在一个容器类里,避免 14 个顶层文件):
```java
package com.minidb.server.exec.functions;

/** Tier-1 标量核接口:每个是原语/引用类型上的无装箱 lambda,由 Kernels 循环调用。 */
public final class ScalarKernels {
    private ScalarKernels() {}

    @FunctionalInterface public interface IntUnary { int apply(int v); }
    @FunctionalInterface public interface LongUnary { long apply(long v); }
    @FunctionalInterface public interface DoubleUnary { double apply(double v); }
    @FunctionalInterface public interface StringUnary { String apply(String v); }
    @FunctionalInterface public interface StringToInt { int apply(String v); }

    @FunctionalInterface public interface IntBinary { int apply(int a, int b); }
    @FunctionalInterface public interface LongBinary { long apply(long a, long b); }
    @FunctionalInterface public interface DoubleBinary { double apply(double a, double b); }
    @FunctionalInterface public interface StringBinary { String apply(String a, String b); }

    @FunctionalInterface public interface IntCompare { int apply(int a, int b); }
    @FunctionalInterface public interface LongCompare { int apply(long a, long b); }
    @FunctionalInterface public interface DoubleCompare { int apply(double a, double b); }
    @FunctionalInterface public interface StringCompare { int apply(String a, String b); }
}
```

`Kernels.java` — 共享循环 + STRICT null。完整方法清单(每个循环体:入参 null → `out.setNull(i)`;否则取原语 → 调 op → `out.setSafe(i, ...)`):
```java
package com.minidb.server.exec.functions;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.sql.SqlKind;

public final class Kernels {
    private Kernels() {}

    public static void fillUnaryInt(IntVector in, IntVector out, ScalarKernels.IntUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryLong(BigIntVector in, BigIntVector out, ScalarKernels.LongUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryDouble(Float8Vector in, Float8Vector out, ScalarKernels.DoubleUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(in.get(i)));
        }
    }
    public static void fillUnaryString(VarCharVector in, VarCharVector out, ScalarKernels.StringUnary op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(new String(in.get(i), StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8));
        }
    }
    public static void fillStringToInt(VarCharVector in, IntVector out, ScalarKernels.StringToInt op) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(new String(in.get(i), StandardCharsets.UTF_8)));
        }
    }

    public static void fillBinaryInt(IntVector l, IntVector r, IntVector out, ScalarKernels.IntBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillBinaryLong(BigIntVector l, BigIntVector r, BigIntVector out, ScalarKernels.LongBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillBinaryDouble(Float8Vector l, Float8Vector r, Float8Vector out, ScalarKernels.DoubleBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(l.get(i), r.get(i)));
        }
    }
    public static void fillBinaryString(VarCharVector l, VarCharVector r, VarCharVector out, ScalarKernels.StringBinary op) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            String a = new String(l.get(i), StandardCharsets.UTF_8);
            String b = new String(r.get(i), StandardCharsets.UTF_8);
            out.setSafe(i, op.apply(a, b).getBytes(StandardCharsets.UTF_8));
        }
    }

    public static void fillCompareInt(IntVector l, IntVector r, BitVector out,
                                      ScalarKernels.IntCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareLong(BigIntVector l, BigIntVector r, BitVector out,
                                       ScalarKernels.LongCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareDouble(Float8Vector l, Float8Vector r, BitVector out,
                                         ScalarKernels.DoubleCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, compareToBool(cmp.apply(l.get(i), r.get(i)), kind) ? 1 : 0);
        }
    }
    public static void fillCompareString(VarCharVector l, VarCharVector r, BitVector out,
                                         ScalarKernels.StringCompare cmp, SqlKind kind) {
        for (int i = 0; i < l.getValueCount(); i++) {
            if (l.isNull(i) || r.isNull(i)) { out.setNull(i); continue; }
            String a = new String(l.get(i), StandardCharsets.UTF_8);
            String b = new String(r.get(i), StandardCharsets.UTF_8);
            out.setSafe(i, compareToBool(cmp.apply(a, b), kind) ? 1 : 0);
        }
    }

    /** 把 -1/0/1 比较结果按 SqlKind 转 bool。 */
    private static boolean compareToBool(int cmp, SqlKind kind) {
        return switch (kind) {
            case EQUALS -> cmp == 0;
            case NOT_EQUALS -> cmp != 0;
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUAL -> cmp <= 0;
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUAL -> cmp >= 0;
            default -> throw new IllegalArgumentException("not a comparison: " + kind);
        };
    }
}
```

- [ ] **Step 4: 跑测试确认通过** — `./mvnw.cmd test -pl minidb-server -Dtest=FunctionFrameworkTest`,预期绿。

- [ ] **Step 5: 提交**
```bash
git add minidb-server/src/main/java/com/minidb/server/exec/functions/Kernel.java minidb-server/src/main/java/com/minidb/server/exec/functions/ScalarKernels.java minidb-server/src/main/java/com/minidb/server/exec/functions/Kernels.java minidb-server/src/test/java/com/minidb/server/exec/FunctionFrameworkTest.java
git commit -m "feat: 列式函数内核抽象(Kernel/ScalarKernels/Kernels 共享循环)"
```

---

## Task 2: 分发与注册表(Overload / Function / FunctionRegistry)

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/functions/Overload.java`
- Create: `minidb-server/src/main/java/com/minidb/server/exec/functions/Function.java`
- Create: `minidb-server/src/main/java/com/minidb/server/exec/functions/FunctionRegistry.java`
- Modify: `minidb-server/src/test/java/com/minidb/server/exec/FunctionFrameworkTest.java`

**Interfaces:**
- Consumes: `Kernel`、`ScalarKernels`、`Kernels`(Task 1)。
- Produces: `Function.evaluate(List<ValueVector> args, RelDataType resultType, BufferAllocator allocator) → ValueVector`;`FunctionRegistry.register/lookup`。

- [ ] **Step 1: 写失败测试** — 在 `FunctionFrameworkTest` 加 `functionDispatchPicksOverload`:`Function` 带两个重载(int→int 用 `a*2`;long→long 用 `a+1`),分别用 IntVector/BigIntVector 输入调 `evaluate`,断言各选对内核、输出类型正确、输入向量被关闭(可只断言结果值)。再加 `unknownOverloadThrows`(用无匹配重载的向量类型,断言异常)。

- [ ] **Step 2: 跑测试确认失败** — 编译失败。

- [ ] **Step 3: 实现** — 三个文件:

`Overload.java`:
```java
package com.minidb.server.exec.functions;

import java.util.List;
import org.apache.arrow.vector.ValueVector;

/** 一个重载:声明的输入向量类型 + 内核。 */
public record Overload(List<Class<? extends ValueVector>> inputTypes, Kernel kernel) {}
```

`Function.java`:
```java
package com.minidb.server.exec.functions;

import com.minidb.server.catalog.ArrowTypes;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.rel.type.RelDataType;

/** 一个函数:若干重载 + 按输入类型分发 + 分配输出向量。 */
public final class Function {
    private final String name;
    private final List<Overload> overloads;

    public Function(String name, List<Overload> overloads) {
        this.name = name;
        this.overloads = overloads;
    }

    public ValueVector evaluate(List<ValueVector> args, RelDataType resultType,
                                BufferAllocator allocator) {
        Kernel kernel = resolve(args);
        int rows = args.get(0).getValueCount();
        FieldVector out = ArrowTypes.field(resultType, "expr").createVector(allocator);
        out.setInitialCapacity(rows);
        out.allocateNew();
        try {
            kernel.execute(args, out);
        } finally {
            for (ValueVector a : args) {
                a.close();
            }
        }
        out.setValueCount(rows);
        return out;
    }

    private Kernel resolve(List<ValueVector> args) {
        for (Overload o : overloads) {
            if (matches(o.inputTypes(), args)) {
                return o.kernel();
            }
        }
        throw new UnsupportedOperationException(
                "no overload of " + name + " for argument types " + argClasses(args));
    }

    private static boolean matches(List<Class<? extends ValueVector>> types, List<ValueVector> args) {
        if (types.size() != args.size()) {
            return false;
        }
        for (int i = 0; i < types.size(); i++) {
            if (!types.get(i).isInstance(args.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static List<Class<?>> argClasses(List<ValueVector> args) {
        return args.stream().map(ValueVector::getClass).toList();
    }
}
```

`FunctionRegistry.java`:
```java
package com.minidb.server.exec.functions;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlOperator;

/** 按 SqlOperator 分发的函数表;可变,供 UDF 挂载。 */
public final class FunctionRegistry {
    private final Map<SqlOperator, Function> byOperator = new HashMap<>();

    public Function lookup(SqlOperator op) {
        return byOperator.get(op);
    }

    public void register(SqlOperator op, Function f) {
        byOperator.put(op, f);
    }
}
```

- [ ] **Step 4: 跑测试确认通过** — 绿。

- [ ] **Step 5: 提交**
```bash
git add minidb-server/src/main/java/com/minidb/server/exec/functions/Overload.java minidb-server/src/main/java/com/minidb/server/exec/functions/Function.java minidb-server/src/main/java/com/minidb/server/exec/functions/FunctionRegistry.java minidb-server/src/test/java/com/minidb/server/exec/FunctionFrameworkTest.java
git commit -m "feat: 函数分发与注册表(Overload/Function/FunctionRegistry)"
```

---

## Task 3: 迁移算术 + 重接 RexInterpreter(BuiltInFunctions / RexInterpreter)

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/RexInterpreter.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/ExecContext.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java`(回归,不改现有断言,可加)

**Interfaces:**
- Consumes: Task 1、Task 2。
- Produces: `BuiltInFunctions.newRegistry()`;`RexInterpreter` 持 `FunctionRegistry` 字段、`evalCall` 走注册表。

- [ ] **Step 1: 写失败测试** — `RexInterpreterTest` 已覆盖 `GREATER_THAN`、`PLUS`、`DIVIDE`(double)、`AND`(3VL),迁移后必须仍绿。新增一个 `longArithmetic`(构造 BIGINT 类型 + `MINUS` 或 `TIMES`,断言 BigIntVector 结果)作为 long 重载的显式覆盖。

- [ ] **Step 2: 跑测试确认失败** — 现有算术/比较走旧代码仍绿;新增 long 测试若走旧代码也绿(旧 `arithmetic` 已支持 long)。此任务更接近「重构保持绿」而非「红→绿」:跑全量 `./mvnw.cmd test -pl minidb-server -Dtest=RexInterpreterTest`,先记下当前绿;实现后必须仍绿。

- [ ] **Step 3: 实现 `BuiltInFunctions`** — 注册算术,写清 helper:

```java
package com.minidb.server.exec.functions;

import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public final class BuiltInFunctions {
    private BuiltInFunctions() {}

    public static FunctionRegistry newRegistry() {
        FunctionRegistry registry = new FunctionRegistry();
        arithmetic(registry);
        return registry;
    }

    private static void arithmetic(FunctionRegistry r) {
        binaryInt(r, SqlStdOperatorTable.PLUS, Integer::sum);
        binaryInt(r, SqlStdOperatorTable.MINUS, (a, b) -> a - b);
        binaryInt(r, SqlStdOperatorTable.TIMES, (a, b) -> a * b);
        binaryInt(r, SqlStdOperatorTable.DIVIDE, (a, b) -> { if (b == 0) throw new ArithmeticException("division by zero"); return a / b; });

        binaryLong(r, SqlStdOperatorTable.PLUS, Long::sum);
        binaryLong(r, SqlStdOperatorTable.MINUS, (a, b) -> a - b);
        binaryLong(r, SqlStdOperatorTable.TIMES, (a, b) -> a * b);
        binaryLong(r, SqlStdOperatorTable.DIVIDE, (a, b) -> { if (b == 0) throw new ArithmeticException("division by zero"); return a / b; });

        binaryDouble(r, SqlStdOperatorTable.PLUS, Double::sum);
        binaryDouble(r, SqlStdOperatorTable.MINUS, (a, b) -> a - b);
        binaryDouble(r, SqlStdOperatorTable.TIMES, (a, b) -> a * b);
        binaryDouble(r, SqlStdOperatorTable.DIVIDE, (a, b) -> { if (b == 0) throw new ArithmeticException("division by zero"); return a / b; });
    }

    private static void binaryInt(FunctionRegistry r, SqlOperator op, ScalarKernels.IntBinary k) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(IntVector.class, IntVector.class),
                (args, out) -> Kernels.fillBinaryInt((IntVector) args.get(0), (IntVector) args.get(1),
                        (IntVector) out, k)))));
    }
    private static void binaryLong(FunctionRegistry r, SqlOperator op, ScalarKernels.LongBinary k) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(BigIntVector.class, BigIntVector.class),
                (args, out) -> Kernels.fillBinaryLong((BigIntVector) args.get(0), (BigIntVector) args.get(1),
                        (BigIntVector) out, k)))));
    }
    private static void binaryDouble(FunctionRegistry r, SqlOperator op, ScalarKernels.DoubleBinary k) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(Float8Vector.class, Float8Vector.class),
                (args, out) -> Kernels.fillBinaryDouble((Float8Vector) args.get(0), (Float8Vector) args.get(1),
                        (Float8Vector) out, k)))));
    }
}
```

- [ ] **Step 4: 实现 `RexInterpreter` 重构** — 保留 `eval`(RexInputRef→copyVector、RexLiteral→literalVector、RexCall→evalCall)、`logic`/`not`/`caseExpr`/`evalCast`/`literalVector`/`nullLiteral`/`newVector`/`asLong`/`asDouble`/`stringCompare`/`isDouble` 原样。**删除** `comparison`/`arithmetic`/`applyLong`/`applyDouble`。改 `evalCall` 为:

```java
private final FunctionRegistry functions;

public RexInterpreter(BufferAllocator allocator) {
    this(allocator, BuiltInFunctions.newRegistry());
}

public RexInterpreter(BufferAllocator allocator, FunctionRegistry functions) {
    this.allocator = allocator;
    this.functions = functions;
}

private ValueVector evalCall(RexCall call, VectorSchemaRoot input) {
    SqlKind kind = call.getKind();
    switch (kind) {
        case AND:  return logic(call.getOperands(), input, true);
        case OR:   return logic(call.getOperands(), input, false);
        case NOT:  return not(call.getOperands().get(0), input);
        case CAST: return evalCast(call, input);
        case CASE: return caseExpr(call, input);
        default: {
            List<ValueVector> args = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                args.add(eval(operand, input));
            }
            Function f = functions.lookup(call.getOperator());
            if (f == null) {
                for (ValueVector a : args) a.close();
                throw new UnsupportedOperationException("unsupported operator: " + call.getOperator());
            }
            return f.evaluate(args, call.getType(), allocator);
        }
    }
}
```

新增 import:`com.minidb.server.exec.functions.*`、`java.util.ArrayList`。删除不再用的 import(SqlTypeName 若只剩 literalVector 用则保留)。

- [ ] **Step 5: 实现 `ExecContext`** — 构造不变(`new RexInterpreter(allocator)`),无改动,仅确认。

- [ ] **Step 6: 跑全量确认绿** — `./mvnw.cmd test -pl minidb-server`,算术/比较/3VL 迁移无回归。

- [ ] **Step 7: 提交**
```bash
git add minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java minidb-server/src/main/java/com/minidb/server/exec/RexInterpreter.java minidb-server/src/main/java/com/minidb/server/exec/ExecContext.java minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java
git commit -m "refactor: RexInterpreter 迁移算术到列式函数框架"
```

---

## Task 4: 迁移比较(= != < <= > >=)

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java`(回归 `comparisonGreaterThan`,新增 `stringComparison`)

**Interfaces:**
- Consumes: Task 3 的 `BuiltInFunctions` 结构、`Kernels.fillCompare*`。

- [ ] **Step 1: 写失败测试** — 在 `RexInterpreterTest` 加 `stringComparison`:构造 VARCHAR 类型 + `EQUALS` 比较两个字符串输入引用,断言 BitVector 结果(含 null 行 STRICT)。现有 `comparisonGreaterThan` 作回归。

- [ ] **Step 2: 跑测试确认失败** — 字符串比较当前 `RexInterpreter` 的 `comparison` 已支持(旧代码 stringDomain 分支),会绿;此任务同样是「重构保持绿」。新增 string 测试先绿(旧代码),实现后仍绿。

- [ ] **Step 3: 实现** — 在 `BuiltInFunctions` 加 `comparison(registry)`(在 `newRegistry()` 里 `arithmetic(registry); comparison(registry);`),用 helper 注册六算子 × 四类型:

```java
private static void comparison(FunctionRegistry r) {
    compareInt(r, SqlStdOperatorTable.EQUALS, SqlKind.EQUALS);
    compareInt(r, SqlStdOperatorTable.NOT_EQUALS, SqlKind.NOT_EQUALS);
    compareInt(r, SqlStdOperatorTable.LESS_THAN, SqlKind.LESS_THAN);
    compareInt(r, SqlStdOperatorTable.LESS_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL);
    compareInt(r, SqlStdOperatorTable.GREATER_THAN, SqlKind.GREATER_THAN);
    compareInt(r, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL);
    // 同样六行 × long / double / string
}

private static void compareInt(FunctionRegistry r, SqlOperator op, SqlKind kind) {
    r.register(op, new Function(op.getName(), List.of(new Overload(
            List.of(IntVector.class, IntVector.class),
            (args, out) -> Kernels.fillCompareInt((IntVector) args.get(0), (IntVector) args.get(1),
                    (BitVector) out, Integer::compare, kind)))));
}
private static void compareLong(FunctionRegistry r, SqlOperator op, SqlKind kind) {
    r.register(op, new Function(op.getName(), List.of(new Overload(
            List.of(BigIntVector.class, BigIntVector.class),
            (args, out) -> Kernels.fillCompareLong((BigIntVector) args.get(0), (BigIntVector) args.get(1),
                    (BitVector) out, Long::compare, kind)))));
}
private static void compareDouble(FunctionRegistry r, SqlOperator op, SqlKind kind) {
    r.register(op, new Function(op.getName(), List.of(new Overload(
            List.of(Float8Vector.class, Float8Vector.class),
            (args, out) -> Kernels.fillCompareDouble((Float8Vector) args.get(0), (Float8Vector) args.get(1),
                    (BitVector) out, Double::compare, kind)))));
}
private static void compareString(FunctionRegistry r, SqlOperator op, SqlKind kind) {
    r.register(op, new Function(op.getName(), List.of(new Overload(
            List.of(VarCharVector.class, VarCharVector.class),
            (args, out) -> Kernels.fillCompareString((VarCharVector) args.get(0), (VarCharVector) args.get(1),
                    (BitVector) out, String::compareTo, kind)))));
}
```

- [ ] **Step 4: 跑全量确认绿** — `./mvnw.cmd test -pl minidb-server`。

- [ ] **Step 5: 提交**
```bash
git add minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java
git commit -m "refactor: 迁移比较算子到列式函数框架"
```

---

## Task 5: 新增字符串函数(UPPER/LOWER/TRIM/LENGTH/CONCAT/SUBSTRING)

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java`

**Interfaces:**
- Consumes: `Kernels.fillUnaryString`/`fillStringToInt`/`fillBinaryString`/`Kernel`(SUBSTRING 用 Tier-2)。

- [ ] **Step 1: 写失败测试** — 在 `RexInterpreterTest` 加字符串函数测试(每个一个 `@Test`):用 `VarCharVector` 输入(`UPPER('ab')→'AB'`、`LOWER`、`TRIM(' x ')→'x'`、`LENGTH('abc')→3`、`CONCAT('a','b')→'ab'`、`SUBSTRING('abc',1,2)→'ab'`),含 null 行断言 STRICT。注意 `RexInterpreterTest` 当前 input 只有 int 列 a/b,需加一个 `varcharInput()` 辅助或新造一个含 VarCharVector 的 `VectorSchemaRoot`。

- [ ] **Step 2: 跑测试确认失败** — 新函数未注册,`evalCall` 抛 `UnsupportedOperationException`。

- [ ] **Step 3: 实现** — 在 `BuiltInFunctions` 加 `stringFunctions(registry)`(注册在 `newRegistry()` 里)。UPPER/LOWER/TRIM 用 `fillUnaryString` + `String::toUpperCase`/`toLowerCase`/`trim`;LENGTH 用 `fillStringToInt` + `String::length`;CONCAT 用 `fillBinaryString` + `String::concat`。SUBSTRING 用 Tier-2 `Kernel`(3 参 + 越界/负 offset 处理):

```java
private static void stringFunctions(FunctionRegistry r) {
    unaryString(r, SqlStdOperatorTable.UPPER, String::toUpperCase);
    unaryString(r, SqlStdOperatorTable.LOWER, String::toLowerCase);
    unaryString(r, SqlStdOperatorTable.TRIM, String::trim);
    r.register(SqlStdOperatorTable.CHAR_LENGTH, new Function(SqlStdOperatorTable.CHAR_LENGTH.getName(), List.of(new Overload(
            List.of(VarCharVector.class),
            (args, out) -> Kernels.fillStringToInt((VarCharVector) args.get(0), (IntVector) out, String::length)))));
    r.register(SqlStdOperatorTable.LENGTH, /* 同上,CHAR_LENGTH 别名 */);
    binaryString(r, SqlStdOperatorTable.CONCAT, String::concat);
    r.register(SqlStdOperatorTable.SUBSTRING, new Function(SqlStdOperatorTable.SUBSTRING.getName(), List.of(new Overload(
            List.of(VarCharVector.class, IntVector.class, IntVector.class),
            BuiltInFunctions::substring))));
}

private static void substring(List<ValueVector> args, FieldVector out) {
    VarCharVector s = (VarCharVector) args.get(0);
    IntVector from = (IntVector) args.get(1);
    IntVector len = (IntVector) args.get(2);
    VarCharVector result = (VarCharVector) out;
    for (int i = 0; i < s.getValueCount(); i++) {
        if (s.isNull(i) || from.isNull(i) || len.isNull(i)) { result.setNull(i); continue; }
        String str = new String(s.get(i), StandardCharsets.UTF_8);
        int start = from.get(i) - 1;   // SQL SUBSTRING 是 1-based
        int length = len.get(i);
        String sub = slice(str, start, length);
        result.setSafe(i, sub.getBytes(StandardCharsets.UTF_8));
    }
}

/** 1-based start,负/越界裁剪到合法区间;超出则返回空串。 */
private static String slice(String s, int start, int length) {
    if (s.isEmpty() || length <= 0) return "";
    int begin = Math.max(0, start);
    int end = Math.min(s.length(), begin + length);
    if (begin >= s.length()) return "";
    return s.substring(begin, end);
}
```

（注:`SqlStdOperatorTable` 中 LENGTH 的 operator 是 `CHAR_LENGTH`/`LENGTH`;按 Calcite 实际单例名注册。helper `unaryString`/`binaryString` 仿照 Task 3/4 的 `binaryInt` 写,勿遗漏 null 处理在 `fillUnaryString` 内已做。）

- [ ] **Step 4: 跑测试确认通过** — `./mvnw.cmd test -pl minidb-server -Dtest=RexInterpreterTest`。

- [ ] **Step 5: 提交**
```bash
git add minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java
git commit -m "feat: 新增字符串函数 UPPER/LOWER/TRIM/LENGTH/CONCAT/SUBSTRING"
```

---

## Task 6: 新增数学函数(ABS/ROUND/FLOOR/CEIL)

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java`

**Interfaces:**
- Consumes: `Kernels.fillUnaryInt/Long/Double`。

- [ ] **Step 1: 写失败测试** — 加 `absInteger`、`absDouble`、`roundFloorCeil`(double 输入):`ABS(-3)→3`、`ABS(-2.5)→2.5`、`ROUND(2.567)→2.6`(注意 ROUND(double) 单参是四舍五入到整数还是保留——按 Calcite `ROUND(double)` 语义,单参返回最接近整数,断言前先确认;若单参有歧义则用 `ROUND(x, n)` 两参,但两参结果类型可能是 DECIMAL→double。建议此任务用 `FLOOR(2.7)→2.0`、`CEIL(2.1)→3.0`、`ABS` 三种无歧义的;ROUND 若结果类型是 double 且语义明确才加)。null 行 STRICT。

- [ ] **Step 2: 跑测试确认失败** — 未注册,抛异常。

- [ ] **Step 3: 实现** — 在 `BuiltInFunctions` 加 `mathFunctions(registry)`:

```java
private static void mathFunctions(FunctionRegistry r) {
    unaryInt(r, SqlStdOperatorTable.ABS, Math::abs);
    unaryLong(r, SqlStdOperatorTable.ABS, Math::abs);
    unaryDouble(r, SqlStdOperatorTable.ABS, Math::abs);
    unaryDouble(r, SqlStdOperatorTable.FLOOR, Math::floor);
    unaryDouble(r, SqlStdOperatorTable.CEIL, Math::ceil);
    // ROUND 视 Calcite 返回类型决定:若单参 ROUND(double)→double 且语义=就近取整,则 unaryDouble(r, ROUND, Math::round) 并返回 double;否则跳过并在测试里只覆盖 FLOOR/CEIL/ABS。
}
```

（helper `unaryInt`/`unaryLong`/`unaryDouble` 仿照 `binaryInt` 写,内用 `Kernels.fillUnary*`。`Math::abs` 对 int/long/double 各自重载自动匹配。）

- [ ] **Step 4: 跑测试确认通过** — 绿。

- [ ] **Step 5: 提交**
```bash
git add minidb-server/src/main/java/com/minidb/server/exec/functions/BuiltInFunctions.java minidb-server/src/test/java/com/minidb/server/exec/RexInterpreterTest.java
git commit -m "feat: 新增数学函数 ABS/FLOOR/CEIL"
```

---

## Task 7: 端到端验证 + 文档

**Files:**
- Modify: `minidb-server/src/test/java/com/minidb/server/exec/QueryExecutorTest.java`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: Task 3-6 全部。

- [ ] **Step 1: 写失败测试** — 在 `QueryExecutorTest` 加 `scalarFunctionsEndToEnd`:建表 `CREATE TABLE t (id INTEGER, name VARCHAR)`,插入含中文 + 空串的数据,查询 `SELECT UPPER(name) AS up, LENGTH(name) AS len, id + 1 AS next_id FROM t ORDER BY id`,断言结果(UPPER 中文原样、LENGTH 按字符数、`id+1`)。用 `StandardCharsets.UTF_8` 解码 VARCHAR。这一步验证函数贯穿 SQL 全链路(解析→校验→规划→列式执行)。

- [ ] **Step 2: 跑测试确认失败** — 若函数未走通会抛异常(实际 Task 5/6 已完成,此处应绿;作为端到端回归)。

- [ ] **Step 3: 跑全量** — `./mvnw.cmd test`,199+16 全绿且新增通过。

- [ ] **Step 4: 更新 CLAUDE.md** — 在 `exec/RexInterpreter` 条目追加:已重构为薄壳 + `exec/functions` 框架;`RexInterpreter` 只剩 RexInputRef/RexLiteral 求值 + AND/OR/NOT/CAST/CASE 专用 handler,算术/比较/标量函数走 `FunctionRegistry`。新增坑 47:**标量函数走 `BuiltInFunctions` 注册表(`Map<SqlOperator,Function>`),`RexInterpreter.evalCall` 的 default 分支 `functions.lookup(call.getOperator())`;加新函数 = 在 `BuiltInFunctions` 注册一个 `Function`(typed lambda + `Kernels.fill*` 或 Tier-2 `Kernel`)。`Function.evaluate` 负责按输入类型分发、用 `ArrowTypes.field(resultType,"expr").createVector(allocator)` 分配输出、STRICT null 由 `Kernels.fill*` 循环统一处理、并在 finally 关闭入参向量。**

- [ ] **Step 5: 提交**
```bash
git add minidb-server/src/test/java/com/minidb/server/exec/QueryExecutorTest.java CLAUDE.md
git commit -m "test+docs: 标量函数端到端测试 + 函数框架说明"
```

---

## 自检

- [ ] **Spec 覆盖**:函数框架(Kernel 两层 / Kernels / Function/Overload/Registry / BuiltInFunctions)、迁移算术+比较、新增字符串+数学、RexInterpreter 薄壳、STRICT null、按 SqlOperator 分发、UDF 扩展点(注册表可变 + RexInterpreter 双参构造)——全部有对应 Task。
- [ ] **无占位**:所有代码块完整,无 TBD/TODO。
- [ ] **类型一致**:`Kernel.execute(List<ValueVector>, FieldVector)`、`ScalarKernels.*`、`Kernels.fill*`、`Function.evaluate(...)`、`FunctionRegistry.lookup/register` 在各 Task 间签名一致。
- [ ] **入口不破坏**:`RexInterpreter.eval(RexNode, VectorSchemaRoot)` 未改;调用方无感。
