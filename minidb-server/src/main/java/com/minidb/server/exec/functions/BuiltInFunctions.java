package com.minidb.server.exec.functions;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** 内置标量函数:按 SqlOperator 挂到 {@link FunctionRegistry},供 {@code RexInterpreter} 分发。 */
public final class BuiltInFunctions {
    private BuiltInFunctions() {}

    public static FunctionRegistry newRegistry() {
        FunctionRegistry registry = new FunctionRegistry();
        arithmetic(registry);
        comparison(registry);
        stringFunctions(registry);
        mathFunctions(registry);
        return registry;
    }

    /** 数学函数:ABS 按输入/输出类型注册;FLOOR/CEIL/ROUND 只对 double 注册。 */
    private static void mathFunctions(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.ABS, absFunction());
        unaryDouble(r, SqlStdOperatorTable.FLOOR, Math::floor);
        unaryDouble(r, SqlStdOperatorTable.CEIL, Math::ceil);
        // ROUND(double) → double,就近取整、0.5 远离零(SQL 标准语义,非 Math.rint 的 tie-to-even)。
        unaryDouble(r, SqlStdOperatorTable.ROUND, BuiltInFunctions::roundHalfAwayFromZero);
    }

    /**
     * ABS 的三个同型重载收进同一个 {@link Function}(注册表按 SqlOperator 覆盖)。
     */
    private static Function absFunction() {
        return new Function(SqlStdOperatorTable.ABS.getName(), List.of(
                new Overload(List.of(IntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillUnaryInt(
                                (IntVector) args.get(0), (IntVector) out, Math::abs)),
                new Overload(List.of(BigIntVector.class), BigIntVector.class,
                        (args, out) -> Kernels.fillUnaryLong(
                                (BigIntVector) args.get(0), (BigIntVector) out, Math::abs)),
                new Overload(List.of(Float8Vector.class), Float8Vector.class,
                        (args, out) -> Kernels.fillUnaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) out, Math::abs))));
    }

    /** 一元双精度函数:Float8Vector → Float8Vector。 */
    private static void unaryDouble(FunctionRegistry r, SqlOperator op, ScalarKernels.DoubleUnary fn) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(Float8Vector.class), Float8Vector.class,
                (args, out) -> Kernels.fillUnaryDouble(
                        (Float8Vector) args.get(0), (Float8Vector) out, fn)))));
    }

    /** 就近取整、0.5 远离零(SQL ROUND 语义):sign(x) * floor(|x| + 0.5)。 */
    private static double roundHalfAwayFromZero(double x) {
        return Math.signum(x) * Math.floor(Math.abs(x) + 0.5);
    }

    /**
     * 字符串函数。UPPER/LOWER 走一元 String 核;CHAR_LENGTH/LENGTH 走 StringToInt 核
     * (结果 INTEGER);CONCAT 是 `||` 算符、CONCAT_FUNCTION 是 `CONCAT(a,b,...)` 变参函数;
     * SUBSTRING 是 3 参单独核。TRIM 不在此注册:其 3 参形式带 SYMBOL 标志字面量,由
     * {@code RexInterpreter} 专特殊 handler 处理。
     */
    private static void stringFunctions(FunctionRegistry r) {
        unaryString(r, SqlStdOperatorTable.UPPER, String::toUpperCase);
        unaryString(r, SqlStdOperatorTable.LOWER, String::toLowerCase);
        registerLength(r, SqlStdOperatorTable.CHAR_LENGTH);
        registerLength(r, SqlLibraryOperators.LENGTH);
        binaryString(r, SqlStdOperatorTable.CONCAT, String::concat);
        r.register(SqlLibraryOperators.CONCAT_FUNCTION, concatFunction());
        r.register(SqlStdOperatorTable.SUBSTRING, substringFunction());
    }

    /** LENGTH / CHAR_LENGTH:VarCharVector → INTEGER(IntVector),按 Unicode code point 计。 */
    private static void registerLength(FunctionRegistry r, SqlOperator op) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(VarCharVector.class), IntVector.class,
                (args, out) -> Kernels.fillStringToInt(
                        (VarCharVector) args.get(0), (IntVector) out,
                        s -> s.codePointCount(0, s.length()))))));
    }

    /** 一元字符串函数:VarCharVector → VarCharVector。 */
    private static void unaryString(FunctionRegistry r, SqlOperator op, ScalarKernels.StringUnary fn) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(VarCharVector.class), VarCharVector.class,
                (args, out) -> Kernels.fillUnaryString(
                        (VarCharVector) args.get(0), (VarCharVector) out, fn)))));
    }

    /** 二元字符串函数(如 `||`):两 VarCharVector → VarCharVector。 */
    private static void binaryString(FunctionRegistry r, SqlOperator op, ScalarKernels.StringBinary fn) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(VarCharVector.class, VarCharVector.class), VarCharVector.class,
                (args, out) -> Kernels.fillBinaryString(
                        (VarCharVector) args.get(0), (VarCharVector) args.get(1),
                        (VarCharVector) out, fn)))));
    }

    /**
     * CONCAT(arg, ...) 变参函数(1+ 参):任一参 null → 结果 null(STRICT)。注册 1/2/3 参
     * 三个常见元数,共用同一个变参核。
     */
    private static Function concatFunction() {
        Kernel kernel = BuiltInFunctions::concat;
        return new Function(SqlLibraryOperators.CONCAT_FUNCTION.getName(), List.of(
                new Overload(List.of(VarCharVector.class), VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, VarCharVector.class), VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, VarCharVector.class, VarCharVector.class),
                        VarCharVector.class, kernel)));
    }

    private static void concat(List<ValueVector> args, FieldVector out) {
        VarCharVector result = (VarCharVector) out;
        for (int i = 0; i < args.get(0).getValueCount(); i++) {
            boolean anyNull = false;
            StringBuilder sb = new StringBuilder();
            for (ValueVector arg : args) {
                if (arg.isNull(i)) { anyNull = true; break; }
                sb.append(new String(((VarCharVector) arg).get(i), StandardCharsets.UTF_8));
            }
            if (anyNull) {
                result.setNull(i);
                continue;
            }
            result.setSafe(i, sb.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * SUBSTRING(s, from, len) 的 3 参核。第二/三参是整型,但可能是 INTEGER(IntVector)或
     * BIGINT(BigIntVector)列/字面量 —— 两种类型都要能接,故各组合都注册到同一个核,核内用
     * {@link #longArg} 类型无关地读取出 long 值(再在 slice 里 clamp)。结果恒 VARCHAR(VarCharVector)。
     */
    private static Function substringFunction() {
        Kernel substringKernel = BuiltInFunctions::substring;
        return new Function(SqlStdOperatorTable.SUBSTRING.getName(), List.of(
                new Overload(List.of(VarCharVector.class, IntVector.class, IntVector.class),
                        VarCharVector.class, substringKernel),
                new Overload(List.of(VarCharVector.class, IntVector.class, BigIntVector.class),
                        VarCharVector.class, substringKernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class, IntVector.class),
                        VarCharVector.class, substringKernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class, BigIntVector.class),
                        VarCharVector.class, substringKernel)));
    }

    private static void substring(List<ValueVector> args, FieldVector out) {
        VarCharVector s = (VarCharVector) args.get(0);
        ValueVector from = args.get(1);
        ValueVector len = args.get(2);
        VarCharVector result = (VarCharVector) out;
        for (int i = 0; i < s.getValueCount(); i++) {
            if (s.isNull(i) || from.isNull(i) || len.isNull(i)) {
                result.setNull(i);
                continue;
            }
            String str = new String(s.get(i), StandardCharsets.UTF_8);
            long start = longArg(from, i) - 1; // SQL SUBSTRING 是 1-based,转成 0-based。
            long length = longArg(len, i);
            result.setSafe(i, slice(str, start, length).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 从 Int/BigInt 向量第 i 行读取 long 值(SUBSTRING 的第二/三参,不截断,交给 slice 在 long 域 clamp)。 */
    private static long longArg(ValueVector v, int i) {
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        throw new IllegalArgumentException("SUBSTRING operand is not an integer vector: " + v.getClass());
    }

    /**
     * 从 0-based start(字符位,code point)截取最多 length 个字符。start/length 用 long 传入、
     * 在本方法内 clamp 到 [0, codePointCount],避免超大字面量在 int 截断时回绕;负 start 裁剪到 0,
     * 越界或 length<=0 返回空串。按 Unicode code point 截取(非 UTF-16 code unit)。
     */
    private static String slice(String s, long start, long length) {
        if (s.isEmpty() || length <= 0) {
            return "";
        }
        int total = s.codePointCount(0, s.length());
        long begin = Math.max(0, start);
        if (begin >= total) {
            return "";
        }
        long end = Math.min(total, begin + length);
        int[] codePoints = s.codePoints().skip((int) begin).limit(end - begin).toArray();
        return new String(codePoints, 0, codePoints.length);
    }

    private static void comparison(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.EQUALS,
                comparisonFunction(SqlStdOperatorTable.EQUALS, SqlKind.EQUALS));
        r.register(SqlStdOperatorTable.NOT_EQUALS,
                comparisonFunction(SqlStdOperatorTable.NOT_EQUALS, SqlKind.NOT_EQUALS));
        r.register(SqlStdOperatorTable.LESS_THAN,
                comparisonFunction(SqlStdOperatorTable.LESS_THAN, SqlKind.LESS_THAN));
        r.register(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                comparisonFunction(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL));
        r.register(SqlStdOperatorTable.GREATER_THAN,
                comparisonFunction(SqlStdOperatorTable.GREATER_THAN, SqlKind.GREATER_THAN));
        r.register(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                comparisonFunction(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL));
    }

    /**
     * 单个比较运算符的所有重载合成一个 {@link Function}(注册表按 SqlOperator 覆盖)。结果恒
     * BOOLEAN(BitVector);同型 Int/Long/Double 各走对应核,字符串走 String 核。真混型
     * INTEGER/BIGINT(如 `int_col > bigint_col`)Calcite 不强制 CAST,需 [Int,BigInt]/[BigInt,Int]
     * 跨型重载(int 侧 promote 到 long)。
     */
    private static Function comparisonFunction(SqlOperator op, SqlKind kind) {
        return new Function(op.getName(), List.of(
                new Overload(List.of(IntVector.class, IntVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareInt(
                                (IntVector) args.get(0), (IntVector) args.get(1),
                                (BitVector) out, Integer::compare, kind)),
                new Overload(List.of(BigIntVector.class, BigIntVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareLong(
                                (BigIntVector) args.get(0), (BigIntVector) args.get(1),
                                (BitVector) out, Long::compare, kind)),
                new Overload(List.of(Float8Vector.class, Float8Vector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareDouble(
                                (Float8Vector) args.get(0), (Float8Vector) args.get(1),
                                (BitVector) out, Double::compare, kind)),
                new Overload(List.of(VarCharVector.class, VarCharVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareString(
                                (VarCharVector) args.get(0), (VarCharVector) args.get(1),
                                (BitVector) out, String::compareTo, kind)),
                new Overload(List.of(IntVector.class, BigIntVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareIntLong(
                                (IntVector) args.get(0), (BigIntVector) args.get(1),
                                (BitVector) out, Long::compare, kind)),
                new Overload(List.of(BigIntVector.class, IntVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareLongInt(
                                (BigIntVector) args.get(0), (IntVector) args.get(1),
                                (BitVector) out, Long::compare, kind))));
    }

    private static void arithmetic(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.PLUS, arithmeticFunction(SqlStdOperatorTable.PLUS));
        r.register(SqlStdOperatorTable.MINUS, arithmeticFunction(SqlStdOperatorTable.MINUS));
        r.register(SqlStdOperatorTable.MULTIPLY, arithmeticFunction(SqlStdOperatorTable.MULTIPLY));
        r.register(SqlStdOperatorTable.DIVIDE, arithmeticFunction(SqlStdOperatorTable.DIVIDE));
    }

    /**
     * 单个算术运算符的所有重载合成一个 {@link Function}。每个 SqlOperator 在注册表里只对应
     * 一个 Function({@code register} 会覆盖)。真混型 INTEGER/BIGINT(如 `int_col + bigint_col`)
     * Calcite **不**强制 CAST,操作数保持 Int+BigInt 混型、结果 BIGINT,故需跨型重载(输出
     * BigIntVector,int 侧 promote 到 long)。整数字面量经 RexInterpreter 已是 IntVector(不再
     * 产 BigIntVector),故不再有「字面量坑」的 Int 结果跨型。
     */
    private static Function arithmeticFunction(SqlOperator op) {
        ScalarKernels.IntBinary intOp = intKernel(op);
        ScalarKernels.LongBinary longOp = longKernel(op);
        ScalarKernels.DoubleBinary doubleOp = doubleKernel(op);
        return new Function(op.getName(), List.of(
                new Overload(List.of(IntVector.class, IntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillBinaryInt(
                                (IntVector) args.get(0), (IntVector) args.get(1), (IntVector) out, intOp)),
                new Overload(List.of(BigIntVector.class, BigIntVector.class), BigIntVector.class,
                        (args, out) -> Kernels.fillBinaryLong(
                                (BigIntVector) args.get(0), (BigIntVector) args.get(1), (BigIntVector) out, longOp)),
                new Overload(List.of(Float8Vector.class, Float8Vector.class), Float8Vector.class,
                        (args, out) -> Kernels.fillBinaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) args.get(1), (Float8Vector) out, doubleOp)),
                new Overload(List.of(IntVector.class, BigIntVector.class), BigIntVector.class,
                        (args, out) -> fillIntLongBinary(
                                (IntVector) args.get(0), (BigIntVector) args.get(1), (BigIntVector) out, longOp)),
                new Overload(List.of(BigIntVector.class, IntVector.class), BigIntVector.class,
                        (args, out) -> fillLongIntBinary(
                                (BigIntVector) args.get(0), (IntVector) args.get(1), (BigIntVector) out, longOp))));
    }

    private static ScalarKernels.IntBinary intKernel(SqlOperator op) {
        if (op == SqlStdOperatorTable.PLUS) {
            return Integer::sum;
        }
        if (op == SqlStdOperatorTable.MINUS) {
            return (a, b) -> a - b;
        }
        if (op == SqlStdOperatorTable.MULTIPLY) {
            return (a, b) -> a * b;
        }
        return (a, b) -> { if (b == 0) throw new ArithmeticException("division by zero"); return a / b; };
    }

    private static ScalarKernels.LongBinary longKernel(SqlOperator op) {
        if (op == SqlStdOperatorTable.PLUS) {
            return Long::sum;
        }
        if (op == SqlStdOperatorTable.MINUS) {
            return (a, b) -> a - b;
        }
        if (op == SqlStdOperatorTable.MULTIPLY) {
            return (a, b) -> a * b;
        }
        return (a, b) -> { if (b == 0) throw new ArithmeticException("division by zero"); return a / b; };
    }

    private static ScalarKernels.DoubleBinary doubleKernel(SqlOperator op) {
        if (op == SqlStdOperatorTable.PLUS) {
            return Double::sum;
        }
        if (op == SqlStdOperatorTable.MINUS) {
            return (a, b) -> a - b;
        }
        if (op == SqlStdOperatorTable.MULTIPLY) {
            return (a, b) -> a * b;
        }
        return (a, b) -> { if (b == 0) throw new ArithmeticException("division by zero"); return a / b; };
    }

    /** 跨型算术(INTEGER 列 vs BIGINT 列):int 侧 promote 到 long,结果写 BigIntVector。 */
    private static void fillIntLongBinary(IntVector left, BigIntVector right, BigIntVector out,
                                          ScalarKernels.LongBinary op) {
        for (int i = 0; i < left.getValueCount(); i++) {
            if (left.isNull(i) || right.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(left.get(i), right.get(i)));
        }
    }

    private static void fillLongIntBinary(BigIntVector left, IntVector right, BigIntVector out,
                                          ScalarKernels.LongBinary op) {
        for (int i = 0; i < left.getValueCount(); i++) {
            if (left.isNull(i) || right.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(left.get(i), right.get(i)));
        }
    }
}
