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

    /**
     * 数学函数:ABS 按输入类型注册 int/long/double 三个同型重载({@code Math::abs} 对每种原语
     * 各自重载,由 ScalarKernels.IntUnary/LongUnary/DoubleUnary 的函数型自动选中对应版本);FLOOR/
     * CEIL 只对 double 注册(输出恒 double)。ROUND 不注册:其返回类型在 DECIMAL→double 的映射下
     * 含糊,超出本任务范围。
     */
    private static void mathFunctions(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.ABS, absFunction());
        unaryDouble(r, SqlStdOperatorTable.FLOOR, Math::floor);
        unaryDouble(r, SqlStdOperatorTable.CEIL, Math::ceil);
    }

    /**
     * ABS 的三个同型重载必须收进同一个 {@link Function}:{@link FunctionRegistry#register} 按
     * SqlOperator 覆盖,逐个 register 会互相覆盖,只剩最后注册的重载(同算术/比较的合成模式)。
     */
    private static Function absFunction() {
        return new Function(SqlStdOperatorTable.ABS.getName(), List.of(
                new Overload(List.of(IntVector.class),
                        (args, out) -> Kernels.fillUnaryInt(
                                (IntVector) args.get(0), (IntVector) out, Math::abs)),
                new Overload(List.of(BigIntVector.class),
                        (args, out) -> Kernels.fillUnaryLong(
                                (BigIntVector) args.get(0), (BigIntVector) out, Math::abs)),
                new Overload(List.of(Float8Vector.class),
                        (args, out) -> Kernels.fillUnaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) out, Math::abs))));
    }

    /** 一元双精度函数:Float8Vector → Float8Vector,结果类型由 call.getType() 决定(DOUBLE)。 */
    private static void unaryDouble(FunctionRegistry r, SqlOperator op, ScalarKernels.DoubleUnary fn) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(Float8Vector.class),
                (args, out) -> Kernels.fillUnaryDouble(
                        (Float8Vector) args.get(0), (Float8Vector) out, fn)))));
    }

    /**
     * 字符串函数:UPPER/LOWER/TRIM 走一元 String 核(STRICT,null 由 Kernels 内做);LENGTH 走
     * StringToInt 核(结果 INTEGER);CONCAT 走二元 String 核;SUBSTRING 是 3 参,单独 Tier-2 核。
     * LENGTH 在 Calcite 里是 CHAR_LENGTH 的别名(无独立的 SqlStdOperatorTable.LENGTH 单例),只注册
     * {@link SqlStdOperatorTable#CHAR_LENGTH}。
     */
    private static void stringFunctions(FunctionRegistry r) {
        unaryString(r, SqlStdOperatorTable.UPPER, String::toUpperCase);
        unaryString(r, SqlStdOperatorTable.LOWER, String::toLowerCase);
        unaryString(r, SqlStdOperatorTable.TRIM, String::trim);
        r.register(SqlStdOperatorTable.CHAR_LENGTH,
                new Function(SqlStdOperatorTable.CHAR_LENGTH.getName(), List.of(new Overload(
                        List.of(VarCharVector.class),
                        (args, out) -> Kernels.fillStringToInt(
                                (VarCharVector) args.get(0), (IntVector) out, String::length)))));
        binaryString(r, SqlStdOperatorTable.CONCAT, String::concat);
        r.register(SqlStdOperatorTable.SUBSTRING, substringFunction());
    }

    /** 一元字符串函数:VarCharVector → VarCharVector,结果类型由 call.getType() 决定(VARCHAR)。 */
    private static void unaryString(FunctionRegistry r, SqlOperator op, ScalarKernels.StringUnary fn) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(VarCharVector.class),
                (args, out) -> Kernels.fillUnaryString(
                        (VarCharVector) args.get(0), (VarCharVector) out, fn)))));
    }

    /** 二元字符串函数:两 VarCharVector → VarCharVector(如 CONCAT)。 */
    private static void binaryString(FunctionRegistry r, SqlOperator op, ScalarKernels.StringBinary fn) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(VarCharVector.class, VarCharVector.class),
                (args, out) -> Kernels.fillBinaryString(
                        (VarCharVector) args.get(0), (VarCharVector) args.get(1),
                        (VarCharVector) out, fn)))));
    }

    /**
     * SUBSTRING(s, from, len) 的 3 参核。第二/三参在真实 SQL 里是整型:字面量恒产 BigIntVector
     * (坑 #23),而 INTEGER 列产 IntVector —— 两种类型都要能接,故各组合都注册到同一个核,核内用
     * {@link #intArg} 类型无关地读取出 int 值。
     */
    private static Function substringFunction() {
        Kernel substringKernel = BuiltInFunctions::substring;
        return new Function(SqlStdOperatorTable.SUBSTRING.getName(), List.of(
                new Overload(List.of(VarCharVector.class, IntVector.class, IntVector.class), substringKernel),
                new Overload(List.of(VarCharVector.class, IntVector.class, BigIntVector.class), substringKernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class, IntVector.class), substringKernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class, BigIntVector.class), substringKernel)));
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
            int start = intArg(from, i) - 1; // SQL SUBSTRING 是 1-based,转成 0-based。
            int length = intArg(len, i);
            result.setSafe(i, slice(str, start, length).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 从 Int/BigInt 向量第 i 行读取 int 值(SUBSTRING 的第二/三参)。 */
    private static int intArg(ValueVector v, int i) {
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return (int) bv.get(i);
        }
        throw new IllegalArgumentException("SUBSTRING operand is not an integer vector: " + v.getClass());
    }

    /** 从 0-based start 截取最多 length 个字符;负 start 裁剪到 0,越界或 length<=0 返回空串。 */
    private static String slice(String s, int start, int length) {
        if (s.isEmpty() || length <= 0) {
            return "";
        }
        int begin = Math.max(0, start);
        if (begin >= s.length()) {
            return "";
        }
        int end = Math.min(s.length(), begin + length);
        return s.substring(begin, end);
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
     * 单个比较运算符的所有重载合成一个 {@link Function}(注册表按 SqlOperator 覆盖,同算术)。
     * 比较恒在数值域:同型 Int/Long/Double 各走对应核,字符串走 String 核;整数字面量恒产
     * BigIntVector(坑 #23)而 INTEGER 列是 IntVector,故注册 [Int,BigInt]/[BigInt,Int] 两个跨型
     * 重载,promote int 到 long 后按 Long 比较。结果类型恒 BOOLEAN(由 Function.evaluate 按
     * call.getType() 分配 BitVector)。
     */
    private static Function comparisonFunction(SqlOperator op, SqlKind kind) {
        return new Function(op.getName(), List.of(
                new Overload(List.of(IntVector.class, IntVector.class),
                        (args, out) -> Kernels.fillCompareInt(
                                (IntVector) args.get(0), (IntVector) args.get(1),
                                (BitVector) out, Integer::compare, kind)),
                new Overload(List.of(BigIntVector.class, BigIntVector.class),
                        (args, out) -> Kernels.fillCompareLong(
                                (BigIntVector) args.get(0), (BigIntVector) args.get(1),
                                (BitVector) out, Long::compare, kind)),
                new Overload(List.of(Float8Vector.class, Float8Vector.class),
                        (args, out) -> Kernels.fillCompareDouble(
                                (Float8Vector) args.get(0), (Float8Vector) args.get(1),
                                (BitVector) out, Double::compare, kind)),
                new Overload(List.of(VarCharVector.class, VarCharVector.class),
                        (args, out) -> Kernels.fillCompareString(
                                (VarCharVector) args.get(0), (VarCharVector) args.get(1),
                                (BitVector) out, String::compareTo, kind)),
                new Overload(List.of(IntVector.class, BigIntVector.class),
                        (args, out) -> Kernels.fillCompareIntLong(
                                (IntVector) args.get(0), (BigIntVector) args.get(1),
                                (BitVector) out, Long::compare, kind)),
                new Overload(List.of(BigIntVector.class, IntVector.class),
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
     * 一个 Function({@code register} 会覆盖),故同型(Int/Long/Double)与跨型重载必须收进同一
     * 个 overload 列表,而不是逐个 register 成独立 Function。
     */
    private static Function arithmeticFunction(SqlOperator op) {
        ScalarKernels.IntBinary intOp = intKernel(op);
        ScalarKernels.LongBinary longOp = longKernel(op);
        ScalarKernels.DoubleBinary doubleOp = doubleKernel(op);
        return new Function(op.getName(), List.of(
                new Overload(List.of(IntVector.class, IntVector.class),
                        (args, out) -> Kernels.fillBinaryInt(
                                (IntVector) args.get(0), (IntVector) args.get(1), (IntVector) out, intOp)),
                new Overload(List.of(BigIntVector.class, BigIntVector.class),
                        (args, out) -> Kernels.fillBinaryLong(
                                (BigIntVector) args.get(0), (BigIntVector) args.get(1), (BigIntVector) out, longOp)),
                new Overload(List.of(Float8Vector.class, Float8Vector.class),
                        (args, out) -> Kernels.fillBinaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) args.get(1), (Float8Vector) out, doubleOp)),
                // 整数字面量经 RexInterpreter.literalVector 恒产 BigIntVector(坑 #23),而 INTEGER
                // 列经 RowCopier.copyVector 产 IntVector —— 二者混算(如 `id + 1`、`id * 2`)没有任何
                // 同型重载匹配,必须注册跨型重载。输出硬编码 IntVector 是安全的:真正混型
                // INTEGER/BIGINT 的操作数 Calcite 会在算子前 CAST 成 BIGINT,故跨型只由「字面量
                // 是 BigIntVector 但 Calcite 类型仍是 INTEGER」的坑触发,结果类型恒为 INTEGER。
                // 两种操作数顺序都注册,且各自按真实操作数顺序计算(非交换的 MINUS/DIVIDE 不能颠倒)。
                new Overload(List.of(IntVector.class, BigIntVector.class),
                        (args, out) -> fillIntLongBinary(
                                (IntVector) args.get(0), (BigIntVector) args.get(1), (IntVector) out, longOp)),
                new Overload(List.of(BigIntVector.class, IntVector.class),
                        (args, out) -> fillLongIntBinary(
                                (BigIntVector) args.get(0), (IntVector) args.get(1), (IntVector) out, longOp))));
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

    private static void fillIntLongBinary(IntVector left, BigIntVector right, IntVector out,
                                          ScalarKernels.LongBinary op) {
        for (int i = 0; i < left.getValueCount(); i++) {
            if (left.isNull(i) || right.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, (int) op.apply(left.get(i), right.get(i)));
        }
    }

    private static void fillLongIntBinary(BigIntVector left, IntVector right, IntVector out,
                                          ScalarKernels.LongBinary op) {
        for (int i = 0; i < left.getValueCount(); i++) {
            if (left.isNull(i) || right.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, (int) op.apply(left.get(i), right.get(i)));
        }
    }
}
