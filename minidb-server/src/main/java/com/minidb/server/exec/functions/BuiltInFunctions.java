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
        // ROUND(double) → double,就近取整(tie 到偶数);MiniDB 的 DECIMAL 坍缩成 double,
        // 故返回 double 与类型边界一致。
        unaryDouble(r, SqlStdOperatorTable.ROUND, Math::rint);
    }

    /**
     * ABS 的四个重载收进同一个 {@link Function}(注册表按 SqlOperator 覆盖)。
     * 整数字面量恒产 BigIntVector(坑 #23)但结果类型仍是 INTEGER → IntVector,故
     * [BigIntVector] 输入按输出类型拆成两个重载:→IntVector(字面量,long 域算完截断)
     * 与 →BigIntVector(BIGINT 列)。输出类型参与分发后无需再在核内 instanceof 分支。
     */
    private static Function absFunction() {
        return new Function(SqlStdOperatorTable.ABS.getName(), List.of(
                new Overload(List.of(IntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillUnaryInt(
                                (IntVector) args.get(0), (IntVector) out, Math::abs)),
                new Overload(List.of(BigIntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillUnaryLongToInt(
                                (BigIntVector) args.get(0), (IntVector) out, Math::abs)),
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

    /** LENGTH / CHAR_LENGTH:VarCharVector → INTEGER(IntVector)。 */
    private static void registerLength(FunctionRegistry r, SqlOperator op) {
        r.register(op, new Function(op.getName(), List.of(new Overload(
                List.of(VarCharVector.class), IntVector.class,
                (args, out) -> Kernels.fillStringToInt(
                        (VarCharVector) args.get(0), (IntVector) out, String::length)))));
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
     * SUBSTRING(s, from, len) 的 3 参核。第二/三参在真实 SQL 里是整型:字面量恒产 BigIntVector
     * (坑 #23),而 INTEGER 列产 IntVector —— 两种类型都要能接,故各组合都注册到同一个核,核内用
     * {@link #intArg} 类型无关地读取出 int 值。结果恒 VARCHAR(VarCharVector)。
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
     * 单个比较运算符的所有重载合成一个 {@link Function}(注册表按 SqlOperator 覆盖)。结果恒
     * BOOLEAN(BitVector);同型 Int/Long/Double 各走对应核,字符串走 String 核;整数字面量恒产
     * BigIntVector(坑 #23)而 INTEGER 列是 IntVector,故注册 [Int,BigInt]/[BigInt,Int] 两个跨型
     * 重载,promote int 到 long 后按 Long 比较。
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
     * 一个 Function({@code register} 会覆盖),故同型(Int/Long/Double)与跨型重载必须收进同一
     * 个 overload 列表。跨型输出恒 IntVector(整数字面量坑,结果类型 INTEGER)。
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
                // 整数字面量经 RexInterpreter.literalVector 恒产 BigIntVector(坑 #23),而 INTEGER
                // 列经 RowCopier.copyVector 产 IntVector —— 二者混算(如 `id + 1`)没有同型重载,
                // 必须注册跨型重载。输出硬编码 IntVector:真正混型 INTEGER/BIGINT 的操作数
                // Calcite 会在算子前 CAST 成 BIGINT,故跨型只由「字面量是 BigIntVector 但 Calcite
                // 类型仍是 INTEGER」的坑触发,结果类型恒 INTEGER。
                new Overload(List.of(IntVector.class, BigIntVector.class), IntVector.class,
                        (args, out) -> fillIntLongBinary(
                                (IntVector) args.get(0), (BigIntVector) args.get(1), (IntVector) out, longOp)),
                new Overload(List.of(BigIntVector.class, IntVector.class), IntVector.class,
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
