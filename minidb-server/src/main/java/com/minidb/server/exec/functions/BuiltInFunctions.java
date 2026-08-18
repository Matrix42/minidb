package com.minidb.server.exec.functions;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
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
        likeFunctions(registry);
        currentTimeFunctions(registry);
        mathFunctions(registry);
        return registry;
    }

    /** 数学函数:ABS 按输入/输出类型注册;FLOOR/CEIL/ROUND 对 double 与 decimal 注册。 */
    private static void mathFunctions(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.ABS, absFunction());
        // ROUND(double) → double,就近取整、0.5 远离零(SQL 标准语义,非 Math.rint 的 tie-to-even)。
        unaryDoubleOrDecimal(r, SqlStdOperatorTable.FLOOR, Math::floor,
                d -> d.setScale(0, RoundingMode.FLOOR));
        unaryDoubleOrDecimal(r, SqlStdOperatorTable.CEIL, Math::ceil,
                d -> d.setScale(0, RoundingMode.CEILING));
        r.register(SqlStdOperatorTable.ROUND, roundFunction());
    }

    /** ROUND(x) 与 ROUND(x, n):单参取整、两参保留 n 位小数(0.5 远离零)。 */
    private static Function roundFunction() {
        return new Function(SqlStdOperatorTable.ROUND.getName(), List.of(
                new Overload(List.of(Float8Vector.class), Float8Vector.class,
                        (args, out) -> Kernels.fillUnaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) out,
                                BuiltInFunctions::roundHalfAwayFromZero)),
                new Overload(List.of(DecimalVector.class), DecimalVector.class,
                        (args, out) -> Kernels.fillUnaryDecimal(
                                (DecimalVector) args.get(0), (DecimalVector) out,
                                d -> d.setScale(0, RoundingMode.HALF_UP))),
                new Overload(List.of(Float8Vector.class, IntVector.class), Float8Vector.class,
                        (args, out) -> {
                            Float8Vector in = (Float8Vector) args.get(0);
                            IntVector n = (IntVector) args.get(1);
                            Float8Vector o = (Float8Vector) out;
                            for (int i = 0; i < in.getValueCount(); i++) {
                                if (in.isNull(i) || n.isNull(i)) {
                                    o.setNull(i);
                                    continue;
                                }
                                double factor = Math.pow(10, n.get(i));
                                double scaled = in.get(i) * factor;
                                o.setSafe(i, Math.signum(scaled)
                                        * Math.floor(Math.abs(scaled) + 0.5) / factor);
                            }
                        }),
                new Overload(List.of(DecimalVector.class, IntVector.class), DecimalVector.class,
                        (args, out) -> {
                            DecimalVector in = (DecimalVector) args.get(0);
                            IntVector n = (IntVector) args.get(1);
                            DecimalVector o = (DecimalVector) out;
                            for (int i = 0; i < in.getValueCount(); i++) {
                                if (in.isNull(i) || n.isNull(i)) {
                                    o.setNull(i);
                                    continue;
                                }
                                o.setSafe(i, Kernels.scaleTo(o,
                                        in.getObject(i).setScale(n.get(i), RoundingMode.HALF_UP)));
                            }
                        }),
                // 整数 ROUND(x, n):n>=0 无小数部分、原样返回;n<0 四舍五入到 10^|n| 位。
                new Overload(List.of(IntVector.class, IntVector.class), IntVector.class,
                        (args, out) -> roundIntKernel(
                                (IntVector) args.get(0), (IntVector) args.get(1), (IntVector) out)),
                new Overload(List.of(BigIntVector.class, IntVector.class), BigIntVector.class,
                        (args, out) -> roundLongKernel(
                                (BigIntVector) args.get(0), (IntVector) args.get(1), (BigIntVector) out))));
    }

    private static void roundIntKernel(IntVector in, IntVector n, IntVector out) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i) || n.isNull(i)) {
                out.setNull(i);
                continue;
            }
            out.setSafe(i, (int) roundIntegral(in.get(i), n.get(i)));
        }
    }

    private static void roundLongKernel(BigIntVector in, IntVector n, BigIntVector out) {
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i) || n.isNull(i)) {
                out.setNull(i);
                continue;
            }
            out.setSafe(i, roundIntegral(in.get(i), n.get(i)));
        }
    }

    /** 整数 x 按 scale 四舍五入:scale>=0 原样返回,scale<0 舍入到 10^|scale| 位。 */
    private static long roundIntegral(long x, int scale) {
        if (scale >= 0) {
            return x;
        }
        long factor = 1;
        for (int k = 0; k < -scale; k++) {
            factor *= 10;
        }
        return Math.round(x / (double) factor) * factor;
    }

    /**
     * ABS 的同型重载收进同一个 {@link Function}(注册表按 SqlOperator 覆盖)。整数/浮点/定点
     * 各走对应域:short/int/long/float/double 用原语 Math.abs,BigDecimal 用 {@link BigDecimal#abs}。
     */
    private static Function absFunction() {
        return new Function(SqlStdOperatorTable.ABS.getName(), List.of(
                new Overload(List.of(SmallIntVector.class), SmallIntVector.class,
                        (args, out) -> Kernels.fillUnaryShort(
                                (SmallIntVector) args.get(0), (SmallIntVector) out,
                                v -> (short) Math.abs(v))),
                new Overload(List.of(IntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillUnaryInt(
                                (IntVector) args.get(0), (IntVector) out, Math::abs)),
                new Overload(List.of(BigIntVector.class), BigIntVector.class,
                        (args, out) -> Kernels.fillUnaryLong(
                                (BigIntVector) args.get(0), (BigIntVector) out, Math::abs)),
                new Overload(List.of(Float4Vector.class), Float4Vector.class,
                        (args, out) -> Kernels.fillUnaryFloat(
                                (Float4Vector) args.get(0), (Float4Vector) out, Math::abs)),
                new Overload(List.of(Float8Vector.class), Float8Vector.class,
                        (args, out) -> Kernels.fillUnaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) out, Math::abs)),
                new Overload(List.of(DecimalVector.class), DecimalVector.class,
                        (args, out) -> Kernels.fillUnaryDecimal(
                                (DecimalVector) args.get(0), (DecimalVector) out, BigDecimal::abs))));
    }

    /**
     * 双精度 + 定点两种重载合成一个 {@link Function}(注册表按 SqlOperator 覆盖,不能分开 register)。
     * double 走 Math 域、DECIMAL 走 BigDecimal 域,输出类型随输入类型。
     */
    private static void unaryDoubleOrDecimal(FunctionRegistry r, SqlOperator op,
                                             ScalarKernels.DoubleUnary doubleFn,
                                             ScalarKernels.DecimalUnary decimalFn) {
        r.register(op, new Function(op.getName(), List.of(
                new Overload(List.of(Float8Vector.class), Float8Vector.class,
                        (args, out) -> Kernels.fillUnaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) out, doubleFn)),
                new Overload(List.of(DecimalVector.class), DecimalVector.class,
                        (args, out) -> Kernels.fillUnaryDecimal(
                                (DecimalVector) args.get(0), (DecimalVector) out, decimalFn)))));
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
        // 查找/替换:POSITION(sub IN str) 是标准 SQL,REPLACE 全量替换。
        r.register(SqlStdOperatorTable.POSITION, positionFunction());
        r.register(SqlStdOperatorTable.REPLACE, replaceFunction());
        // 截取/重复/反转(LEFT/RIGHT/REPEAT 属 MYSQL+POSTGRESQL,REVERSE 属 MYSQL)。
        r.register(SqlLibraryOperators.LEFT, leftRightFunction(SqlLibraryOperators.LEFT, true));
        r.register(SqlLibraryOperators.RIGHT, leftRightFunction(SqlLibraryOperators.RIGHT, false));
        r.register(SqlLibraryOperators.REPEAT, repeatFunction());
        unaryString(r, SqlLibraryOperators.REVERSE, BuiltInFunctions::reverseString);
        // 填充(LPAD/RPAD 2 或 3 参:默认 pad 为单空格)。
        r.register(SqlLibraryOperators.LPAD, padFunction(SqlLibraryOperators.LPAD, true));
        r.register(SqlLibraryOperators.RPAD, padFunction(SqlLibraryOperators.RPAD, false));
        // 大小写/字符码(INITCAP/ASCII 是标准,CHR 是 POSTGRESQL)。
        unaryString(r, SqlStdOperatorTable.INITCAP, BuiltInFunctions::initcap);
        r.register(SqlStdOperatorTable.ASCII, asciiFunction());
        r.register(SqlLibraryOperators.CHR, chrFunction());
        // 按分隔符取段(SPLIT_PART,POSTGRESQL)。
        r.register(SqlLibraryOperators.SPLIT_PART, splitPartFunction());
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

    /** POSITION(sub IN str):子串在串中的 1-based 字符(code point)位置,未找到返回 0,空子串返回 1。 */
    private static Function positionFunction() {
        return new Function(SqlStdOperatorTable.POSITION.getName(), List.of(new Overload(
                List.of(VarCharVector.class, VarCharVector.class), IntVector.class,
                (args, out) -> {
                    VarCharVector sub = (VarCharVector) args.get(0);
                    VarCharVector str = (VarCharVector) args.get(1);
                    IntVector result = (IntVector) out;
                    for (int i = 0; i < str.getValueCount(); i++) {
                        if (sub.isNull(i) || str.isNull(i)) {
                            result.setNull(i);
                            continue;
                        }
                        String needle = new String(sub.get(i), StandardCharsets.UTF_8);
                        String hay = new String(str.get(i), StandardCharsets.UTF_8);
                        int utf16 = hay.indexOf(needle);
                        result.setSafe(i, utf16 < 0 ? 0 : hay.codePointCount(0, utf16) + 1);
                    }
                })));
    }

    /** REPLACE(str, from, to):全量替换字面 from 为 to;from 为空串返回原串(MySQL/PostgreSQL 语义)。 */
    private static Function replaceFunction() {
        return new Function(SqlStdOperatorTable.REPLACE.getName(), List.of(new Overload(
                List.of(VarCharVector.class, VarCharVector.class, VarCharVector.class), VarCharVector.class,
                (args, out) -> {
                    VarCharVector str = (VarCharVector) args.get(0);
                    VarCharVector from = (VarCharVector) args.get(1);
                    VarCharVector to = (VarCharVector) args.get(2);
                    VarCharVector result = (VarCharVector) out;
                    for (int i = 0; i < str.getValueCount(); i++) {
                        if (str.isNull(i) || from.isNull(i) || to.isNull(i)) {
                            result.setNull(i);
                            continue;
                        }
                        String s = new String(str.get(i), StandardCharsets.UTF_8);
                        String f = new String(from.get(i), StandardCharsets.UTF_8);
                        String t = new String(to.get(i), StandardCharsets.UTF_8);
                        result.setSafe(i, (f.isEmpty() ? s : s.replace(f, t))
                                .getBytes(StandardCharsets.UTF_8));
                    }
                })));
    }

    /** LEFT(str, n) / RIGHT(str, n):取左/右 n 个字符(code point),n<=0 空串、n>=长度整串。 */
    private static Function leftRightFunction(SqlOperator op, boolean left) {
        Kernel kernel = (args, out) -> leftRight(args, out, left);
        return new Function(op.getName(), List.of(
                new Overload(List.of(VarCharVector.class, IntVector.class), VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class), VarCharVector.class, kernel)));
    }

    private static void leftRight(List<ValueVector> args, FieldVector out, boolean left) {
        VarCharVector str = (VarCharVector) args.get(0);
        ValueVector n = args.get(1);
        VarCharVector result = (VarCharVector) out;
        for (int i = 0; i < str.getValueCount(); i++) {
            if (str.isNull(i) || n.isNull(i)) {
                result.setNull(i);
                continue;
            }
            String s = new String(str.get(i), StandardCharsets.UTF_8);
            int[] cp = s.codePoints().toArray();
            int count = (int) Math.min(Math.max(longArg(n, i), 0), cp.length);
            result.setSafe(i, (left
                    ? new String(cp, 0, count)
                    : new String(cp, cp.length - count, count)).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** REPEAT(str, n):串重复 n 次,n<=0 空串。 */
    private static Function repeatFunction() {
        Kernel kernel = (args, out) -> repeat(args, out);
        return new Function(SqlLibraryOperators.REPEAT.getName(), List.of(
                new Overload(List.of(VarCharVector.class, IntVector.class), VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class), VarCharVector.class, kernel)));
    }

    private static void repeat(List<ValueVector> args, FieldVector out) {
        VarCharVector str = (VarCharVector) args.get(0);
        ValueVector n = args.get(1);
        VarCharVector result = (VarCharVector) out;
        for (int i = 0; i < str.getValueCount(); i++) {
            if (str.isNull(i) || n.isNull(i)) {
                result.setNull(i);
                continue;
            }
            String s = new String(str.get(i), StandardCharsets.UTF_8);
            long times = longArg(n, i);
            result.setSafe(i, (times <= 0 ? "" : s.repeat((int) Math.min(times, Integer.MAX_VALUE)))
                    .getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 按 Unicode code point 反转(非 UTF-16 code unit),多字节字符不被拆坏。 */
    private static String reverseString(String s) {
        int[] cp = s.codePoints().toArray();
        for (int i = 0, j = cp.length - 1; i < j; i++, j--) {
            int tmp = cp[i];
            cp[i] = cp[j];
            cp[j] = tmp;
        }
        return new String(cp, 0, cp.length);
    }

    /** INITCAP:每个字母数字词的词首大写、其余小写(词边界为非字母数字字符)。 */
    private static String initcap(String s) {
        int[] cp = s.codePoints().toArray();
        StringBuilder sb = new StringBuilder(cp.length);
        boolean capitalize = true;
        for (int code : cp) {
            if (Character.isLetterOrDigit(code)) {
                sb.appendCodePoint(capitalize ? Character.toUpperCase(code) : Character.toLowerCase(code));
                capitalize = false;
            } else {
                sb.appendCodePoint(code);
                capitalize = true;
            }
        }
        return sb.toString();
    }

    /** LPAD(str, n [, pad]) / RPAD(str, n [, pad]):填到 n 字符,pad 默认单空格。 */
    private static Function padFunction(SqlOperator op, boolean left) {
        Kernel kernel = (args, out) -> pad(args, out, left);
        return new Function(op.getName(), List.of(
                new Overload(List.of(VarCharVector.class, IntVector.class), VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class), VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, IntVector.class, VarCharVector.class),
                        VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, BigIntVector.class, VarCharVector.class),
                        VarCharVector.class, kernel)));
    }

    private static void pad(List<ValueVector> args, FieldVector out, boolean left) {
        VarCharVector str = (VarCharVector) args.get(0);
        ValueVector n = args.get(1);
        VarCharVector padStr = args.size() > 2 ? (VarCharVector) args.get(2) : null;
        VarCharVector result = (VarCharVector) out;
        for (int i = 0; i < str.getValueCount(); i++) {
            if (str.isNull(i) || n.isNull(i) || (padStr != null && padStr.isNull(i))) {
                result.setNull(i);
                continue;
            }
            String s = new String(str.get(i), StandardCharsets.UTF_8);
            long len = longArg(n, i);
            String pad = padStr == null ? " " : new String(padStr.get(i), StandardCharsets.UTF_8);
            result.setSafe(i, pad(s, len, pad, left).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 填/截到 target 个 code point。pad 为空串且需填充时返回空串(MySQL/PostgreSQL 语义)。 */
    private static String pad(String s, long target, String pad, boolean left) {
        int[] cp = s.codePoints().toArray();
        int len = (int) Math.max(target, 0);
        if (len <= cp.length) {
            return new String(cp, 0, len);
        }
        if (pad.isEmpty()) {
            return "";
        }
        int[] padCp = pad.codePoints().toArray();
        int missing = len - cp.length;
        StringBuilder block = new StringBuilder(missing);
        for (int k = 0; k < missing; k++) {
            block.appendCodePoint(padCp[k % padCp.length]);
        }
        return left ? block.append(new String(cp, 0, cp.length)).toString()
                : new String(cp, 0, cp.length) + block;
    }

    /** ASCII(str):首字符的 Unicode code point,空串返回 0(MySQL/PostgreSQL 语义)。 */
    private static Function asciiFunction() {
        return new Function(SqlStdOperatorTable.ASCII.getName(), List.of(new Overload(
                List.of(VarCharVector.class), IntVector.class,
                (args, out) -> {
                    VarCharVector str = (VarCharVector) args.get(0);
                    IntVector result = (IntVector) out;
                    for (int i = 0; i < str.getValueCount(); i++) {
                        if (str.isNull(i)) {
                            result.setNull(i);
                            continue;
                        }
                        String s = new String(str.get(i), StandardCharsets.UTF_8);
                        result.setSafe(i, s.isEmpty() ? 0 : s.codePointAt(0));
                    }
                })));
    }

    /** CHR(n):code point 转字符。0 返回 NUL 字符(与 Calcite 常量折叠的 charFromUtf8 一致,
     * 保证常量与列求值结果相同;VARCHAR 能存 NUL,无需 PostgreSQL 那套「存储受限才返 null」)。 */
    private static Function chrFunction() {
        Kernel kernel = (args, out) -> chr(args, out);
        return new Function(SqlLibraryOperators.CHR.getName(), List.of(
                new Overload(List.of(IntVector.class), VarCharVector.class, kernel),
                new Overload(List.of(BigIntVector.class), VarCharVector.class, kernel)));
    }

    private static void chr(List<ValueVector> args, FieldVector out) {
        ValueVector n = args.get(0);
        VarCharVector result = (VarCharVector) out;
        for (int i = 0; i < n.getValueCount(); i++) {
            if (n.isNull(i)) {
                result.setNull(i);
                continue;
            }
            int code = (int) longArg(n, i);
            result.setSafe(i, new String(new int[]{code}, 0, 1).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** SPLIT_PART(str, delim, n):按 delim 切分取第 n 段(1-based),越界/空 delim 返回空串。 */
    private static Function splitPartFunction() {
        Kernel kernel = (args, out) -> splitPart(args, out);
        return new Function(SqlLibraryOperators.SPLIT_PART.getName(), List.of(
                new Overload(List.of(VarCharVector.class, VarCharVector.class, IntVector.class),
                        VarCharVector.class, kernel),
                new Overload(List.of(VarCharVector.class, VarCharVector.class, BigIntVector.class),
                        VarCharVector.class, kernel)));
    }

    private static void splitPart(List<ValueVector> args, FieldVector out) {
        VarCharVector str = (VarCharVector) args.get(0);
        VarCharVector delim = (VarCharVector) args.get(1);
        ValueVector n = args.get(2);
        VarCharVector result = (VarCharVector) out;
        for (int i = 0; i < str.getValueCount(); i++) {
            if (str.isNull(i) || delim.isNull(i) || n.isNull(i)) {
                result.setNull(i);
                continue;
            }
            String s = new String(str.get(i), StandardCharsets.UTF_8);
            String d = new String(delim.get(i), StandardCharsets.UTF_8);
            result.setSafe(i, splitPart(s, d, longArg(n, i)).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String splitPart(String str, String delim, long n) {
        if (n < 1) {
            return "";
        }
        if (delim.isEmpty()) {
            return n == 1 ? str : "";
        }
        String[] parts = str.split(java.util.regex.Pattern.quote(delim), -1);
        int idx = (int) (n - 1);
        return idx < parts.length ? parts[idx] : "";
    }

    /**
     * LIKE / NOT LIKE:% 匹配任意序列、_ 匹配单个字符,其余字面量按正则转义后原样。
     * 模式经 likeToRegex 转正则后整体匹配(非部分匹配)。NOT LIKE 是独立算子(SqlLikeOperator
     * negated=true),故分别注册;ESCAPE 子句(3 参形式)暂不支持。
     */
    private static void likeFunctions(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.LIKE, likeFunction(SqlStdOperatorTable.LIKE, false));
        r.register(SqlStdOperatorTable.NOT_LIKE, likeFunction(SqlStdOperatorTable.NOT_LIKE, true));
    }

    private static Function likeFunction(SqlOperator op, boolean negate) {
        Kernel kernel = (args, out) -> like(args, out, negate);
        return new Function(op.getName(), List.of(new Overload(
                List.of(VarCharVector.class, VarCharVector.class), BitVector.class, kernel)));
    }

    private static void like(List<ValueVector> args, FieldVector out, boolean negate) {
        VarCharVector str = (VarCharVector) args.get(0);
        VarCharVector pattern = (VarCharVector) args.get(1);
        BitVector result = (BitVector) out;
        for (int i = 0; i < str.getValueCount(); i++) {
            if (str.isNull(i) || pattern.isNull(i)) {
                result.setNull(i);
                continue;
            }
            String s = new String(str.get(i), StandardCharsets.UTF_8);
            String p = new String(pattern.get(i), StandardCharsets.UTF_8);
            boolean matches = likeToRegex(p).matcher(s).matches();
            result.setSafe(i, (negate ? !matches : matches) ? 1 : 0);
        }
    }

    /** 把 SQL LIKE 模式转正则:% → .*、_ → .、其余正则元字符转义后原样。 */
    private static java.util.regex.Pattern likeToRegex(String pattern) {
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                if ("\\.^$|?*+()[]{}".indexOf(c) >= 0) {
                    regex.append('\\');
                }
                regex.append(c);
            }
        }
        return java.util.regex.Pattern.compile(regex.toString());
    }

    /** CURRENT_DATE / CURRENT_TIMESTAMP:零参「当前时间」函数(见 Function.evaluate 对 0 参的支持)。 */
    private static void currentTimeFunctions(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.CURRENT_DATE, currentDateFunction());
        r.register(SqlStdOperatorTable.CURRENT_TIMESTAMP, currentTimestampFunction());
    }

    private static Function currentDateFunction() {
        Kernel kernel = (args, out) -> {
            DateDayVector dv = (DateDayVector) out;
            int days = (int) java.time.LocalDate.now().toEpochDay();
            for (int i = 0; i < dv.getValueCount(); i++) {
                dv.setSafe(i, days);
            }
        };
        return new Function("CURRENT_DATE",
                List.of(new Overload(List.of(), DateDayVector.class, kernel)));
    }

    private static Function currentTimestampFunction() {
        Kernel kernel = (args, out) -> {
            TimeStampMilliVector tv = (TimeStampMilliVector) out;
            long millis = java.time.Instant.now().toEpochMilli();
            for (int i = 0; i < tv.getValueCount(); i++) {
                tv.setSafe(i, millis);
            }
        };
        return new Function("CURRENT_TIMESTAMP",
                List.of(new Overload(List.of(), TimeStampMilliVector.class, kernel)));
    }

    private static void comparison(FunctionRegistry r) {
        r.register(SqlStdOperatorTable.EQUALS,
                comparisonFunction(SqlStdOperatorTable.EQUALS, SqlKind.EQUALS));        r.register(SqlStdOperatorTable.NOT_EQUALS,
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
                new Overload(List.of(SmallIntVector.class, SmallIntVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareShort(
                                (SmallIntVector) args.get(0), (SmallIntVector) args.get(1),
                                (BitVector) out, Short::compare, kind)),
                new Overload(List.of(IntVector.class, IntVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareInt(
                                (IntVector) args.get(0), (IntVector) args.get(1),
                                (BitVector) out, Integer::compare, kind)),
                new Overload(List.of(BigIntVector.class, BigIntVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareLong(
                                (BigIntVector) args.get(0), (BigIntVector) args.get(1),
                                (BitVector) out, Long::compare, kind)),
                new Overload(List.of(Float4Vector.class, Float4Vector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareFloat(
                                (Float4Vector) args.get(0), (Float4Vector) args.get(1),
                                (BitVector) out, Float::compare, kind)),
                new Overload(List.of(Float8Vector.class, Float8Vector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareDouble(
                                (Float8Vector) args.get(0), (Float8Vector) args.get(1),
                                (BitVector) out, Double::compare, kind)),
                new Overload(List.of(DecimalVector.class, DecimalVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareDecimal(
                                (DecimalVector) args.get(0), (DecimalVector) args.get(1),
                                (BitVector) out, BigDecimal::compareTo, kind)),
                new Overload(List.of(VarCharVector.class, VarCharVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareString(
                                (VarCharVector) args.get(0), (VarCharVector) args.get(1),
                                (BitVector) out, String::compareTo, kind)),
                new Overload(List.of(TimeMilliVector.class, TimeMilliVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareTime(
                                (TimeMilliVector) args.get(0), (TimeMilliVector) args.get(1),
                                (BitVector) out, Integer::compare, kind)),
                new Overload(List.of(DateDayVector.class, DateDayVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareDate(
                                (DateDayVector) args.get(0), (DateDayVector) args.get(1),
                                (BitVector) out, Integer::compare, kind)),
                new Overload(List.of(VarBinaryVector.class, VarBinaryVector.class), BitVector.class,
                        (args, out) -> Kernels.fillCompareBytes(
                                (VarBinaryVector) args.get(0), (VarBinaryVector) args.get(1),
                                (BitVector) out, Arrays::compareUnsigned, kind)),
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
        ScalarKernels.ShortBinary shortOp = shortKernel(op);
        ScalarKernels.IntBinary intOp = intKernel(op);
        ScalarKernels.LongBinary longOp = longKernel(op);
        ScalarKernels.FloatBinary floatOp = floatKernel(op);
        ScalarKernels.DoubleBinary doubleOp = doubleKernel(op);
        ScalarKernels.DecimalBinary decimalOp = decimalKernel(op);
        // 跨族(整型/浮点/定点)混合算术:Calcite 对 +-*/ 不插 CAST,直接把混型操作数交给
        // 执行引擎(与比较相反,比较会插 CAST 到同型),故这里按「结果取更宽类型」补重载。
        Kernel doubleMixed = (args, out) -> fillDoubleMixed(
                args.get(0), args.get(1), (Float8Vector) out, doubleOp);
        Kernel decimalMixed = (args, out) -> fillDecimalMixed(
                args.get(0), args.get(1), (DecimalVector) out, decimalOp);
        return new Function(op.getName(), List.of(
                new Overload(List.of(SmallIntVector.class, SmallIntVector.class), SmallIntVector.class,
                        (args, out) -> Kernels.fillBinaryShort(
                                (SmallIntVector) args.get(0), (SmallIntVector) args.get(1),
                                (SmallIntVector) out, shortOp)),
                new Overload(List.of(IntVector.class, IntVector.class), IntVector.class,
                        (args, out) -> Kernels.fillBinaryInt(
                                (IntVector) args.get(0), (IntVector) args.get(1), (IntVector) out, intOp)),
                new Overload(List.of(BigIntVector.class, BigIntVector.class), BigIntVector.class,
                        (args, out) -> Kernels.fillBinaryLong(
                                (BigIntVector) args.get(0), (BigIntVector) args.get(1), (BigIntVector) out, longOp)),
                new Overload(List.of(Float4Vector.class, Float4Vector.class), Float4Vector.class,
                        (args, out) -> Kernels.fillBinaryFloat(
                                (Float4Vector) args.get(0), (Float4Vector) args.get(1),
                                (Float4Vector) out, floatOp)),
                new Overload(List.of(Float8Vector.class, Float8Vector.class), Float8Vector.class,
                        (args, out) -> Kernels.fillBinaryDouble(
                                (Float8Vector) args.get(0), (Float8Vector) args.get(1), (Float8Vector) out, doubleOp)),
                new Overload(List.of(DecimalVector.class, DecimalVector.class), DecimalVector.class,
                        (args, out) -> Kernels.fillBinaryDecimal(
                                (DecimalVector) args.get(0), (DecimalVector) args.get(1),
                                (DecimalVector) out, decimalOp)),
                new Overload(List.of(IntVector.class, BigIntVector.class), BigIntVector.class,
                        (args, out) -> fillIntLongBinary(
                                (IntVector) args.get(0), (BigIntVector) args.get(1), (BigIntVector) out, longOp)),
                new Overload(List.of(BigIntVector.class, IntVector.class), BigIntVector.class,
                        (args, out) -> fillLongIntBinary(
                                (BigIntVector) args.get(0), (IntVector) args.get(1), (BigIntVector) out, longOp)),
                // 整型 × DOUBLE → DOUBLE
                new Overload(List.of(SmallIntVector.class, Float8Vector.class), Float8Vector.class, doubleMixed),
                new Overload(List.of(Float8Vector.class, SmallIntVector.class), Float8Vector.class, doubleMixed),
                new Overload(List.of(IntVector.class, Float8Vector.class), Float8Vector.class, doubleMixed),
                new Overload(List.of(Float8Vector.class, IntVector.class), Float8Vector.class, doubleMixed),
                new Overload(List.of(BigIntVector.class, Float8Vector.class), Float8Vector.class, doubleMixed),
                new Overload(List.of(Float8Vector.class, BigIntVector.class), Float8Vector.class, doubleMixed),
                // FLOAT × DOUBLE → DOUBLE
                new Overload(List.of(Float4Vector.class, Float8Vector.class), Float8Vector.class, doubleMixed),
                new Overload(List.of(Float8Vector.class, Float4Vector.class), Float8Vector.class, doubleMixed),
                // DOUBLE × DECIMAL → DOUBLE
                new Overload(List.of(Float8Vector.class, DecimalVector.class), Float8Vector.class, doubleMixed),
                new Overload(List.of(DecimalVector.class, Float8Vector.class), Float8Vector.class, doubleMixed),
                // 整型 × DECIMAL → DECIMAL
                new Overload(List.of(SmallIntVector.class, DecimalVector.class), DecimalVector.class, decimalMixed),
                new Overload(List.of(DecimalVector.class, SmallIntVector.class), DecimalVector.class, decimalMixed),
                new Overload(List.of(IntVector.class, DecimalVector.class), DecimalVector.class, decimalMixed),
                new Overload(List.of(DecimalVector.class, IntVector.class), DecimalVector.class, decimalMixed),
                new Overload(List.of(BigIntVector.class, DecimalVector.class), DecimalVector.class, decimalMixed),
                new Overload(List.of(DecimalVector.class, BigIntVector.class), DecimalVector.class, decimalMixed)));
    }

    /** 跨族混合算术:两侧都读成 double,结果写 Float8Vector(结果类型为 DOUBLE)。 */
    private static void fillDoubleMixed(ValueVector left, ValueVector right, Float8Vector out,
                                        ScalarKernels.DoubleBinary op) {
        for (int i = 0; i < left.getValueCount(); i++) {
            if (left.isNull(i) || right.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, op.apply(toDouble(left, i), toDouble(right, i)));
        }
    }

    /** 跨族混合算术:两侧都读成 BigDecimal,结果写 DecimalVector(结果类型为 DECIMAL)。 */
    private static void fillDecimalMixed(ValueVector left, ValueVector right, DecimalVector out,
                                         ScalarKernels.DecimalBinary op) {
        for (int i = 0; i < left.getValueCount(); i++) {
            if (left.isNull(i) || right.isNull(i)) { out.setNull(i); continue; }
            out.setSafe(i, Kernels.scaleTo(out, op.apply(toBigDecimal(left, i), toBigDecimal(right, i))));
        }
    }

    /** 从任意数值向量第 i 行读 double(供跨族混合算术的结果 DOUBLE 分支)。 */
    private static double toDouble(ValueVector v, int i) {
        if (v instanceof Float8Vector f) {
            return f.get(i);
        }
        if (v instanceof Float4Vector f) {
            return f.get(i);
        }
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(i);
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).doubleValue();
        }
        throw new IllegalArgumentException("not a numeric vector: " + v.getClass());
    }

    /** 从整型/定点向量第 i 行读 BigDecimal(供跨族混合算术的结果 DECIMAL 分支)。 */
    private static BigDecimal toBigDecimal(ValueVector v, int i) {
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i);
        }
        if (v instanceof IntVector iv) {
            return BigDecimal.valueOf(iv.get(i));
        }
        if (v instanceof BigIntVector bv) {
            return BigDecimal.valueOf(bv.get(i));
        }
        if (v instanceof SmallIntVector sv) {
            return BigDecimal.valueOf(sv.get(i));
        }
        throw new IllegalArgumentException("not an integral/decimal vector: " + v.getClass());
    }

    private static ScalarKernels.ShortBinary shortKernel(SqlOperator op) {
        if (op == SqlStdOperatorTable.PLUS) {
            return (a, b) -> (short) (a + b);
        }
        if (op == SqlStdOperatorTable.MINUS) {
            return (a, b) -> (short) (a - b);
        }
        if (op == SqlStdOperatorTable.MULTIPLY) {
            return (a, b) -> (short) (a * b);
        }
        return (a, b) -> b == 0 ? (short) 0 : (short) (a / b);
    }

    private static ScalarKernels.FloatBinary floatKernel(SqlOperator op) {
        if (op == SqlStdOperatorTable.PLUS) {
            return (a, b) -> a + b;
        }
        if (op == SqlStdOperatorTable.MINUS) {
            return (a, b) -> a - b;
        }
        if (op == SqlStdOperatorTable.MULTIPLY) {
            return (a, b) -> a * b;
        }
        return (a, b) -> b == 0 ? 0 : a / b;
    }

    /** DECIMAL 算术在 BigDecimal 域执行,保精确。除法按 SQL 标准:除零抛错,否则半入保留足够精度。 */
    private static ScalarKernels.DecimalBinary decimalKernel(SqlOperator op) {
        if (op == SqlStdOperatorTable.PLUS) {
            return BigDecimal::add;
        }
        if (op == SqlStdOperatorTable.MINUS) {
            return BigDecimal::subtract;
        }
        if (op == SqlStdOperatorTable.MULTIPLY) {
            return BigDecimal::multiply;
        }
        return (a, b) -> b.signum() == 0 ? BigDecimal.ZERO
                : a.divide(b, Math.max(a.scale(), b.scale()) + 6, RoundingMode.HALF_UP);
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
        return (a, b) -> b == 0 ? 0 : a / b;
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
        return (a, b) -> b == 0 ? 0 : a / b;
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
        return (a, b) -> b == 0 ? 0 : a / b;
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
