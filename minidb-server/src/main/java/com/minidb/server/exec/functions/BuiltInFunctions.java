package com.minidb.server.exec.functions;

import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** 内置标量函数:按 SqlOperator 挂到 {@link FunctionRegistry},供 {@code RexInterpreter} 分发。 */
public final class BuiltInFunctions {
    private BuiltInFunctions() {}

    public static FunctionRegistry newRegistry() {
        FunctionRegistry registry = new FunctionRegistry();
        arithmetic(registry);
        return registry;
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
                // 同型重载匹配,必须注册跨型重载,结果类型仍为 INTEGER。两种操作数顺序都注册。
                new Overload(List.of(IntVector.class, BigIntVector.class),
                        (args, out) -> fillIntLongBinary(
                                (IntVector) args.get(0), (BigIntVector) args.get(1), (IntVector) out, longOp)),
                new Overload(List.of(BigIntVector.class, IntVector.class),
                        (args, out) -> fillIntLongBinary(
                                (IntVector) args.get(1), (BigIntVector) args.get(0), (IntVector) out, longOp))));
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
}
