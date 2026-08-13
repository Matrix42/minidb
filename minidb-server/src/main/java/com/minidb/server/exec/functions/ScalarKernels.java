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
