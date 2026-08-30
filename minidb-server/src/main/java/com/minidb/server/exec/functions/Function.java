package com.minidb.server.exec.functions;

import com.minidb.storage.common.ArrowTypes;

import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.List;

/** 一个函数:若干重载 + 按输入/输出类型分发 + 分配输出向量。 */
public final class Function {
    private final String name;
    private final List<Overload> overloads;

    public Function(String name, List<Overload> overloads) {
        this.name = name;
        this.overloads = overloads;
    }

    public ValueVector evaluate(
            List<ValueVector> args, RelDataType resultType, int rows, BufferAllocator allocator) {
        // 先解析(含输出类型)再分配:解析失败不泄漏输出向量。
        Class<? extends FieldVector> outputClass = outputVectorClass(resultType);
        Overload matched = resolveOverload(args, outputClass);
        // 精确匹配失败时(如 STDDEV 提升为 Float8 但 plan 结果类型仍是 INTEGER),
        // 用重载声明的 outputType 创建向量,避免类型不匹配。
        Class<? extends FieldVector> actualOutput = matched.outputType();
        FieldVector out = createOutputVector(actualOutput, allocator);
        out.setInitialCapacity(rows);
        out.allocateNew();
        // 先 setValueCount 再执行 kernel:0 参函数(如 CURRENT_DATE)没有入参可读行数,
        // 靠 out.getValueCount() 得知要写多少行;有参函数不受影响(仍读 args 的行数)。
        out.setValueCount(rows);
        try {
            matched.kernel().execute(args, out);
        } catch (RuntimeException e) {
            // kernel 抛异常(如除零)时,args 与已分配的 out 都要释放,否则查询失败泄漏 Arrow 内存。
            for (ValueVector a : args) {
                a.close();
            }
            out.close();
            throw e;
        }
        for (ValueVector a : args) {
            a.close();
        }
        return out;
    }

    private Overload resolveOverload(
            List<ValueVector> args, Class<? extends FieldVector> outputClass) {
        for (Overload o : overloads) {
            if (o.outputType().equals(outputClass) && matches(o.inputTypes(), args)) {
                return o;
            }
        }
        // 精确匹配失败:操作数类型因精度提升(AVG/STDDEV→Float8)与 plan 推导结果类型
        // (INTEGER)不一致。回退到忽略 outputType 的匹配,用重载声明的 outputType。
        for (Overload o : overloads) {
            if (matches(o.inputTypes(), args)) {
                return o;
            }
        }
        throw new UnsupportedOperationException(
                "no overload of "
                        + name
                        + " for argument types "
                        + argClasses(args)
                        + " with result "
                        + outputClass.getSimpleName());
    }

    private static FieldVector createOutputVector(
            Class<? extends FieldVector> vectorClass, BufferAllocator allocator) {
        if (vectorClass == SmallIntVector.class) {
            return new SmallIntVector("expr", allocator);
        }
        if (vectorClass == IntVector.class) {
            return new IntVector("expr", allocator);
        }
        if (vectorClass == BigIntVector.class) {
            return new BigIntVector("expr", allocator);
        }
        if (vectorClass == Float4Vector.class) {
            return new Float4Vector("expr", allocator);
        }
        if (vectorClass == Float8Vector.class) {
            return new Float8Vector("expr", allocator);
        }
        if (vectorClass == DecimalVector.class) {
            return new DecimalVector("expr", allocator, 38, 6);
        }
        if (vectorClass == BitVector.class) {
            return new BitVector("expr", allocator);
        }
        if (vectorClass == VarCharVector.class) {
            return new VarCharVector("expr", allocator);
        }
        if (vectorClass == DateDayVector.class) {
            return new DateDayVector("expr", allocator);
        }
        if (vectorClass == TimeMilliVector.class) {
            return new TimeMilliVector("expr", allocator);
        }
        if (vectorClass == TimeStampMilliVector.class) {
            return new TimeStampMilliVector("expr", allocator);
        }
        if (vectorClass == VarBinaryVector.class) {
            return new VarBinaryVector("expr", allocator);
        }
        throw new IllegalArgumentException("unsupported output vector class: " + vectorClass);
    }

    private static boolean matches(
            List<Class<? extends ValueVector>> types, List<ValueVector> args) {
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
        // 逐个 add 到预类型 List<Class<?>>:stream 版会把 getClass() 推断成
        // Class<? extends ValueVector>,其 toList() 与 List<Class<?>> 不兼容(泛型不变性)。
        List<Class<?>> classes = new ArrayList<>(args.size());
        for (ValueVector a : args) {
            classes.add(a.getClass());
        }
        return classes;
    }

    /** 把 Calcite 结果类型映射到 Arrow 向量类,与 {@link ArrowTypes} 的映射保持一致。 */
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
            default ->
                    throw new IllegalArgumentException(
                            "unsupported result type: " + type.getSqlTypeName());
        };
    }
}
