package com.minidb.server.exec.functions;

import com.minidb.server.catalog.ArrowTypes;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.rel.type.RelDataType;

/** 一个函数:若干重载 + 按输入/输出类型分发 + 分配输出向量。 */
public final class Function {
    private final String name;
    private final List<Overload> overloads;

    public Function(String name, List<Overload> overloads) {
        this.name = name;
        this.overloads = overloads;
    }

    public ValueVector evaluate(List<ValueVector> args, RelDataType resultType,
                                BufferAllocator allocator) {
        int rows = args.get(0).getValueCount();
        // 先解析(含输出类型)再分配:解析失败不泄漏输出向量。
        Class<? extends FieldVector> outputClass = outputVectorClass(resultType);
        Kernel kernel = resolve(args, outputClass);
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

    private Kernel resolve(List<ValueVector> args, Class<? extends FieldVector> outputClass) {
        for (Overload o : overloads) {
            if (o.outputType().equals(outputClass) && matches(o.inputTypes(), args)) {
                return o.kernel();
            }
        }
        throw new UnsupportedOperationException(
                "no overload of " + name + " for argument types " + argClasses(args)
                        + " with result " + outputClass.getSimpleName());
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
            case INTEGER -> IntVector.class;
            case BIGINT -> BigIntVector.class;
            case DOUBLE, FLOAT, REAL, DECIMAL -> Float8Vector.class;
            case VARCHAR, CHAR -> VarCharVector.class;
            case BOOLEAN -> BitVector.class;
            case DATE -> DateDayVector.class;
            case TIMESTAMP -> TimeStampMilliVector.class;
            default -> throw new IllegalArgumentException(
                    "unsupported result type: " + type.getSqlTypeName());
        };
    }
}
