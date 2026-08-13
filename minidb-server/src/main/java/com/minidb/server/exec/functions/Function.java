package com.minidb.server.exec.functions;

import com.minidb.server.catalog.ArrowTypes;
import java.util.ArrayList;
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
        // 逐个 add 到预类型 List<Class<?>>:stream 版会把 getClass() 推断成
        // Class<? extends ValueVector>,其 toList() 与 List<Class<?>> 不兼容(泛型不变性)。
        List<Class<?>> classes = new ArrayList<>(args.size());
        for (ValueVector a : args) {
            classes.add(a.getClass());
        }
        return classes;
    }
}
