package com.minidb.server.exec.functions;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import java.util.List;

/** 完整内核:args 为已求值的操作数向量(与 out 等长),out 已按结果类型分配、未 setValueCount。 */
@FunctionalInterface
public interface Kernel {
    void execute(List<ValueVector> args, FieldVector out);
}
