package com.minidb.server.exec.functions;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import java.util.List;

/**
 * 一个重载:声明的输入向量类型 + 输出向量类型 + 内核。
 *
 * <p>输出类型参与分发:同一输入向量类型可能对应多种输出(如 ABS 的 [BigIntVector] 输入,对 INTEGER 字面量产 IntVector、对 BIGINT 列产
 * BigIntVector),仅按输入类型 无法区分,必须连同输出类型一起匹配。
 */
public record Overload(
        List<Class<? extends ValueVector>> inputTypes,
        Class<? extends FieldVector> outputType,
        Kernel kernel) {}
