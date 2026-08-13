package com.minidb.server.exec.functions;

import java.util.List;
import org.apache.arrow.vector.ValueVector;

/** 一个重载:声明的输入向量类型 + 内核。 */
public record Overload(List<Class<? extends ValueVector>> inputTypes, Kernel kernel) {}
