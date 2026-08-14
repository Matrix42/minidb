package com.minidb.server.exec;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.exec.functions.BuiltInFunctions;
import com.minidb.server.exec.functions.Function;
import com.minidb.server.exec.functions.FunctionRegistry;
import com.minidb.server.exec.functions.Kernels;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;

public class RexInterpreter {

    private final BufferAllocator allocator;
    private final FunctionRegistry functions;

    public RexInterpreter(BufferAllocator allocator) {
        this(allocator, BuiltInFunctions.newRegistry());
    }

    public RexInterpreter(BufferAllocator allocator, FunctionRegistry functions) {
        this.allocator = allocator;
        this.functions = functions;
    }

    public ValueVector eval(RexNode expr, VectorSchemaRoot input) {
        if (expr instanceof RexInputRef ref) {
            return RowCopier.copyVector(input.getVector(ref.getIndex()), allocator);
        }
        if (expr instanceof RexLiteral literal) {
            return literalVector(literal, input.getRowCount());
        }
        if (expr instanceof RexCall call) {
            return evalCall(call, input);
        }
        throw new UnsupportedOperationException("unsupported rex: " + expr);
    }

    private ValueVector evalCall(RexCall call, VectorSchemaRoot input) {
        SqlKind kind = call.getKind();
        switch (kind) {
            case AND:
                return logic(call.getOperands(), input, true);
            case OR:
                return logic(call.getOperands(), input, false);
            case NOT:
                return not(call.getOperands().get(0), input);
            case CAST:
                return evalCast(call, input);
            case CASE:
                return caseExpr(call, input);
            case TRIM:
                return evalTrim(call, input);
            default: {
                List<ValueVector> args = new ArrayList<>();
                for (RexNode operand : call.getOperands()) {
                    args.add(eval(operand, input));
                }
                Function f = functions.lookup(call.getOperator());
                if (f == null) {
                    for (ValueVector a : args) {
                        a.close();
                    }
                    throw new UnsupportedOperationException("unsupported operator: " + call.getOperator());
                }
                return f.evaluate(args, call.getType(), allocator);
            }
        }
    }

    /**
     * TRIM 的 3 参形式(Calcite 把 `TRIM(s)` 解析期重写为 `TRIM(Flag, ' ', s)`):第一参是
     * SYMBOL 字面量(LEADING/TRAILING/BOTH,经 RexBuilder.makeFlag 产出),不是列值,无法走
     * 常规字面量向量,故从 RexLiteral 直接取 Flag;第二/三参(trim 字符集、输入串)正常求值。
     */
    private ValueVector evalTrim(RexCall call, VectorSchemaRoot input) {
        SqlTrimFunction.Flag flag =
                ((RexLiteral) call.getOperands().get(0)).getValueAs(SqlTrimFunction.Flag.class);
        ValueVector trimChars = eval(call.getOperands().get(1), input);
        ValueVector str = eval(call.getOperands().get(2), input);
        int rows = input.getRowCount();
        VarCharVector out = new VarCharVector("trim", allocator);
        out.setInitialCapacity(rows);
        out.allocateNew();
        try {
            for (int i = 0; i < rows; i++) {
                if (trimChars.isNull(i) || str.isNull(i)) {
                    out.setNull(i);
                    continue;
                }
                String chars = new String(((VarCharVector) trimChars).get(i), StandardCharsets.UTF_8);
                String s = new String(((VarCharVector) str).get(i), StandardCharsets.UTF_8);
                out.setSafe(i, trim(s, chars, flag.getLeft() == 1, flag.getRight() == 1)
                        .getBytes(StandardCharsets.UTF_8));
            }
            out.setValueCount(rows);
            return out;
        } catch (RuntimeException e) {
            out.close();
            throw e;
        } finally {
            trimChars.close();
            str.close();
        }
    }

    /** 按 Flag 的 left/right 掩码,从字符串两端剥离 chars 集合内的字符(按 Unicode code point)。 */
    private static String trim(String s, String chars, boolean stripLeading, boolean stripTrailing) {
        int[] codePoints = s.codePoints().toArray();
        int[] trimSet = chars.codePoints().toArray();
        int begin = 0;
        int end = codePoints.length;
        if (stripLeading) {
            while (begin < end && contains(trimSet, codePoints[begin])) {
                begin++;
            }
        }
        if (stripTrailing) {
            while (end > begin && contains(trimSet, codePoints[end - 1])) {
                end--;
            }
        }
        return new String(codePoints, begin, end - begin);
    }

    private static boolean contains(int[] values, int target) {
        for (int value : values) {
            if (value == target) {
                return true;
            }
        }
        return false;
    }

    private ValueVector logic(List<RexNode> operands, VectorSchemaRoot input, boolean isAnd) {
        int rows = input.getRowCount();
        boolean[] stateNull = new boolean[rows];
        int[] stateValue = new int[rows];
        for (int i = 0; i < rows; i++) {
            stateValue[i] = isAnd ? 1 : 0;
        }
        for (RexNode operand : operands) {
            ValueVector v = eval(operand, input);
            try {
                for (int i = 0; i < rows; i++) {
                    boolean accNull = stateNull[i];
                    int acc = accNull ? (isAnd ? 1 : 0) : stateValue[i];
                    if (v.isNull(i)) {
                        boolean decisive = isAnd ? acc == 0 : acc == 1;
                        stateNull[i] = !decisive;
                        stateValue[i] = acc;
                        continue;
                    }
                    int val = ((BitVector) v).get(i);
                    if (accNull) {
                        boolean decisive = isAnd ? val == 0 : val == 1;
                        stateNull[i] = !decisive;
                        stateValue[i] = decisive ? val : (isAnd ? 1 : 0);
                        continue;
                    }
                    stateValue[i] = isAnd ? (acc & val) : (acc | val);
                }
            } finally {
                v.close();
            }
        }
        BitVector out = new BitVector(isAnd ? "and" : "or", allocator);
        out.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
            if (stateNull[i]) {
                out.setNull(i);
            } else {
                out.setSafe(i, stateValue[i]);
            }
        }
        out.setValueCount(rows);
        return out;
    }

    private ValueVector not(RexNode operand, VectorSchemaRoot input) {
        int rows = input.getRowCount();
        ValueVector v = eval(operand, input);
        try {
            BitVector out = new BitVector("not", allocator);
            out.allocateNew(rows);
            for (int i = 0; i < rows; i++) {
                if (v.isNull(i)) {
                    out.setNull(i);
                } else {
                    out.setSafe(i, ((BitVector) v).get(i) == 1 ? 0 : 1);
                }
            }
            out.setValueCount(rows);
            return out;
        } finally {
            v.close();
        }
    }

    private ValueVector caseExpr(RexCall call, VectorSchemaRoot input) {
        int rows = input.getRowCount();
        List<RexNode> operands = call.getOperands();
        List<ValueVector> conds = new java.util.ArrayList<>();
        List<ValueVector> thens = new java.util.ArrayList<>();
        int i = 0;
        while (i + 1 < operands.size()) {
            conds.add(eval(operands.get(i), input));
            thens.add(eval(operands.get(i + 1), input));
            i += 2;
        }
        ValueVector elseV = i < operands.size() ? eval(operands.get(i), input) : null;
        FieldVector out = newVector(call.getType());
        out.setInitialCapacity(rows);
        out.allocateNew();
        try {
            for (int r = 0; r < rows; r++) {
                boolean set = false;
                for (int c = 0; c < conds.size(); c++) {
                    ValueVector cond = conds.get(c);
                    if (!cond.isNull(r) && ((BitVector) cond).get(r) == 1) {
                        RowCopier.writeValue(out, r, thens.get(c), r);
                        set = true;
                        break;
                    }
                }
                if (!set) {
                    if (elseV != null) {
                        RowCopier.writeValue(out, r, elseV, r);
                    } else {
                        out.setNull(r);
                    }
                }
            }
            out.setValueCount(rows);
            return out;
        } catch (RuntimeException e) {
            out.close();
            throw e;
        } finally {
            for (ValueVector v : conds) {
                v.close();
            }
            for (ValueVector v : thens) {
                v.close();
            }
            if (elseV != null) {
                elseV.close();
            }
        }
    }

    private FieldVector newVector(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case SMALLINT:
                return new SmallIntVector("case", allocator);
            case INTEGER:
                return new IntVector("case", allocator);
            case BIGINT:
                return new BigIntVector("case", allocator);
            case REAL:
            case FLOAT:
                return new Float4Vector("case", allocator);
            case DOUBLE:
                return new Float8Vector("case", allocator);
            case DECIMAL:
                // DecimalVector 构造需要 precision/scale,只能经 ArrowTypes.field 从 RelDataType 取。
                return ArrowTypes.field(type, "case").createVector(allocator);
            case VARCHAR:
            case CHAR:
                return new VarCharVector("case", allocator);
            case BOOLEAN:
                return new BitVector("case", allocator);
            case DATE:
                return new DateDayVector("case", allocator);
            case TIME:
                return new TimeMilliVector("case", allocator);
            case TIMESTAMP:
                return new TimeStampMilliVector("case", allocator);
            case BINARY:
            case VARBINARY:
                return new VarBinaryVector("case", allocator);
            default:
                throw new UnsupportedOperationException(
                        "CASE result type: " + type.getSqlTypeName());
        }
    }

    private ValueVector evalCast(RexCall call, VectorSchemaRoot input) {
        ValueVector v = eval(call.getOperands().get(0), input);
        try {
            SqlTypeName target = call.getType().getSqlTypeName();
            int rows = input.getRowCount();
            switch (target) {
                case SMALLINT: {
                    SmallIntVector out = new SmallIntVector("cast", allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, (short) asLong(v, i));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case INTEGER: {
                    IntVector out = new IntVector("cast", allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, (int) asLong(v, i));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case BIGINT: {
                    BigIntVector out = new BigIntVector("cast", allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, asLong(v, i));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case REAL:
                case FLOAT: {
                    Float4Vector out = new Float4Vector("cast", allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, (float) asDouble(v, i));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case DOUBLE: {
                    Float8Vector out = new Float8Vector("cast", allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, asDouble(v, i));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case DECIMAL: {
                    // DecimalVector 构造需要 precision/scale,经 ArrowTypes.field 从 RelDataType 取。
                    DecimalVector out = (DecimalVector) ArrowTypes.field(call.getType(), "cast")
                            .createVector(allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, Kernels.scaleTo(out, new BigDecimal(asString(v, i))));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case CHAR:
                case VARCHAR: {
                    VarCharVector out = new VarCharVector("cast", allocator);
                    out.allocateNew();
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, asString(v, i).getBytes(StandardCharsets.UTF_8));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case BOOLEAN: {
                    BitVector out = new BitVector("cast", allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, asBoolean(v, i) ? 1 : 0);
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case TIME: {
                    TimeMilliVector out = new TimeMilliVector("cast", allocator);
                    out.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, (int) asLong(v, i));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                case BINARY:
                case VARBINARY: {
                    VarBinaryVector out = new VarBinaryVector("cast", allocator);
                    out.allocateNew();
                    for (int i = 0; i < rows; i++) {
                        if (v.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, asString(v, i).getBytes(StandardCharsets.UTF_8));
                        }
                    }
                    out.setValueCount(rows);
                    return out;
                }
                default:
                    throw new UnsupportedOperationException(
                            "unsupported CAST target: " + target);
            }
        } finally {
            v.close();
        }
    }

    private ValueVector literalVector(RexLiteral literal, int rows) {
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (literal.isNull()) {
            return nullLiteral(literal.getType(), rows);
        }
        switch (typeName) {
            case TINYINT:
            case INTEGER: {
                IntVector out = new IntVector("lit", allocator);
                out.allocateNew(rows);
                int value = literal.getValueAs(BigDecimal.class).intValue();
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, value);
                }
                out.setValueCount(rows);
                return out;
            }
            case SMALLINT: {
                SmallIntVector out = new SmallIntVector("lit", allocator);
                out.allocateNew(rows);
                short value = literal.getValueAs(BigDecimal.class).shortValue();
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, value);
                }
                out.setValueCount(rows);
                return out;
            }
            case BIGINT: {
                BigIntVector out = new BigIntVector("lit", allocator);
                out.allocateNew(rows);
                long value = literal.getValueAs(BigDecimal.class).longValue();
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, value);
                }
                out.setValueCount(rows);
                return out;
            }
            case REAL:
            case FLOAT: {
                Float4Vector out = new Float4Vector("lit", allocator);
                out.allocateNew(rows);
                float value = literal.getValueAs(BigDecimal.class).floatValue();
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, value);
                }
                out.setValueCount(rows);
                return out;
            }
            case DOUBLE: {
                Float8Vector out = new Float8Vector("lit", allocator);
                out.allocateNew(rows);
                double value = literal.getValueAs(BigDecimal.class).doubleValue();
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, value);
                }
                out.setValueCount(rows);
                return out;
            }
            case DECIMAL: {
                // DecimalVector 构造需要 precision/scale,只能经 ArrowTypes.field 从 RelDataType 取。
                DecimalVector out = (DecimalVector) ArrowTypes.field(literal.getType(), "lit")
                        .createVector(allocator);
                out.allocateNew(rows);
                BigDecimal value = literal.getValueAs(BigDecimal.class);
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, Kernels.scaleTo(out, value));
                }
                out.setValueCount(rows);
                return out;
            }
            case CHAR:
            case VARCHAR: {
                VarCharVector out = new VarCharVector("lit", allocator);
                out.allocateNew();
                byte[] bytes = literal.getValueAs(String.class)
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, bytes);
                }
                out.setValueCount(rows);
                return out;
            }
            case BOOLEAN: {
                BitVector out = new BitVector("lit", allocator);
                out.allocateNew(rows);
                boolean value = literal.getValueAs(Boolean.class);
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, value ? 1 : 0);
                }
                out.setValueCount(rows);
                return out;
            }
            case DATE: {
                DateDayVector out = new DateDayVector("lit", allocator);
                out.allocateNew(rows);
                Calendar cal = literal.getValueAs(Calendar.class);
                int days = (int) TimeUnit.MILLISECONDS.toDays(cal.getTimeInMillis());
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, days);
                }
                out.setValueCount(rows);
                return out;
            }
            case TIME: {
                TimeMilliVector out = new TimeMilliVector("lit", allocator);
                out.allocateNew(rows);
                Calendar cal = literal.getValueAs(Calendar.class);
                int millis = (int) (cal.get(Calendar.HOUR_OF_DAY) * 3_600_000L
                        + cal.get(Calendar.MINUTE) * 60_000L
                        + cal.get(Calendar.SECOND) * 1_000L
                        + cal.get(Calendar.MILLISECOND));
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, millis);
                }
                out.setValueCount(rows);
                return out;
            }
            case TIMESTAMP: {
                TimeStampMilliVector out = new TimeStampMilliVector("lit", allocator);
                out.allocateNew(rows);
                Calendar cal = literal.getValueAs(Calendar.class);
                long millis = cal.getTimeInMillis();
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, millis);
                }
                out.setValueCount(rows);
                return out;
            }
            case BINARY:
            case VARBINARY: {
                VarBinaryVector out = new VarBinaryVector("lit", allocator);
                out.allocateNew();
                byte[] bytes = literalBytes(literal);
                for (int i = 0; i < rows; i++) {
                    out.setSafe(i, bytes);
                }
                out.setValueCount(rows);
                return out;
            }
            default:
                throw new UnsupportedOperationException(
                        "unsupported literal type: " + typeName);
        }
    }

    private ValueVector nullLiteral(RelDataType type, int rows) {
        SqlTypeName typeName = type.getSqlTypeName();
        ValueVector out;
        switch (typeName) {
            case TINYINT:
            case INTEGER:
                out = new IntVector("lit", allocator);
                break;
            case SMALLINT:
                out = new SmallIntVector("lit", allocator);
                break;
            case BIGINT:
                out = new BigIntVector("lit", allocator);
                break;
            case REAL:
            case FLOAT:
                out = new Float4Vector("lit", allocator);
                break;
            case DOUBLE:
                out = new Float8Vector("lit", allocator);
                break;
            case DECIMAL:
                // DecimalVector 构造需要 precision/scale,经 ArrowTypes.field 从 RelDataType 取。
                out = ArrowTypes.field(type, "lit").createVector(allocator);
                break;
            case CHAR:
            case VARCHAR:
                out = new VarCharVector("lit", allocator);
                break;
            case BOOLEAN:
                out = new BitVector("lit", allocator);
                break;
            case DATE:
                out = new DateDayVector("lit", allocator);
                break;
            case TIME:
                out = new TimeMilliVector("lit", allocator);
                break;
            case TIMESTAMP:
                out = new TimeStampMilliVector("lit", allocator);
                break;
            case BINARY:
            case VARBINARY:
                out = new VarBinaryVector("lit", allocator);
                break;
            default:
                throw new UnsupportedOperationException(
                        "unsupported literal type: " + typeName);
        }
        out.setInitialCapacity(rows);
        out.allocateNew();
        out.setValueCount(rows); // all null by default
        return out;
    }

    /** BINARY/VARBINARY 字面量的字节值:Calcite 1.42 把 `X'...'`/`B'...'` 存为 ByteString,
     * 旧版本可能存 byte[] 或 BitString,三者都兼容。 */
    private static byte[] literalBytes(RexLiteral literal) {
        Object raw = literal.getValue();
        if (raw instanceof byte[] bytes) {
            return bytes;
        }
        if (raw instanceof org.apache.calcite.avatica.util.ByteString byteString) {
            return byteString.getBytes();
        }
        if (raw instanceof org.apache.calcite.util.BitString bitString) {
            return bitString.getAsByteArray();
        }
        throw new UnsupportedOperationException(
                "unsupported binary literal value: " + raw.getClass());
    }

    static long asLong(ValueVector v, int i) {
        if (v instanceof SmallIntVector sv) {
            return sv.get(i);
        }
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof Float4Vector fv) {
            return (long) fv.get(i);
        }
        if (v instanceof Float8Vector fv) {
            return (long) fv.get(i);
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).longValue();
        }
        if (v instanceof BitVector bv) {
            return bv.get(i);
        }
        if (v instanceof VarCharVector vv) {
            return Long.parseLong(new String(vv.get(i), StandardCharsets.UTF_8).trim());
        }
        throw new IllegalArgumentException("not a numeric vector: " + v.getClass());
    }

    static double asDouble(ValueVector v, int i) {
        if (v instanceof SmallIntVector sv) {
            return sv.get(i);
        }
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(i);
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(i);
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).doubleValue();
        }
        if (v instanceof BitVector bv) {
            return bv.get(i);
        }
        if (v instanceof VarCharVector vv) {
            return Double.parseDouble(new String(vv.get(i), StandardCharsets.UTF_8).trim());
        }
        throw new IllegalArgumentException("not a numeric vector: " + v.getClass());
    }

    private static String asString(ValueVector v, int i) {
        if (v instanceof VarCharVector vv) {
            return new String(vv.get(i), StandardCharsets.UTF_8);
        }
        if (v instanceof SmallIntVector sv) {
            return Short.toString(sv.get(i));
        }
        if (v instanceof IntVector iv) {
            return Integer.toString(iv.get(i));
        }
        if (v instanceof BigIntVector bv) {
            return Long.toString(bv.get(i));
        }
        if (v instanceof Float4Vector fv) {
            return Float.toString(fv.get(i));
        }
        if (v instanceof Float8Vector fv) {
            return Double.toString(fv.get(i));
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).toPlainString();
        }
        if (v instanceof BitVector bv) {
            return bv.get(i) == 1 ? "true" : "false";
        }
        throw new IllegalArgumentException("cannot cast to string: " + v.getClass());
    }

    private static boolean asBoolean(ValueVector v, int i) {
        if (v instanceof BitVector bv) {
            return bv.get(i) == 1;
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(i) != 0;
        }
        if (v instanceof IntVector iv) {
            return iv.get(i) != 0;
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i) != 0;
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(i) != 0;
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(i) != 0;
        }
        if (v instanceof DecimalVector dv) {
            return dv.getObject(i).signum() != 0;
        }
        if (v instanceof VarCharVector vv) {
            return Boolean.parseBoolean(new String(vv.get(i), StandardCharsets.UTF_8).trim());
        }
        throw new IllegalArgumentException("cannot cast to boolean: " + v.getClass());
    }
}
