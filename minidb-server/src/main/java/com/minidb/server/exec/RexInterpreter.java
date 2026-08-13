package com.minidb.server.exec;

import com.minidb.server.exec.functions.BuiltInFunctions;
import com.minidb.server.exec.functions.Function;
import com.minidb.server.exec.functions.FunctionRegistry;
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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
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

    /** 按 Flag 的 left/right 掩码,从字符串两端剥离 chars 集合内的字符(同 SQL TRIM 语义)。 */
    private static String trim(String s, String chars, boolean stripLeading, boolean stripTrailing) {
        int begin = 0;
        int end = s.length();
        if (stripLeading) {
            while (begin < end && chars.indexOf(s.charAt(begin)) >= 0) {
                begin++;
            }
        }
        if (stripTrailing) {
            while (end > begin && chars.indexOf(s.charAt(end - 1)) >= 0) {
                end--;
            }
        }
        return s.substring(begin, end);
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
            case INTEGER:
                return new IntVector("case", allocator);
            case BIGINT:
                return new BigIntVector("case", allocator);
            case DOUBLE:
            case FLOAT:
            case REAL:
            case DECIMAL:
                return new Float8Vector("case", allocator);
            case VARCHAR:
                return new VarCharVector("case", allocator);
            case BOOLEAN:
                return new BitVector("case", allocator);
            case DATE:
                return new DateDayVector("case", allocator);
            case TIMESTAMP:
                return new TimeStampMilliVector("case", allocator);
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
                case INTEGER:
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
                case DOUBLE:
                case FLOAT:
                case REAL:
                case DECIMAL: {
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
            return nullLiteral(typeName, rows);
        }
        switch (typeName) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
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
            case DECIMAL:
            case FLOAT:
            case REAL:
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
            default:
                throw new UnsupportedOperationException(
                        "unsupported literal type: " + typeName);
        }
    }

    private ValueVector nullLiteral(SqlTypeName typeName, int rows) {
        ValueVector out;
        switch (typeName) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                out = new BigIntVector("lit", allocator);
                break;
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                out = new Float8Vector("lit", allocator);
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
            case TIMESTAMP:
                out = new TimeStampMilliVector("lit", allocator);
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

    static long asLong(ValueVector v, int i) {
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof Float8Vector fv) {
            return (long) fv.get(i);
        }
        throw new IllegalArgumentException("not a numeric vector: " + v.getClass());
    }

    static double asDouble(ValueVector v, int i) {
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(i);
        }
        throw new IllegalArgumentException("not a numeric vector: " + v.getClass());
    }
}
