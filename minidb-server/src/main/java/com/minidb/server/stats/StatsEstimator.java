package com.minidb.server.stats;

import com.minidb.storage.common.TableSchema;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Locale;

public final class StatsEstimator {

    private StatsEstimator() {}

    public static Histogram histogramForCondition(RexNode cond, TableSchema schema, TableStats ts) {
        if (ts.columnHistograms().isEmpty()) {
            return null;
        }
        Integer colIndex = findFirstInputRef(cond);
        if (colIndex == null) {
            return null;
        }
        if (colIndex < 0 || colIndex >= schema.columns().size()) {
            return null;
        }
        String colName = schema.columns().get(colIndex).name().toLowerCase(Locale.ROOT);
        return ts.columnHistograms().get(colName);
    }

    public static Integer findFirstInputRef(RexNode node) {
        if (node instanceof RexInputRef ref) {
            return ref.getIndex();
        }
        if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                Integer idx = findFirstInputRef(operand);
                if (idx != null) {
                    return idx;
                }
            }
        }
        return null;
    }
}
