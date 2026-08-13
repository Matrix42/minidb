package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;

public interface MiniDbRel {
    BatchIterator execute(ExecContext ctx);
}
