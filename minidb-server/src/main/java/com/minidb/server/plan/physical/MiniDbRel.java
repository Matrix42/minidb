package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import com.minidb.storage.common.BatchIterator;

public interface MiniDbRel {
    BatchIterator execute(ExecContext ctx);
}
