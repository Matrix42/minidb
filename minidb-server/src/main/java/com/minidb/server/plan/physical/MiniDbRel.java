package com.minidb.server.plan.physical;

import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;

public interface MiniDbRel {
    BatchIterator execute(ExecContext ctx);
}
