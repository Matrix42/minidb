package com.minidb.server.plan.physical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public class MiniDbConvention extends Convention.Impl {

    public static final MiniDbConvention INSTANCE = new MiniDbConvention();

    private MiniDbConvention() {
        super("MINIDB", RelNode.class);
    }
}
