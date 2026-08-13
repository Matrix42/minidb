package com.minidb.server.plan.physical;

import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableSpool;

/**
 * 递归 CTE 计划里的 spool 节点(对应 Calcite 的 LogicalTableSpool)。在
 * RepeatUnion 树里,seed 和 iterative 两个输入各自包在一个
 * TableSpool(table=nums) 中,两者(以及递归自引用里的 TableScan(nums))
 * 共享同一个瞬态表 RelOptTable(qualified name 只有 1 段,如 ["nums"])。
 *
 * 在 Calcite 的 Enumerable 实现里,spool 的职责是「把子节点输出写入瞬态表」
 * (lazy collection spool),供下一轮 TableScan 读取。但 MiniDB 把 spooling 统一
 * 收进 MiniDbRepeatUnion:它自己在每轮迭代前把 working 行放进 ExecContext 瞬态
 * 表,再执行 iterative 项。所以本节点无需做任何事,只是把输入透传出去。
 *
 * 为什么还要保留这个「空壳」算子:Calcite 生成的确实是 LogicalTableSpool
 * 节点,若没有对应的 ConverterRule 把它转成 MINIDB 约定,VolcanoPlanner 会抛
 * CannotPlanException(找不到规则把 RepeatUnion 的输入降到 MINIDB 约定)。
 */
public class MiniDbTableSpool extends TableSpool implements MiniDbRel {

    public MiniDbTableSpool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                            Spool.Type readType, Spool.Type writeType, RelOptTable table) {
        super(cluster, traitSet, input, readType, writeType, table);
    }

    @Override
    protected Spool copy(RelTraitSet traitSet, RelNode input,
                         Spool.Type readType, Spool.Type writeType) {
        return new MiniDbTableSpool(getCluster(), traitSet, input, readType, writeType,
                table);
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        return ((MiniDbRel) getInput()).execute(ctx);
    }
}
