package com.minidb.server.plan.physical;

import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RepeatUnion;

/**
 * 递归 CTE(WITH RECURSIVE)的物理算子,对应 Calcite 的 LogicalRepeatUnion
 * (seed + iterative 两个输入)。SQL 例子:
 *
 *   WITH RECURSIVE nums(n) AS (
 *       VALUES (1)                          -- seed:非递归项,只算一次
 *     UNION ALL
 *       SELECT n + 1 FROM nums WHERE n < 5  -- iterative:递归项,反复算
 *   ) SELECT n FROM nums
 *
 * Calcite 把它规划成(注意 nums 只出现 3 处,共享同一个瞬态表):
 *
 *   LogicalRepeatUnion(all=true)
 *     LogicalTableSpool(table=nums)       -- seed 侧
 *       LogicalValues(1)
 *     LogicalTableSpool(table=nums)       -- iterative 侧
 *       LogicalProject(n+1)
 *         LogicalFilter(n<5)
 *           LogicalTableScan(nums)        -- 递归自引用,读瞬态表
 *
 * 执行原理(对齐 SQL 标准递归 CTE 的 working 表语义):
 *   1. 算一次 seed,得到初始 working 表;UNION(非 ALL)时先按行去重。
 *   2. 每轮:把当前 working 表注册进 ExecContext 的瞬态表,再执行 iterative
 *      项——它内部的 TableScan(nums) 读到 working 行,算出下一层 produced。
 *   3. produced 去重后 append 进结果,working 表替换成这些新行。
 *   4. produced 为空、或全是已见过的重复行(不动点)、或达到 iterationLimit
 *      时停止。
 *
 * working 表每轮必须「替换」而不是累积:递归项只应对上一轮新产生的行计算;
 * 累积会让同一行被反复重新推导——UNION ALL 下会重复输出(结果错误),UNION
 * 下虽去重不改变结果但白算。
 *
 * 瞬态表如何共享:working 行以 ExecContext 的 Map<String,List<Object[]>> 传递,
 * key 是 transientTable 限定名的最后一段(即 CTE 名)。MiniDbScan 对单段限定名
 * 走该注册表读取(见 MiniDbScan.execute)。ExecContext 是 per-query 单线程,普通
 * HashMap 足够,无需并发保护。
 */
public class MiniDbRepeatUnion extends RepeatUnion implements MiniDbRel {

    public MiniDbRepeatUnion(RelOptCluster cluster, RelTraitSet traitSet,
                             RelNode seed, RelNode iterative, boolean all,
                             int iterationLimit, RelOptTable transientTable) {
        super(cluster, traitSet, seed, iterative, all, iterationLimit, transientTable);
    }

    @Override
    public MiniDbRepeatUnion copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbRepeatUnion(getCluster(), traitSet, inputs.get(0), inputs.get(1),
                all, iterationLimit, getTransientTable());
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        String transientName = transientName();
        List<Object[]> result = new ArrayList<>();
        // Only UNION (not UNION ALL) dedups; the set spans the seed and every
        // iteration so a row is emitted at most once globally.
        Set<List<Object>> seen = all ? null : new LinkedHashSet<>();

        List<Object[]> workingRows = RowVectors.materialize(getSeedRel(), ctx);
        for (Object[] row : workingRows) {
            if (addIfNew(seen, row)) {
                result.add(row);
            }
        }

        int iterations = 0;
        while (iterationLimit < 0 || iterations < iterationLimit) {
            ctx.putTransientTable(transientName, workingRows);
            List<Object[]> produced;
            try {
                produced = RowVectors.materialize(getIterativeRel(), ctx);
            } finally {
                ctx.removeTransientTable(transientName);
            }
            if (produced.isEmpty()) {
                break;
            }
            List<Object[]> newRows = new ArrayList<>();
            for (Object[] row : produced) {
                if (addIfNew(seen, row)) {
                    result.add(row);
                    newRows.add(row);
                }
            }
            if (newRows.isEmpty()) {
                // Everything this iteration produced was already emitted;
                // iterating again would only repeat it, so stop.
                break;
            }
            workingRows = newRows;
            iterations++;
        }

        VectorSchemaRoot root =
                RowVectors.buildRoot(result, getRowType(), ctx.allocator());
        boolean[] done = {false};
        return BatchIterator.interruptible(new BatchIterator() {
            @Override
            public boolean hasNext() {
                return !done[0];
            }

            @Override
            public VectorSchemaRoot next() {
                done[0] = true;
                return root;
            }

            @Override
            public void close() {
                root.close();
            }
        });
    }

    /** The transient table name (last segment of its qualified name) — the
     *  same key the recursive body's scan looks up in ExecContext. */
    private String transientName() {
        RelOptTable table = getTransientTable();
        List<String> qualified = table.getQualifiedName();
        return qualified.get(qualified.size() - 1);
    }

    /** Records {@code row} in the dedup set and reports whether it is new.
     *  With UNION ALL (all=true) there is no set and every row is new. */
    private static boolean addIfNew(Set<List<Object>> seen, Object[] row) {
        if (seen == null) {
            return true;
        }
        return seen.add(Arrays.asList(row));
    }
}
