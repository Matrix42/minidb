package com.minidb.server.plan.physical;

import com.minidb.storage.common.BatchIterator;
import com.minidb.server.exec.ExecContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

/**
 * 公共子表达式消除(CSE):物化子树的执行结果,相同 key 的后续执行直接回放。
 *
 * <p>查询计划中相同子树(如 query65 的 ss⨝date_dim 出现两次)会被 Planner 的 CSE 遍历
 * 替换为共享的 MiniDbCse 节点。首次执行时物化全部批到 ExecContext 缓存,后续命中
 * 返回回放迭代器,避免重复执行昂贵的 Join/聚合。</p>
 */
public final class MiniDbCse extends SingleRel implements MiniDbRel {

    private final String key;

    public MiniDbCse(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, String key) {
        super(cluster, traitSet, input);
        this.key = key;
    }

    public String key() {
        return key;
    }

    @Override
    public BatchIterator execute(ExecContext ctx) {
        List<VectorSchemaRoot> cached = ctx.getCseCache(key);
        if (cached != null) {
            return replay(cached);
        }
        // 首次执行:物化全部批
        List<VectorSchemaRoot> batches = new ArrayList<>();
        BatchIterator it = ((MiniDbRel) getInput()).execute(ctx);
        try {
            while (it.hasNext()) {
                batches.add(it.next());
            }
        } catch (RuntimeException e) {
            it.close();
            // 释放已收集的批
            for (VectorSchemaRoot b : batches) {
                b.close();
            }
            throw e;
        } finally {
            // 不在此 close it——批已转移给 batches,it.close() 会关源迭代器
            // 但批的所有权在我们这,it.close() 安全
            it.close();
        }
        ctx.putCseCache(key, batches);
        return replay(batches);
    }

    private BatchIterator replay(List<VectorSchemaRoot> batches) {
        return BatchIterator.interruptible(new BatchIterator() {
            int idx;
            @Override
            public boolean hasNext() {
                return idx < batches.size();
            }
            @Override
            public VectorSchemaRoot next() {
                return batches.get(idx++);
            }
            @Override
            public void close() {
                // 批由 ExecContext 生命周期统一释放,不在此关
            }
        });
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MiniDbCse(getCluster(), traitSet, sole(inputs), key);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("key", key);
    }
}