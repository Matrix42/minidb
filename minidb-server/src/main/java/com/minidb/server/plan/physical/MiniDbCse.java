package com.minidb.server.plan.physical;

import com.minidb.server.exec.ExecContext;
import com.minidb.storage.common.BatchIterator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

import java.util.ArrayList;
import java.util.List;

/**
 * 公共子表达式消除(CSE):物化子树的执行结果,相同 key 的后续执行直接回放。
 *
 * <p>查询计划中相同子树(如 query65 的 ss⨝date_dim 出现两次)会被 Planner 的 CSE 遍历 替换为共享的 MiniDbCse 节点。首次执行时物化全部批到
 * ExecContext 缓存,后续命中 返回回放迭代器,避免重复执行昂贵的 Join/聚合。
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
        // 首次执行:物化输入到单一 owned root(数据深拷贝到新 root),再关源迭代器。
        // 不能直接收集源迭代器发出的批并缓存——所有权模型是「批归迭代器所有」,
        // 源迭代器 close 会释放它发出的批(如 MiniDbAggregate 的迭代器 close 关
        // 输出 root),缓存已释放的批 → 回放读到 null。materializeToRoot 先拷贝
        // 到自有 root 再安全 close 源,缓存的生命周期由 ExecContext 统一释放。
        VectorSchemaRoot materialized = RowVectors.materializeToRoot(getInput(), ctx);
        List<VectorSchemaRoot> batches = new ArrayList<>();
        batches.add(materialized);
        ctx.putCseCache(key, batches);
        return replay(batches);
    }

    private BatchIterator replay(List<VectorSchemaRoot> batches) {
        return BatchIterator.interruptible(
                new BatchIterator() {
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
