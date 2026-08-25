package com.minidb.server.netty;

import com.minidb.protocol.Message;
import com.minidb.protocol.Protocol;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.CursorHandle;
import com.minidb.server.exec.MetadataExecutor;
import com.minidb.server.exec.Paginator;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHandler.class);

    private static final int DEFAULT_FETCH_SIZE = 4096;
    private final QueryExecutor executor;
    private final MetadataExecutor metadata;
    private final ExecutorService queryPool;
    private String currentSchema = MiniDbCatalog.DEFAULT_SCHEMA;
    // Active cursors keyed by the ExecuteRequest id that opened them. A
    // SessionHandler is per-channel and only touched by the Netty event loop
    // thread, so a plain HashMap needs no synchronization.
    private final Map<Long, Paginator> cursors = new HashMap<>();
    /** 未完成的查询 Future,用于 channelInactive 时 cancel(true) 中断线程。 */
    private final Set<Future<?>> outstanding = ConcurrentHashMap.newKeySet();

    public SessionHandler(QueryExecutor executor, MetadataExecutor metadata,
                         ExecutorService queryPool) {
        this.executor = executor;
        this.metadata = metadata;
        this.queryPool = queryPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        if (msg instanceof Message.Handshake h) {
            ctx.writeAndFlush(new Message.HandshakeAck(Protocol.VERSION));
        } else if (msg instanceof Message.ExecuteRequest req) {
            handleExecute(ctx, req);
        } else if (msg instanceof Message.FetchRequest req) {
            handleFetch(ctx, req);
        } else if (msg instanceof Message.CloseCursorRequest req) {
            handleCloseCursor(req);
        } else if (msg instanceof Message.SchemasRequest req) {
            LOG.info("metadata schemas: schemaPattern='{}'", req.schemaPattern());
            handleMetadata(ctx, req.requestId(), () -> metadata.schemas(req.schemaPattern()));
        } else if (msg instanceof Message.TablesRequest req) {
            LOG.info("metadata tables: schemaPattern='{}', tableNamePattern='{}'",
                    req.schemaPattern(), req.tableNamePattern());
            handleMetadata(ctx, req.requestId(), () -> metadata.tables(
                    req.schemaPattern(), req.tableNamePattern(), req.types()));
        } else if (msg instanceof Message.ColumnsRequest req) {
            LOG.info("metadata columns: schemaPattern='{}', tableNamePattern='{}', columnNamePattern='{}'",
                    req.schemaPattern(), req.tableNamePattern(), req.columnNamePattern());
            handleMetadata(ctx, req.requestId(), () -> metadata.columns(
                    req.schemaPattern(), req.tableNamePattern(), req.columnNamePattern()));
        } else if (msg instanceof Message.CloseRequest) {
            ctx.close();
        }
    }

    private void handleExecute(ChannelHandlerContext ctx, Message.ExecuteRequest req) {
        LOG.debug("executing: {}", req.sql());
        // 快照 currentSchema:查询执行在 worker 线程,不在事件循环线程。
        String schema = currentSchema;
        Future<?> future = queryPool.submit(() -> {
            long start = System.nanoTime();
            try {
                QueryResult result = executor.executeCursor(req.sql(), schema);
                // 客户端断开(channelInactive→cancel)时线程被中断,丢弃结果。
                if (Thread.currentThread().isInterrupted()) {
                    closeResult(result);
                    return;
                }
                ctx.executor().execute(() -> handleResult(ctx, req, result, start));
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                ctx.executor().execute(() -> handleError(ctx, req, e, start));
            }
        });
        outstanding.add(future);
    }

    /** 在事件循环线程处理查询结果。 */
    private void handleResult(ChannelHandlerContext ctx, Message.ExecuteRequest req,
                              QueryResult result, long start) {
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        if (result instanceof QueryResult.UseSchema us) {
            currentSchema = us.schemaName();
            LOG.info("use schema: {} in {} ms", currentSchema, elapsedMs);
            ctx.writeAndFlush(new Message.UpdateCount(req.requestId(), 0));
        } else if (result instanceof QueryResult.Update update) {
            LOG.info("query ok: {} rows affected in {} ms", update.count(), elapsedMs);
            ctx.writeAndFlush(new Message.UpdateCount(req.requestId(), update.count()));
        } else if (result instanceof QueryResult.Rows rows) {
            LOG.info("query ok: {} rows returned in {} ms", rows.data().getRowCount(), elapsedMs);
            sendRows(ctx, req.requestId(), rows.data(), true);
            rows.data().close();
        } else if (result instanceof QueryResult.Cursor cursor) {
            CursorHandle handle = cursor.handle();
            BufferAllocator allocator = handle.context().allocator();
            Paginator paginator = new Paginator(handle.iterator(), handle.schema(), allocator);
            boolean retained = false;
            try {
                int pageSize = req.fetchSize() > 0 ? req.fetchSize() : DEFAULT_FETCH_SIZE;
                VectorSchemaRoot page = paginator.nextPage(pageSize);
                boolean last = paginator.isDone();
                LOG.info("query ok: first page {} rows (last={}) in {} ms",
                        page.getRowCount(), last, elapsedMs);
                sendRows(ctx, req.requestId(), page, last);
                page.close();
                if (last) {
                    paginator.close();
                } else {
                    cursors.put(req.requestId(), paginator);
                    retained = true;
                }
            } catch (Exception e) {
                if (!retained) {
                    paginator.close();
                }
                throw e;
            }
        }
    }

    /** 在事件循环线程处理查询失败。 */
    private void handleError(ChannelHandlerContext ctx, Message.ExecuteRequest req, Exception e, long start) {
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        LOG.warn("query failed in {} ms: {}", elapsedMs, req.sql(), e);
        String message = e.getMessage() == null ? e.toString() : e.getMessage();
        ctx.writeAndFlush(Message.ExecuteResponse.error(req.requestId(), message));
    }

    /** 客户端已断开时释放查询结果(worker 线程调用,非事件循环)。 */
    private static void closeResult(QueryResult result) {
        if (result instanceof QueryResult.Rows rows) {
            rows.data().close();
        } else if (result instanceof QueryResult.Cursor cursor) {
            // 游标尚未被任何人取出,直接关闭句柄释放资源。
            cursor.handle().close();
        }
    }

    private void sendRows(ChannelHandlerContext ctx, long requestId, VectorSchemaRoot root,
                          boolean lastBatch) {
        // 编码直接进 Netty ByteBuf(零 byte[] 中间拷贝),数据所有权随消息转给 Encoder。
        ByteBuf buf = ctx.alloc().buffer();
        try {
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, new ByteBufChannel(buf))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            ctx.writeAndFlush(new Message.ArrowBatch(requestId, lastBatch, buf));
        } catch (Exception e) {
            buf.release();
            LOG.warn("failed to send rows for request {}", requestId, e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
        }
    }

    /**
     * 分页续批:只编码 IPC record-batch message(无 magic/schema/EOS)。schema 已在
     * 本 cursor 的首批发过,客户端按 cursorId 复用——大结果集每页省掉 schema 重复编码
     * 的固定开销。VectorUnloader.getRecordBatch 已 retain buffers,serialize 后可安全 close。
     */
    private void sendContinuation(ChannelHandlerContext ctx, long requestId, long cursorId,
                                  VectorSchemaRoot root, boolean lastBatch) {
        ByteBuf buf = ctx.alloc().buffer();
        try {
            ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch();
            try {
                MessageSerializer.serialize(
                        new WriteChannel(new ByteBufChannel(buf)), recordBatch, IpcOption.DEFAULT);
            } finally {
                recordBatch.close();
            }
            ctx.writeAndFlush(new Message.ArrowContinuation(requestId, cursorId, lastBatch, buf));
        } catch (Exception e) {
            buf.release();
            LOG.warn("failed to send continuation for request {}", requestId, e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
        }
    }

    private void handleFetch(ChannelHandlerContext ctx, Message.FetchRequest req) {
        Paginator paginator = cursors.get(req.cursorId());
        if (paginator == null) {
            ctx.writeAndFlush(Message.ExecuteResponse.error(req.requestId(),
                    "unknown cursor: " + req.cursorId()));
            return;
        }
        try {
            VectorSchemaRoot page = paginator.nextPage(req.maxRows());
            if (page == null) {
                cursors.remove(req.cursorId());
                paginator.close();
                ctx.writeAndFlush(Message.ExecuteResponse.error(req.requestId(),
                        "cursor already exhausted: " + req.cursorId()));
                return;
            }
            boolean last = paginator.isDone();
            sendContinuation(ctx, req.requestId(), req.cursorId(), page, last);
            page.close();
            if (last) {
                // Last page emitted: the cursor is exhausted, so release it now.
                cursors.remove(req.cursorId());
                paginator.close();
            }
        } catch (Exception e) {
            cursors.remove(req.cursorId());
            paginator.close();
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(req.requestId(), message));
        }
    }

    private void handleCloseCursor(Message.CloseCursorRequest req) {
        Paginator paginator = cursors.remove(req.cursorId());
        if (paginator != null) {
            paginator.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 客户端断开:取消所有未完成的查询(cancel(true)中断 worker 线程)。
        for (Future<?> f : outstanding) {
            f.cancel(true);
        }
        outstanding.clear();
        // Release every still-open cursor when the connection drops, otherwise
        // the paginator's batches leak (their allocator is only released at
        // iterator close).
        for (Paginator p : cursors.values()) {
            p.close();
        }
        cursors.clear();
        super.channelInactive(ctx);
    }

    private void handleMetadata(ChannelHandlerContext ctx, long requestId,
                                java.util.function.Supplier<VectorSchemaRoot> supplier) {
        long start = System.nanoTime();
        try {
            VectorSchemaRoot root = supplier.get();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            LOG.info("metadata ok: {} rows in {} ms", root.getRowCount(), elapsedMs);
            sendRows(ctx, requestId, root, true);
            root.close();
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            LOG.warn("metadata failed in {} ms", elapsedMs, e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("channel exception, closing", cause);
        ctx.close();
    }
}
