# 结果集服务端分页实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现结果集服务端分页(游标式、拉取式),让 JDBC `Statement.setFetchSize(n)` 落地,客户端内存只占一页,防大结果集 OOM。

**Architecture:** 服务端 `QueryExecutor` 新增 `executeCursor` 返回**不物化**的 `QueryResult.Cursor(CursorHandle)`,SessionHandler 用它建 `Paginator` 逐页切片;协议新增 `FetchRequest`/`CloseCursorRequest`,`ExecuteRequest` 加 `fetchSize`;客户端 `MiniDbResultSet` 读到底时经 `MiniDbClient.fetch` 拉下一页。`execute`(物化)保留给 30 个测试文件与 EXPLAIN,零测试 churn。

**Tech Stack:** JDK 17、Maven wrapper `./mvnw.cmd`、Netty、Apache Arrow、Calcite 1.42、JUnit 5。

**Spec:** `docs/superpowers/specs/2026-08-16-result-set-pagination-design.md`

## Global Constraints

- **JDK 17 必须**,`JAVA_HOME` 指向 JDK 17。
- 构建命令在 bash 下用 `./mvnw.cmd ...`(不是 `mvnw.cmd`、`mvn`、`cmd //c`)。
- 单模块测试:`./mvnw.cmd test -pl minidb-protocol` / `-pl minidb-server` / `-pl minidb-jdbc`;单测试类 `-Dtest=XxxTest`。
- **conventional commit**(`feat:`/`fix:`/`test:`/`docs:`/`refactor:`),不 amend、不 `--no-verify`;在 `master` 分支工作;小步提交。
- **代码给人读**:命名自解释(不用 `lk`/`rn` 这类缩写),注释解释 WHY 而非复述 WHAT。
- **测试用 JUnit 5 + `@TempDir` + `RootAllocator`**;断言关系/比例,浮点用 delta。
- **minidb-protocol 与现有 7 个物理算子尽量不改**——本次协议改动是**新增**消息/字段(向后兼容的增量),不动既有消息语义;算子层完全不动(分页是编排层能力)。
- 中文回复用户;代码/标识符/路径保持原文。

---

### Task 1: 协议层 — ExecuteRequest.fetchSize + FetchRequest + CloseCursorRequest

**Files:**
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/Message.java`
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/MessageType.java`
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/MessageEncoder.java`
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/MessageDecoder.java`
- Modify: `minidb-protocol/src/test/java/com/minidb/protocol/CodecTest.java`

**Interfaces:**
- Consumes: 无(协议层第一)。
- Produces:
  - `Message.ExecuteRequest(long requestId, String sql, int fetchSize)` + 2 参便捷构造 `(requestId, sql)`(fetchSize 默认 0)。
  - `Message.FetchRequest(long requestId, long cursorId, int maxRows)`
  - `Message.CloseCursorRequest(long cursorId)`
  - `MessageType.FETCH_REQUEST = 0x15`、`MessageType.CLOSE_CURSOR_REQUEST = 0x16`

- [ ] **Step 1: 改 Message.java**

在 `Message` 接口内:
1. `ExecuteRequest` 改为 3 字段,加便捷构造:
```java
record ExecuteRequest(long requestId, String sql, int fetchSize) implements Message {
    public ExecuteRequest(long requestId, String sql) {
        this(requestId, sql, 0);
    }
}
```
2. 在 `CloseRequest` 之后新增两条消息:
```java
record FetchRequest(long requestId, long cursorId, int maxRows) implements Message {
}

record CloseCursorRequest(long cursorId) implements Message {
}
```

- [ ] **Step 2: 改 MessageType.java**

在 `COLUMNS_REQUEST = 0x14;` 之后加:
```java
public static final byte FETCH_REQUEST = 0x15;
public static final byte CLOSE_CURSOR_REQUEST = 0x16;
```

- [ ] **Step 3: 改 MessageEncoder.java**

`ExecuteRequest` 分支改为写 4 字节 fetchSize(长度 `8 + 4 + sql.length + 4`):
```java
} else if (msg instanceof Message.ExecuteRequest r) {
    byte[] sql = r.sql().getBytes(StandardCharsets.UTF_8);
    out.writeByte(MessageType.EXECUTE_REQUEST);
    out.writeInt(8 + 4 + sql.length + 4);
    out.writeLong(r.requestId());
    out.writeInt(sql.length);
    out.writeBytes(sql);
    out.writeInt(r.fetchSize());
}
```
在 `Message.CloseRequest` 分支之后、`Message.ExecuteResponse` 之前新增两分支:
```java
} else if (msg instanceof Message.FetchRequest r) {
    out.writeByte(MessageType.FETCH_REQUEST);
    out.writeInt(8 + 8 + 4);
    out.writeLong(r.requestId());
    out.writeLong(r.cursorId());
    out.writeInt(r.maxRows());
} else if (msg instanceof Message.CloseCursorRequest r) {
    out.writeByte(MessageType.CLOSE_CURSOR_REQUEST);
    out.writeInt(8);
    out.writeLong(r.cursorId());
}
```

- [ ] **Step 4: 改 MessageDecoder.java**

`EXECUTE_REQUEST` 分支末尾读 fetchSize:
```java
case MessageType.EXECUTE_REQUEST -> {
    long requestId = in.readLong();
    int sqlLen = in.readInt();
    byte[] sql = new byte[sqlLen];
    in.readBytes(sql);
    int fetchSize = in.readInt();
    return new Message.ExecuteRequest(requestId,
            new String(sql, StandardCharsets.UTF_8), fetchSize);
}
```
在 `CLOSE_REQUEST` 分支之后新增:
```java
case MessageType.FETCH_REQUEST -> {
    long requestId = in.readLong();
    long cursorId = in.readLong();
    int maxRows = in.readInt();
    return new Message.FetchRequest(requestId, cursorId, maxRows);
}
case MessageType.CLOSE_CURSOR_REQUEST -> {
    long cursorId = in.readLong();
    return new Message.CloseCursorRequest(cursorId);
}
```

- [ ] **Step 5: 改 CodecTest.java**

`executeRequestRoundTrip` 改为用 3 参构造并断言 fetchSize:
```java
@Test
void executeRequestRoundTrip() {
    String sql = "SELECT * FROM t WHERE name = 'abc'";
    Message.ExecuteRequest out =
            (Message.ExecuteRequest) roundTrip(new Message.ExecuteRequest(42L, sql, 128));
    assertEquals(42L, out.requestId());
    assertEquals(sql, out.sql());
    assertEquals(128, out.fetchSize());
}
```
新增两个测试:
```java
@Test
void fetchRequestRoundTrip() {
    Message.FetchRequest out =
            (Message.FetchRequest) roundTrip(new Message.FetchRequest(9L, 42L, 100));
    assertEquals(9L, out.requestId());
    assertEquals(42L, out.cursorId());
    assertEquals(100, out.maxRows());
}

@Test
void closeCursorRequestRoundTrip() {
    Message.CloseCursorRequest out =
            (Message.CloseCursorRequest) roundTrip(new Message.CloseCursorRequest(42L));
    assertEquals(42L, out.cursorId());
}
```
`fragmentedFramesReassemble` 用了 2 参 `new Message.ExecuteRequest(1L, "SELECT 1")`,靠便捷构造仍编译,无需改。

- [ ] **Step 6: 跑协议测试**

Run: `./mvnw.cmd test -pl minidb-protocol`
Expected: 全绿(含新增 2 个 round-trip 测试)。

- [ ] **Step 7: 提交**

```bash
git add minidb-protocol/src/main/java/com/minidb/protocol/Message.java minidb-protocol/src/main/java/com/minidb/protocol/MessageType.java minidb-protocol/src/main/java/com/minidb/protocol/MessageEncoder.java minidb-protocol/src/main/java/com/minidb/protocol/MessageDecoder.java minidb-protocol/src/test/java/com/minidb/protocol/CodecTest.java
git commit -m "feat: 协议新增 FetchRequest/CloseCursorRequest + ExecuteRequest.fetchSize"
```

---

### Task 2: 服务端 — QueryResult.Cursor + CursorHandle + QueryExecutor.executeCursor

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/CursorHandle.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/QueryResult.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java`

**Interfaces:**
- Consumes: `BatchIterator`(`com.minidb.storage.common`)、`ExecContext`、`RowCopier.copyRow(VectorSchemaRoot,int,VectorSchemaRoot,int)`、`ArrowTypes.field(RelDataTypeField)`。
- Produces:
  - `CursorHandle(BatchIterator iterator, ExecContext context, Schema schema)` + `VectorSchemaRoot materialize()`
  - `QueryResult.Cursor(CursorHandle handle)`
  - `QueryExecutor.executeCursor(String sql, String currentSchema) → QueryResult`(SELECT 返回 Cursor)
  - `QueryExecutor.execute(String sql, String currentSchema) → QueryResult`(SELECT 返回物化的 Rows,行为不变)

- [ ] **Step 1: 写失败测试(CursorHandleTest.java)**

Create `minidb-server/src/test/java/com/minidb/server/exec/CursorHandleTest.java`:
```java
package com.minidb.server.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CursorHandleTest {

    @Test
    void executeReturnsRowsButCursorReturnsUnmaterialized(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        try {
            executor.execute("CREATE TABLE t (id INTEGER)");
            executor.execute("INSERT INTO t VALUES (1), (2), (3)");

            // execute() still materializes to Rows (compat for tests)
            QueryResult materialized = executor.execute("SELECT id FROM t ORDER BY id");
            assertTrue(materialized instanceof QueryResult.Rows);
            VectorSchemaRoot root = ((QueryResult.Rows) materialized).data();
            assertEquals(3, root.getRowCount());
            root.close();

            // executeCursor() returns an unmaterialized Cursor handle
            QueryResult cursor = executor.executeCursor("SELECT id FROM t ORDER BY id");
            assertTrue(cursor instanceof QueryResult.Cursor);
            VectorSchemaRoot viaHandle = ((QueryResult.Cursor) cursor).handle().materialize();
            assertEquals(3, viaHandle.getRowCount());
            viaHandle.close();
        } finally {
            storage.close();
            allocator.close();
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=CursorHandleTest`
Expected: 编译失败(`QueryResult.Cursor`/`CursorHandle`/`executeCursor` 不存在)。

- [ ] **Step 3: 新建 CursorHandle.java**

Create `minidb-server/src/main/java/com/minidb/server/exec/CursorHandle.java`:
```java
package com.minidb.server.exec;

import com.minidb.storage.common.BatchIterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A not-yet-consumed query result: the pull-mode iterator plus the execution
 * context it runs against. The server keeps this alive across fetch requests
 * (cursor paging); materialize() is the eager fallback used by the test-facing
 * {@link QueryExecutor#execute} entry point.
 */
public record CursorHandle(BatchIterator iterator, ExecContext context, Schema schema) {

    /** Pulls every remaining batch into a single owned root and closes the iterator. */
    public VectorSchemaRoot materialize() {
        VectorSchemaRoot merged = null;
        int dst = 0;
        try {
            while (iterator.hasNext()) {
                VectorSchemaRoot batch = iterator.next();
                if (merged == null) {
                    merged = VectorSchemaRoot.create(batch.getSchema(), context.allocator());
                    merged.allocateNew();
                }
                for (int i = 0; i < batch.getRowCount(); i++) {
                    RowCopier.copyRow(batch, i, merged, dst++);
                }
            }
        } finally {
            iterator.close();
        }
        if (merged == null) {
            return emptyRoot();
        }
        merged.setRowCount(dst);
        return merged;
    }

    private VectorSchemaRoot emptyRoot() {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, context.allocator());
        root.allocateNew();
        root.setRowCount(0);
        return root;
    }
}
```

- [ ] **Step 4: 改 QueryResult.java 加 Cursor 变体**

在 `Rows` record 之后加:
```java
record Cursor(CursorHandle handle) implements QueryResult {
}
```
(嵌套 record,sealed interface 无需显式 `permits`。)

- [ ] **Step 5: 改 QueryExecutor.java**

顶部加 import:
```java
import org.apache.arrow.vector.types.pojo.Schema;
```
把 `execute(String sql, String currentSchema)` 的方法体重构为委托 `executeCursor`:
```java
public QueryResult execute(String sql, String currentSchema) {
    QueryResult result = executeCursor(sql, currentSchema);
    if (result instanceof QueryResult.Cursor cursor) {
        return new QueryResult.Rows(cursor.handle().materialize());
    }
    return result;
}

/** Like {@link #execute}, but leaves SELECT results as an unmaterialized cursor for paging. */
public QueryResult executeCursor(String sql, String currentSchema) {
    String trimmed = sql.strip();
    QueryResult command = tryHandleCommand(trimmed, currentSchema);
    if (command != null) {
        return command;
    }
    SqlNode parsed = calcite.parse(trimmed);
    if (parsed instanceof SqlDdl ddl) {
        return handleDdl(ddl, currentSchema);
    }
    return executeQuery(trimmed, currentSchema);
}
```
把 `executeQuery` 的 SELECT 分支改为返回 Cursor(不再 `materialize`):
```java
private QueryResult executeQuery(String sql, String currentSchema) {
    RelNode plan = planner.plan(sql, currentSchema);
    ExecContext ctx = new ExecContext(storage, allocator, currentSchema);
    if (plan instanceof MiniDbModify modify) {
        try (BatchIterator it = modify.execute(ctx)) {
            while (it.hasNext()) {
                it.next();
            }
            return new QueryResult.Update(modify.affected());
        }
    }
    BatchIterator it = ((MiniDbRel) plan).execute(ctx);
    return new QueryResult.Cursor(new CursorHandle(it, ctx, schemaFromRowType(plan.getRowType())));
}
```
删除私有 `materialize(BatchIterator, RelNode)` 与 `emptyRoot(RelNode)`,替换为:
```java
private Schema schemaFromRowType(RelDataType rowType) {
    List<Field> fields = new ArrayList<>();
    for (RelDataTypeField f : rowType.getFieldList()) {
        fields.add(ArrowTypes.field(f));
    }
    return new Schema(fields);
}
```

- [ ] **Step 6: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=CursorHandleTest`
Expected: 通过(execute 仍物化 Rows,executeCursor 返回 Cursor)。

- [ ] **Step 7: 全量服务端测试回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 全绿(30 个测试文件依赖 `execute` 物化,行为不变;`QueryResult.Rows` 仍由 EXPLAIN 与 `execute` 产出)。

- [ ] **Step 8: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/CursorHandle.java minidb-server/src/main/java/com/minidb/server/exec/QueryResult.java minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java minidb-server/src/test/java/com/minidb/server/exec/CursorHandleTest.java
git commit -m "feat: QueryExecutor.executeCursor 返回不物化游标,execute 保留物化路径"
```

---

### Task 3: 服务端 — Paginator 跨批切片

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/Paginator.java`
- Create: `minidb-server/src/test/java/com/minidb/server/exec/PaginatorTest.java`

**Interfaces:**
- Consumes: `BatchIterator`、`RowCopier.copyRow`、`Schema`、`BufferAllocator`。
- Produces:
  - `Paginator(BatchIterator iterator, Schema schema, BufferAllocator allocator)`
  - `VectorSchemaRoot nextPage(int maxRows)`(返回新 root,0..maxRows 行;耗尽后返回 null)
  - `boolean isDone()`
  - `void close()`

- [ ] **Step 1: 写失败测试(PaginatorTest.java)**

Create `minidb-server/src/test/java/com/minidb/server/exec/PaginatorTest.java`:
```java
package com.minidb.server.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.storage.common.BatchIterator;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PaginatorTest {

    private final RootAllocator allocator = new RootAllocator();

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private static Schema schema() {
        return new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    private VectorSchemaRoot root(int... values) {
        IntVector v = new IntVector("id", allocator);
        v.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            v.setSafe(i, values[i]);
        }
        v.setValueCount(values.length);
        return new VectorSchemaRoot(List.of(v));
    }

    private static BatchIterator iterator(VectorSchemaRoot... batches) {
        return new BatchIterator() {
            int i = 0;
            @Override public boolean hasNext() { return i < batches.length; }
            @Override public VectorSchemaRoot next() { return batches[i++]; }
            @Override public void close() {}
        };
    }

    @Test
    void slicesAcrossBatchBoundaries() {
        Paginator p = new Paginator(iterator(root(1, 2, 3), root(4, 5)), schema(), allocator);
        VectorSchemaRoot page1 = p.nextPage(2);
        assertEquals(2, page1.getRowCount());
        assertEquals(1, ((IntVector) page1.getVector(0)).get(0));
        assertEquals(2, ((IntVector) page1.getVector(0)).get(1));
        assertFalse(p.isDone());
        page1.close();

        VectorSchemaRoot page2 = p.nextPage(2);
        assertEquals(2, page2.getRowCount());
        assertEquals(3, ((IntVector) page2.getVector(0)).get(0));
        assertEquals(4, ((IntVector) page2.getVector(0)).get(1));
        assertFalse(p.isDone());
        page2.close();

        VectorSchemaRoot page3 = p.nextPage(2);
        assertEquals(1, page3.getRowCount());
        assertEquals(5, ((IntVector) page3.getVector(0)).get(0));
        assertTrue(p.isDone());
        page3.close();
        p.close();
    }

    @Test
    void emptyInputReturnsSingleEmptyPage() {
        Paginator p = new Paginator(iterator(), schema(), allocator);
        VectorSchemaRoot page = p.nextPage(10);
        assertNotNull(page);
        assertEquals(0, page.getRowCount());
        assertEquals(1, page.getFieldVectors().size());
        assertTrue(p.isDone());
        page.close();
        assertNull(p.nextPage(10));
        p.close();
    }

    @Test
    void exactMultipleOfPageSize() {
        Paginator p = new Paginator(iterator(root(1, 2, 3, 4)), schema(), allocator);
        VectorSchemaRoot page1 = p.nextPage(2);
        assertEquals(2, page1.getRowCount());
        assertFalse(p.isDone());
        page1.close();
        VectorSchemaRoot page2 = p.nextPage(2);
        assertEquals(2, page2.getRowCount());
        assertTrue(p.isDone());
        page2.close();
        p.close();
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=PaginatorTest`
Expected: 编译失败(`Paginator` 不存在)。

- [ ] **Step 3: 新建 Paginator.java**

Create `minidb-server/src/main/java/com/minidb/server/exec/Paginator.java`:
```java
package com.minidb.server.exec;

import com.minidb.storage.common.BatchIterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Slices a pull-mode batch iterator into fixed-size pages for cursor paging.
 * Each page is a fresh, owned root (the caller serializes then closes it); the
 * input batches are closed as they are fully consumed. nextPage returns null
 * only after at least one page has been emitted.
 */
public final class Paginator implements AutoCloseable {

    private final BatchIterator iterator;
    private final Schema schema;
    private final BufferAllocator allocator;
    private VectorSchemaRoot current;
    private int offset;
    private boolean done;
    private boolean emitted;

    public Paginator(BatchIterator iterator, Schema schema, BufferAllocator allocator) {
        this.iterator = iterator;
        this.schema = schema;
        this.allocator = allocator;
    }

    public VectorSchemaRoot nextPage(int maxRows) {
        if (done && emitted) {
            return null;
        }
        VectorSchemaRoot out = VectorSchemaRoot.create(schema, allocator);
        out.allocateNew();
        int dst = 0;
        while (dst < maxRows) {
            if (current == null || offset >= current.getRowCount()) {
                if (current != null) {
                    current.close();
                    current = null;
                }
                if (iterator.hasNext()) {
                    current = iterator.next();
                    offset = 0;
                    continue;
                }
                done = true;
                break;
            }
            RowCopier.copyRow(current, offset, out, dst);
            offset++;
            dst++;
        }
        out.setValueCount(dst);
        emitted = true;
        return out;
    }

    public boolean isDone() {
        return done;
    }

    @Override
    public void close() {
        if (current != null) {
            current.close();
            current = null;
        }
        iterator.close();
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=PaginatorTest`
Expected: 通过(跨批切片 / 空输入 / 整数倍页)。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/Paginator.java minidb-server/src/test/java/com/minidb/server/exec/PaginatorTest.java
git commit -m "feat: Paginator 跨批切片出定长页(游标分页核心)"
```

---

### Task 4: 服务端 — SessionHandler 游标接线

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java`
- Create: `minidb-server/src/test/java/com/minidb/server/netty/SessionHandlerCursorTest.java`

**Interfaces:**
- Consumes: `QueryResult.Cursor`、`CursorHandle`、`Paginator`、`Message.FetchRequest`、`Message.CloseCursorRequest`。
- Produces: 服务端完整游标协议流程(execute 回首页 + lastBatch、fetch 回后续页、close/耗尽/断连关游标)。

- [ ] **Step 1: 写失败测试(SessionHandlerCursorTest.java)**

Create `minidb-server/src/test/java/com/minidb/server/netty/SessionHandlerCursorTest.java`:
```java
package com.minidb.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.protocol.Message;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.MetadataExecutor;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionHandlerCursorTest {

    private static int rowCount(byte[] data, BufferAllocator allocator) throws Exception {
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(data), allocator)) {
            reader.loadNextBatch();
            return reader.getVectorSchemaRoot().getRowCount();
        }
    }

    @Test
    void selectStreamsPagesAndClosesOnExhaustion(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        EmbeddedChannel ch = new EmbeddedChannel(
                new SessionHandler(executor, new MetadataExecutor(catalog, allocator)));
        try {
            ch.writeInbound(new Message.ExecuteRequest(1, "CREATE TABLE t (id INTEGER)"));
            ch.outboundMessages().poll(); // UpdateCount
            ch.writeInbound(new Message.ExecuteRequest(2, "INSERT INTO t VALUES (1), (2), (3)"));
            ch.outboundMessages().poll(); // UpdateCount
            ch.writeInbound(new Message.ExecuteRequest(3, "SELECT id FROM t ORDER BY id", 2));

            Message.ArrowBatch first = (Message.ArrowBatch) ch.outboundMessages().poll();
            assertEquals(3, first.requestId());
            assertFalse(first.lastBatch());
            assertEquals(2, rowCount(first.data(), allocator));

            ch.writeInbound(new Message.FetchRequest(10, 3, 2));
            Message.ArrowBatch second = (Message.ArrowBatch) ch.outboundMessages().poll();
            assertEquals(10, second.requestId());
            assertTrue(second.lastBatch());
            assertEquals(1, rowCount(second.data(), allocator));

            // fetching a cursor that is already exhausted reports an error
            ch.writeInbound(new Message.FetchRequest(11, 3, 2));
            assertTrue(ch.outboundMessages().poll() instanceof Message.ExecuteResponse);

            // closing an unknown cursor is a no-op (no response)
            ch.writeInbound(new Message.CloseCursorRequest(3));
            assertNull(ch.outboundMessages().poll());
        } finally {
            storage.close();
            allocator.close();
            ch.close();
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SessionHandlerCursorTest`
Expected: 编译失败(`ExecuteRequest` 3 参构造已存在但 SessionHandler 未处理 `Cursor`,测试里 `Message.ArrowBatch` 断言会因未实现而失败——此刻 `handleExecute` 对 `Cursor` 无分支,`executeCursor` 尚未被调用)。

- [ ] **Step 3: 改 SessionHandler.java**

顶部加 import:
```java
import com.minidb.server.exec.CursorHandle;
import com.minidb.server.exec.Paginator;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
```
类字段加:
```java
private static final int DEFAULT_FETCH_SIZE = 4096;
private final Map<Long, Paginator> cursors = new HashMap<>();
```
`handleExecute` 内:`executor.execute(req.sql(), currentSchema)` 改为 `executor.executeCursor(req.sql(), currentSchema)`,并在 `Rows` 分支之后新增 `Cursor` 分支:
```java
} else if (result instanceof QueryResult.Cursor cursor) {
    CursorHandle handle = cursor.handle();
    BufferAllocator allocator = handle.context().allocator();
    Paginator paginator = new Paginator(handle.iterator(), handle.schema(), allocator);
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
    }
}
```
`sendRows` 加 `lastBatch` 参数(现 `ArrowBatch(requestId, true, ...)` 改传 `lastBatch`):
```java
private void sendRows(ChannelHandlerContext ctx, long requestId, VectorSchemaRoot root, boolean lastBatch) {
    try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(
                root, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        ctx.writeAndFlush(new Message.ArrowBatch(requestId, lastBatch, out.toByteArray()));
    } catch (Exception e) {
        LOG.warn("failed to send rows for request {}", requestId, e);
        String message = e.getMessage() == null ? e.toString() : e.getMessage();
        ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
    }
}
```
两处既有调用补 `true`:EXPLAIN 的 `sendRows(ctx, req.requestId(), rows.data(), true)` 与 `handleMetadata` 的 `sendRows(ctx, requestId, root, true)`。

`channelRead0` 加两个分支(在 `CloseRequest` 分支之前):
```java
} else if (msg instanceof Message.FetchRequest req) {
    handleFetch(ctx, req);
} else if (msg instanceof Message.CloseCursorRequest req) {
    handleCloseCursor(req);
}
```
新增方法:
```java
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
        sendRows(ctx, req.requestId(), page, last);
        page.close();
        if (last) {
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
```
新增断连兜底:
```java
@Override
public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    for (Paginator p : cursors.values()) {
        p.close();
    }
    cursors.clear();
    super.channelInactive(ctx);
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SessionHandlerCursorTest`
Expected: 通过(execute 回首页 lastBatch=false + 2 行,fetch 回尾页 lastBatch=true + 1 行,再 fetch 报错,close 无响应)。

- [ ] **Step 5: 全量服务端测试回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 全绿(现有 SessionHandlerSchemaTest 用 2 参 ExecuteRequest + 非 SELECT,不受影响)。

- [ ] **Step 6: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java minidb-server/src/test/java/com/minidb/server/netty/SessionHandlerCursorTest.java
git commit -m "feat: SessionHandler 游标分页(FetchRequest/CloseCursorRequest/断连清理)"
```

---

### Task 5: 客户端 — MiniDbClient / MiniDbStatement / MiniDbResultSet 游标化

**Files:**
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java`
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbStatement.java`
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSet.java`

**Interfaces:**
- Consumes: `Message.ExecuteRequest(requestId, sql, fetchSize)`、`Message.FetchRequest`、`Message.CloseCursorRequest`。
- Produces:
  - `MiniDbClient.execute(String sql, int fetchSize) → ClientResult`(SELECT 返回 `ClientResult.Cursor`)
  - `MiniDbClient.fetch(long cursorId, int maxRows) → ClientResult.Rows`
  - `MiniDbClient.closeCursor(long cursorId)`
  - `MiniDbStatement.setFetchSize/getFetchSize`(落地)+ `DEFAULT_FETCH_SIZE=4096`
  - `MiniDbResultSet(MiniDbStatement, MiniDbClient, ClientResult.Cursor)`(游标模式)+ 保留 2 参构造(物化模式)

- [ ] **Step 1: 改 MiniDbClient.java**

`ClientResult` sealed interface 改为:
```java
public sealed interface ClientResult {
    record Cursor(long cursorId, int fetchSize, VectorSchemaRoot firstPage,
                  boolean lastBatch) implements ClientResult {
    }

    record Rows(VectorSchemaRoot data, boolean lastBatch) implements ClientResult {
    }

    record Update(long count) implements ClientResult {
    }
}
```
`execute` 改签名并加游标包装:
```java
public ClientResult execute(String sql, int fetchSize) throws SQLException {
    if (!connected) {
        throw new SQLException("connection is closed");
    }
    long id = nextRequestId.getAndIncrement();
    CompletableFuture<ClientResult> fut = new CompletableFuture<>();
    pending.put(id, fut);
    if (!connected) {
        pending.remove(id, fut);
        throw new SQLException("connection is closed");
    }
    try {
        channel.writeAndFlush(new Message.ExecuteRequest(id, sql, fetchSize)).sync();
    } catch (Exception e) {
        pending.remove(id, fut);
        throw new SQLException("failed to send request", e);
    }
    try {
        ClientResult result = fut.get(timeoutSeconds, TimeUnit.SECONDS);
        if (result instanceof ClientResult.Rows rows) {
            return new ClientResult.Cursor(id, fetchSize, rows.data(), rows.lastBatch());
        }
        return result;
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted during execute", e);
    } catch (java.util.concurrent.TimeoutException e) {
        throw new SQLException("timeout waiting for server response");
    } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof SQLException sqle) {
            throw sqle;
        }
        throw new SQLException(cause != null ? cause.getMessage() : "query failed",
                cause != null ? cause : e);
    } finally {
        pending.remove(id, fut);
    }
}
```
在 `execute` 之后新增 `fetch` / `closeCursor`:
```java
public ClientResult.Rows fetch(long cursorId, int maxRows) throws SQLException {
    if (!connected) {
        throw new SQLException("connection is closed");
    }
    long id = nextRequestId.getAndIncrement();
    CompletableFuture<ClientResult> fut = new CompletableFuture<>();
    pending.put(id, fut);
    if (!connected) {
        pending.remove(id, fut);
        throw new SQLException("connection is closed");
    }
    try {
        channel.writeAndFlush(new Message.FetchRequest(id, cursorId, maxRows)).sync();
    } catch (Exception e) {
        pending.remove(id, fut);
        throw new SQLException("failed to send fetch", e);
    }
    try {
        ClientResult result = fut.get(timeoutSeconds, TimeUnit.SECONDS);
        if (result instanceof ClientResult.Rows rows) {
            return rows;
        }
        throw new SQLException("unexpected response to fetch");
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted during fetch", e);
    } catch (java.util.concurrent.TimeoutException e) {
        throw new SQLException("timeout waiting for server response");
    } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof SQLException sqle) {
            throw sqle;
        }
        throw new SQLException(cause != null ? cause.getMessage() : "fetch failed",
                cause != null ? cause : e);
    } finally {
        pending.remove(id, fut);
    }
}

public void closeCursor(long cursorId) {
    if (connected && channel != null) {
        channel.writeAndFlush(new Message.CloseCursorRequest(cursorId));
    }
}
```
`ResponseCollector` 的 `ArrowBatch` 分支改为传 `lastBatch`:
```java
f.complete(new ClientResult.Rows(arrowDecoder.decode(b.data()), b.lastBatch()));
```
(`sendMetadata` 里 `rows.data()` 仍编译;`ClientResult.Rows` 现在有两个字段,`data()` 不变。)

- [ ] **Step 2: 改 MiniDbStatement.java**

加字段与常量:
```java
private static final int DEFAULT_FETCH_SIZE = 4096;
private int fetchSize = 0;
```
`setFetchSize`/`getFetchSize` 落地(替换现有 no-op 两处):
```java
@Override
public void setFetchSize(int rows) {
    this.fetchSize = rows;
}

@Override
public int getFetchSize() {
    return fetchSize;
}
```
`execute` 内改调并分派 Cursor:
```java
MiniDbClient.ClientResult result = client.execute(sql, effectiveFetchSize());
if (result instanceof MiniDbClient.ClientResult.Cursor cursor) {
    current = new MiniDbResultSet(this, client, cursor);
    updateCount = -1;
    return true;
} else {
    MiniDbClient.ClientResult.Update update = (MiniDbClient.ClientResult.Update) result;
    updateCount = update.count();
    return false;
}
```
类末尾加私有方法:
```java
private int effectiveFetchSize() {
    return fetchSize > 0 ? fetchSize : DEFAULT_FETCH_SIZE;
}
```

- [ ] **Step 3: 改 MiniDbResultSet.java**

字段区替换(去 `root` 的 `final`,加游标字段):
```java
private final MiniDbStatement statement;
private final MiniDbClient client;
private final java.sql.ResultSetMetaData metaData;
private VectorSchemaRoot root;
private int cursor = -1;
private boolean wasNull;
private boolean closed;
private long cursorId;
private int fetchSize;
private boolean lastBatch;
private int rowNumber;
```
两个构造器:
```java
public MiniDbResultSet(MiniDbStatement statement, VectorSchemaRoot root) {
    this.statement = statement;
    this.client = null;
    this.root = root;
    this.lastBatch = true;
    this.metaData = new MiniDbResultSetMetaData(root);
}

public MiniDbResultSet(MiniDbStatement statement, MiniDbClient client,
                       MiniDbClient.ClientResult.Cursor cursor) {
    this.statement = statement;
    this.client = client;
    this.root = cursor.firstPage();
    this.cursorId = cursor.cursorId();
    this.fetchSize = cursor.fetchSize();
    this.lastBatch = cursor.lastBatch();
    this.metaData = new MiniDbResultSetMetaData(this.root);
}
```
`next()` 改为跨页拉取:
```java
@Override
public boolean next() throws SQLException {
    checkClosed();
    while (true) {
        cursor++;
        if (cursor < root.getRowCount()) {
            rowNumber++;
            return true;
        }
        if (lastBatch) {
            return false;
        }
        MiniDbClient.ClientResult.Rows page = client.fetch(cursorId, fetchSize);
        root.close();
        root = page.data();
        lastBatch = page.lastBatch();
        cursor = -1;
    }
}
```
`getMetaData()` 返回缓存:
```java
@Override
public ResultSetMetaData getMetaData() {
    return metaData;
}
```
`getRow()` 改为绝对行号:
```java
@Override
public int getRow() {
    return rowNumber;
}
```
`close()` 加游标释放:
```java
@Override
public void close() {
    if (!closed) {
        closed = true;
        root.close();
        if (cursorId != 0 && !lastBatch && client != null) {
            client.closeCursor(cursorId);
        }
    }
}
```

- [ ] **Step 4: 跑客户端测试回归(应保持绿)**

Run: `./mvnw.cmd test -pl minidb-jdbc`
Expected: 全绿。理由:小结果集经游标单页返回(`lastBatch=true`),`next()` 行为与原来一致;元数据路径(`getTables/getColumns/getSchemas` → `sendMetadata` → `Rows`)与 2 参构造不变;`MiniDbResultSetMetaDataTest` 用 2 参构造不受影响。

- [ ] **Step 5: 提交**

```bash
git add minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbStatement.java minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSet.java
git commit -m "feat: JDBC 客户端游标分页(setFetchSize 落地,ResultSet 跨页拉取)"
```

---

### Task 6: 端到端分页集成测试

**Files:**
- Create: `minidb-jdbc/src/test/java/com/minidb/jdbc/ResultSetPaginationTest.java`

**Interfaces:**
- Consumes: 已完成的游标协议 + JDBC `Statement.setFetchSize`。
- Produces: 端到端分页行为验证(多页 / 默认页 / 元数据稳定 / 提前 close / 大表流式)。

- [ ] **Step 1: 新建 ResultSetPaginationTest.java**

Create `minidb-jdbc/src/test/java/com/minidb/jdbc/ResultSetPaginationTest.java`:
```java
package com.minidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.server.MiniDbServer;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ResultSetPaginationTest {

    static MiniDbServer server;
    static String url;

    @BeforeAll
    static void startServer() throws Exception {
        server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-paging"));
        url = "jdbc:minidb://127.0.0.1:" + server.port();
    }

    @AfterAll
    static void stopServer() {
        server.close();
    }

    @Test
    void setFetchSizePaginatesAcrossPages() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE p (id INTEGER)");
            s.executeUpdate("INSERT INTO p VALUES (1), (2), (3), (4), (5)");
            s.setFetchSize(2);
            try (ResultSet rs = s.executeQuery("SELECT id FROM p ORDER BY id")) {
                int count = 0;
                int sum = 0;
                while (rs.next()) {
                    count++;
                    sum += rs.getInt(1);
                }
                assertEquals(5, count);
                assertEquals(15, sum);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void defaultFetchSizeReturnsAllRows() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE d (id INTEGER)");
            s.executeUpdate("INSERT INTO d VALUES (1), (2), (3)");
            try (ResultSet rs = s.executeQuery("SELECT id FROM d ORDER BY id")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(3, count);
            }
        }
    }

    @Test
    void metadataStableAcrossPages() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE m (id INTEGER)");
            s.executeUpdate("INSERT INTO m VALUES (1), (2), (3)");
            s.setFetchSize(2);
            try (ResultSet rs = s.executeQuery("SELECT id, id * 2 AS doubled FROM m ORDER BY id")) {
                assertEquals(2, rs.getMetaData().getColumnCount());
                assertEquals("id", rs.getMetaData().getColumnName(1));
                assertEquals("doubled", rs.getMetaData().getColumnName(2));
                while (rs.next()) {
                    // drive through all pages; metadata must remain readable
                }
                assertEquals(2, rs.getMetaData().getColumnCount());
            }
        }
    }

    @Test
    void earlyCloseDoesNotThrow() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE c (id INTEGER)");
            s.executeUpdate("INSERT INTO c VALUES (1), (2), (3), (4)");
            s.setFetchSize(2);
            try (ResultSet rs = s.executeQuery("SELECT id FROM c ORDER BY id")) {
                assertTrue(rs.next());
                assertTrue(rs.next());
                // closes early, before exhausting the cursor
            }
            // server must have released the cursor; no assertion beyond no-throw
        }
    }

    @Test
    void largeResultStreamsInPages() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE big (id INTEGER)");
            StringBuilder insert = new StringBuilder("INSERT INTO big VALUES ");
            for (int i = 1; i <= 4100; i++) {
                if (i > 1) {
                    insert.append(',');
                }
                insert.append('(').append(i).append(')');
            }
            s.executeUpdate(insert.toString());
            // default fetchSize (4096) → two pages for 4100 rows
            try (ResultSet rs = s.executeQuery("SELECT id FROM big ORDER BY id")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(4100, count);
            }
        }
    }
}
```

- [ ] **Step 2: 跑集成测试**

Run: `./mvnw.cmd test -pl minidb-jdbc -Dtest=ResultSetPaginationTest`
Expected: 通过(多页 2/2/1、默认页、元数据跨页稳定、提前 close、4100 行默认 4096 分页流式)。

- [ ] **Step 3: 全量测试回归(三模块)**

Run: `./mvnw.cmd test`
Expected: 全绿(minidb-protocol / minidb-server / minidb-jdbc)。

- [ ] **Step 4: 提交**

```bash
git add minidb-jdbc/src/test/java/com/minidb/jdbc/ResultSetPaginationTest.java
git commit -m "test: 结果集分页端到端集成测试"
```

---

## Self-Review Notes(执行前已核对)

- **Spec 覆盖**:协议(任务 1)、QueryExecutor 游标(任务 2)、Paginator(任务 3)、SessionHandler 接线与断连清理(任务 4)、客户端三件套(任务 5)、集成测试(任务 6)——spec 每节均有对应任务;`ResultSet.setFetchSize` 保持 no-op 的「不在本范围」无需任务。
- **类型一致性**:`CursorHandle(BatchIterator, ExecContext, Schema)` 在任务 2 定义,任务 3/4 消费;`Paginator.nextPage/isDone/close` 在任务 3 定义,任务 4 消费;`Message.FetchRequest/CloseCursorRequest/ExecuteRequest.fetchSize` 在任务 1 定义,任务 4/5 消费;`ClientResult.Cursor(firstPage, lastBatch)` 在任务 5 定义并同任务消费。全部前后一致。
- **无占位符**:每个代码步骤均有完整源码,测试有完整断言。
