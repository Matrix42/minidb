# MiniDbClient Multiplexed Single-Channel + requestId Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `MiniDbClient` so responses are routed by `requestId` to per-request `CompletableFuture`s, and so a server disconnect fails pending requests immediately instead of hanging until the 30s timeout.

**Architecture:** One TCP socket per `MiniDbConnection`. Each `execute()` allocates a unique `requestId`, registers a `CompletableFuture<ClientResult>` in a `ConcurrentHashMap<Long, CompletableFuture<ClientResult>> pending`, and blocks on that future (not a shared queue). A rewritten `ResponseCollector` routes inbound messages by `requestId`, and a new `channelInactive` override fails all pending futures when the server dies. Concurrent statements interleave safely on the shared wire.

**Tech Stack:** Java 17, Netty 4, Apache Arrow, JUnit 5, Maven (wrapper `./mvnw.cmd`).

## Global Constraints

- Module under test: `minidb-jdbc`. Run its tests with `./mvnw.cmd -pl minidb-jdbc -am test` (the `-am` also builds `minidb-server`, which the JDBC tests depend on at runtime via `MiniDbServer`).
- The protocol `MessageEncoder`/`MessageDecoder` already thread `requestId` through `ExecuteResponse`, `ArrowBatch`, `UpdateCount`. **Do not modify** `minidb-protocol` or `minidb-server`.
- Server-side per-socket serialization is preserved: Netty binds one `SessionHandler` per socket and runs that channel's events on one worker thread, so requests on one socket stay serialized server-side. Multiplexing the client does not introduce server-side races.
- TDD strictly: every production change has a failing test first, watched fail, then minimal code, then green. No code before the test.
- Commit after each task. Use the existing commit-message style: `feat:`, `fix:`, `test:`, `docs:`.
- Existing tests `PersistenceTest` (server restart with reconnect) and `ArrowResultDecoderTest` (e2e queries) must stay green throughout.
- `ClientResult` sealed interface and its `Rows`/`Update` records stay unchanged.

## File Structure

- **Modify** `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java` — rewrite `execute()`, `poll()` → removed, `connect()`, `close()`, and `ResponseCollector`. Fields change: drop `BlockingQueue responses` and plain `long nextRequestId`; add `ConcurrentHashMap<Long, CompletableFuture<ClientResult>> pending`, `AtomicLong nextRequestId`, `CompletableFuture<Void> handshakeFuture`, `volatile boolean connected`. Add an injectable timeout field.
- **Create** `minidb-jdbc/src/test/java/com/minidb/jdbc/ClientLifecycleTest.java` — new test class for disconnect fast-fail and concurrent-statement cross-talk (Task 1 & Task 2 respectively). Kept separate from `ArrowResultDecoderTest`/`PersistenceTest` so the existing e2e suite stays focused on query correctness.
- `MiniDbStatement`, `MiniDbPreparedStatement`, `MiniDbConnection`, `MiniDbDriver` — **unchanged**; they already call `client.execute(sql)`.

### Pending-map ownership rule (memorize this; it drives Task 1's correctness)

A `pending` entry is removed by exactly one of three paths, and they must not clobber each other:
1. **Router on success** (`channelRead0`): `pending.remove(requestId)` after completing the future.
2. **`channelInactive` / `exceptionCaught` on failure**: snapshot `pending.values()`, `pending.clear()`, fail each snapshot future. (Clearing means a `put` that races in afterward sees `connected == false` and fast-fails via the execute race-guard rather than registering a live orphan.)
3. **`execute` `finally` backstop**: `pending.remove(id, fut)` — atomic identity check; a no-op if path 1 or 2 already removed the entry, or if a different future now occupies the slot (shouldn't happen, but the identity check makes it safe).

---

### Task 1: Rewrite `MiniDbClient` — requestId routing + disconnect fast-fail

This is one task because the routing map, the future-based `execute()`, and the `channelInactive` fan-out are mutually dependent — you cannot ship one without the others (the old shared-queue path must be fully replaced). Tests are split into TDD steps below.

**Files:**
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java` (full rewrite of the body; keep `ClientResult` interface and `connect`/`execute`/`close` signatures)
- Test: `minidb-jdbc/src/test/java/com/minidb/jdbc/ClientLifecycleTest.java` (create)

**Interfaces:**
- Consumes: `Message.HandshakeAck`, `Message.ExecuteResponse` (`.requestId()`, `.ok()`, `.error()`), `Message.UpdateCount` (`.requestId()`, `.count()`), `Message.ArrowBatch` (`.requestId()`, `.lastBatch()`, `.data()`), `Protocol.VERSION`, `MessageDecoder`, `MessageEncoder`, `Protocol`, `Message.ExecuteRequest`, `Message.CloseRequest`, `ArrowStreamReader` (existing `readArrow` helper).
- Produces: `MiniDbClient` with the same public surface (`connect(host, port)`, `ClientResult execute(sql) throws SQLException`, `close()`), plus a package-private constructor `MiniDbClient(long timeoutSeconds)` for tests. `MiniDbConnection`/`MiniDbStatement` are unchanged.

- [ ] **Step 1: Write the failing disconnect test**

Create `minidb-jdbc/src/test/java/com/minidb/jdbc/ClientLifecycleTest.java`:

```java
package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientLifecycleTest {

    @Test
    void restartFailsOpenExecuteFast() throws Exception {
        MiniDbServer server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-restart"));
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        Connection c = DriverManager.getConnection(url);
        Statement s = c.createStatement();

        server.close(); // kill the server; the socket goes away

        long start = System.nanoTime();
        SQLException ex = assertThrows(SQLException.class,
                () -> s.execute("CREATE TABLE gone (id INTEGER)"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // Fast-fail must beat the 30s timeout by a wide margin.
        assertTrue(elapsedMs < 5_000,
                "expected fast-fail < 5000ms, got " + elapsedMs);
        assertTrue(ex.getMessage() != null && !ex.getMessage().contains("timeout"),
                "should report connection closed, not timeout: " + ex.getMessage());

        c.close(); // client close must be safe even on a dead connection
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw.cmd -pl minidb-jdbc -am test -Dtest=ClientLifecycleTest#restartFailsOpenExecuteFast`
Expected: FAIL. Current behavior blocks ~30s then throws `SQLException: timeout waiting for server response`. (The `elapsedMs < 5_000` assertion fails.)

- [ ] **Step 3: Rewrite `MiniDbClient` body**

Replace the entire contents of `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java` with:

```java
package com.minidb.jdbc;

import com.minidb.protocol.Message;
import com.minidb.protocol.MessageDecoder;
import com.minidb.protocol.MessageEncoder;
import com.minidb.protocol.Protocol;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

public class MiniDbClient implements AutoCloseable {

    public sealed interface ClientResult {
        record Rows(VectorSchemaRoot data) implements ClientResult {
        }

        record Update(long count) implements ClientResult {
        }
    }

    private static final long DEFAULT_TIMEOUT_SECONDS = 30;

    private final long timeoutSeconds;
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final BufferAllocator allocator = new RootAllocator();
    private final Map<Long, CompletableFuture<ClientResult>> pending =
            new ConcurrentHashMap<>();
    private final AtomicLong nextRequestId = new AtomicLong(1);
    private volatile boolean connected = false;

    private Channel channel;

    public MiniDbClient() {
        this(DEFAULT_TIMEOUT_SECONDS);
    }

    MiniDbClient(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public void connect(String host, int port) throws SQLException {
        CompletableFuture<Void> handshake = new CompletableFuture<>();
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new MessageDecoder());
                            ch.pipeline().addLast(new MessageEncoder());
                            ch.pipeline().addLast(
                                    new ResponseCollector(handshake, pending,
                                            MiniDbClient.this::markDisconnected,
                                            MiniDbClient.this::readArrow));
                        }
                    });
            channel = bootstrap.connect(host, port).sync().channel();
            channel.writeAndFlush(new Message.Handshake(Protocol.VERSION)).sync();
            handshake.get(timeoutSeconds, TimeUnit.SECONDS);
            connected = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted during connect", e);
        } catch (java.util.concurrent.TimeoutException e) {
            channel.close();
            throw new SQLException("handshake timeout", e);
        } catch (ExecutionException e) {
            channel.close();
            throw new SQLException("handshake failed: " + e.getCause().getMessage(),
                    e.getCause());
        }
    }

    public ClientResult execute(String sql) throws SQLException {
        if (!connected) {
            throw new SQLException("connection is closed");
        }
        long id = nextRequestId.getAndIncrement();
        CompletableFuture<ClientResult> fut = new CompletableFuture<>();
        pending.put(id, fut);
        // Race guard: the channel may have died between the connected check
        // and the put. If so, channelInactive already cleared pending and
        // missed our entry — fail it ourselves.
        if (!connected) {
            pending.remove(id, fut);
            throw new SQLException("connection is closed");
        }
        try {
            channel.writeAndFlush(new Message.ExecuteRequest(id, sql)).sync();
        } catch (Exception e) {
            pending.remove(id, fut);
            throw new SQLException("failed to send request", e);
        }
        try {
            return fut.get(timeoutSeconds, TimeUnit.SECONDS);
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

    private void markDisconnected() {
        connected = false;
    }

    @Override
    public void close() {
        connected = false;
        if (channel != null) {
            channel.writeAndFlush(new Message.CloseRequest());
            channel.close();
        }
        group.shutdownGracefully();
        allocator.close();
        failAllPending("connection is closed");
    }

    private void failAllPending(String reason) {
        for (CompletableFuture<ClientResult> f : new ArrayList<>(pending.values())) {
            f.completeExceptionally(new SQLException(reason));
        }
        pending.clear();
    }

    private VectorSchemaRoot readArrow(byte[] data) throws SQLException {
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(data), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot source = reader.getVectorSchemaRoot();
            VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
            org.apache.arrow.vector.ipc.message.ArrowRecordBatch recordBatch =
                    new VectorUnloader(source).getRecordBatch();
            new VectorLoader(copy).load(recordBatch);
            recordBatch.close();
            return copy;
        } catch (IOException e) {
            throw new SQLException("failed to decode arrow result", e);
        }
    }

    /**
     * Routes inbound messages to the per-request future, and fans connection
     * loss out to all pending futures so execute() fails fast instead of
     * blocking until the timeout.
     */
    private static class ResponseCollector extends SimpleChannelInboundHandler<Message> {
        // readArrow declares `throws SQLException` (checked), so it cannot target
        // java.util.function.Function (whose apply declares no checked exceptions).
        // Use a custom functional interface that propagates SQLException.
        @FunctionalInterface
        interface ArrowDecoder {
            VectorSchemaRoot decode(byte[] data) throws SQLException;
        }

        private final CompletableFuture<Void> handshake;
        private final Map<Long, CompletableFuture<ClientResult>> pending;
        private final Runnable onDisconnect;
        private final ArrowDecoder arrowDecoder;

        ResponseCollector(CompletableFuture<Void> handshake,
                           Map<Long, CompletableFuture<ClientResult>> pending,
                           Runnable onDisconnect,
                           ArrowDecoder arrowDecoder) {
            this.handshake = handshake;
            this.pending = pending;
            this.onDisconnect = onDisconnect;
            this.arrowDecoder = arrowDecoder;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            if (msg instanceof Message.HandshakeAck) {
                handshake.complete(null);
                return;
            }
            if (msg instanceof Message.ExecuteResponse r) {
                CompletableFuture<ClientResult> f = pending.remove(r.requestId());
                if (f == null) {
                    return; // late/orphan response, drop it
                }
                if (r.ok()) {
                    f.complete(new ClientResult.Update(0));
                } else {
                    f.completeExceptionally(new SQLException(r.error()));
                }
                return;
            }
            if (msg instanceof Message.UpdateCount u) {
                CompletableFuture<ClientResult> f = pending.remove(u.requestId());
                if (f != null) {
                    f.complete(new ClientResult.Update(u.count()));
                }
                return;
            }
            if (msg instanceof Message.ArrowBatch b) {
                CompletableFuture<ClientResult> f = pending.remove(b.requestId());
                if (f == null) {
                    return;
                }
                try {
                    f.complete(new ClientResult.Rows(arrowDecoder.decode(b.data())));
                } catch (SQLException e) {
                    f.completeExceptionally(e);
                }
                return;
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            onDisconnect.run();
            for (CompletableFuture<ClientResult> f : new ArrayList<>(pending.values())) {
                f.completeExceptionally(new SQLException("connection closed"));
            }
            pending.clear();
            if (!handshake.isDone()) {
                handshake.completeExceptionally(new SQLException("connection closed"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            onDisconnect.run();
            SQLException sqle = new SQLException("connection error", cause);
            for (CompletableFuture<ClientResult> f : new ArrayList<>(pending.values())) {
                f.completeExceptionally(sqle);
            }
            pending.clear();
            if (!handshake.isDone()) {
                handshake.completeExceptionally(sqle);
            }
            ctx.close();
        }
    }
}
```

- [ ] **Step 4: Run the disconnect test to verify it passes**

Run: `./mvnw.cmd -pl minidb-jdbc -am test -Dtest=ClientLifecycleTest#restartFailsOpenExecuteFast`
Expected: PASS, in well under 5s.

- [ ] **Step 5: Run the full existing JDBC suite to verify no regression**

Run: `./mvnw.cmd -pl minidb-jdbc -am test`
Expected: `PersistenceTest`, `ArrowResultDecoderTest`, and `ClientLifecycleTest#restartFailsOpenExecuteFast` all pass.

- [ ] **Step 6: Commit**

```bash
git add minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java \
        minidb-jdbc/src/test/java/com/minidb/jdbc/ClientLifecycleTest.java
git commit -m "fix: fast-fail client execute on server disconnect via requestId routing"
```

---

### Task 2: Cross-talk regression test for concurrent statements

**Files:**
- Test: `minidb-jdbc/src/test/java/com/minidb/jdbc/ClientLifecycleTest.java` (append a test)

**Interfaces:**
- Consumes: `MiniDbClient` from Task 1 (same `execute` signature). `MiniDbServer` for an in-process server.

- [ ] **Step 1: Write the failing cross-talk test**

Append to `ClientLifecycleTest.java` (add these imports to the existing file: `java.util.concurrent.CountDownLatch`, `java.util.concurrent.atomic.AtomicInteger`, `java.util.ArrayList`, `java.util.List`):

```java
@Test
void concurrentStatementsDoNotCrossTalk() throws Exception {
    MiniDbServer server = new MiniDbServer();
    server.start(0, Files.createTempDirectory("minidb-xtalk"));
    String url = "jdbc:minidb://127.0.0.1:" + server.port();
    java.sql.Connection c = DriverManager.getConnection(url);
    try (java.sql.Statement s = c.createStatement()) {
        s.execute("CREATE TABLE a (id INTEGER)");
        s.executeUpdate("INSERT INTO a VALUES (1), (2), (3)");
        s.execute("CREATE TABLE b (id INTEGER)");
        s.executeUpdate("INSERT INTO b VALUES (10), (20)");
    }

    int threads = 8;
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    AtomicInteger errors = new AtomicInteger();
    List<Integer> aSums = java.util.Collections.synchronizedList(new ArrayList<>());
    List<Integer> bSums = java.util.Collections.synchronizedList(new ArrayList<>());

    for (int i = 0; i < threads; i++) {
        int which = i % 2;
        Thread t = new Thread(() -> {
            try (java.sql.Statement s = c.createStatement();
                 java.sql.ResultSet rs = s.executeQuery(
                         which == 0
                                 ? "SELECT id FROM a ORDER BY id"
                                 : "SELECT id FROM b ORDER BY id")) {
                start.await();
                int sum = 0;
                while (rs.next()) {
                    sum += rs.getInt(1);
                }
                (which == 0 ? aSums : bSums).add(sum);
            } catch (Exception e) {
                errors.incrementAndGet();
            } finally {
                done.countDown();
            }
        });
        t.start();
    }

    start.countDown();
    assertTrue(done.await(60, TimeUnit.SECONDS), "threads did not finish");
    assertEquals(0, errors.get(), "some threads threw");

    // Each a-thread must see rows summing to 1+2+3=6; each b-thread 10+20=30.
    assertEquals(threads / 2, aSums.size(), "a-thread count");
    assertEquals(threads / 2, bSums.size(), "b-thread count");
    for (Integer v : aSums) {
        assertEquals(6, v, "a-thread got wrong rows (cross-talk?)");
    }
    for (Integer v : bSums) {
        assertEquals(30, v, "b-thread got wrong rows (cross-talk?)");
    }

    c.close();
    server.close();
}
```

Also add the missing imports at the top of the file: `java.util.concurrent.TimeUnit`, `static org.junit.jupiter.api.Assertions.assertEquals`, `static org.junit.jupiter.api.Assertions.assertTrue`.

- [ ] **Step 2: Run the test — verify it passes immediately (Task 1 already fixed the defect)**

Run: `./mvnw.cmd -pl minidb-jdbc -am test -Dtest=ClientLifecycleTest#concurrentStatementsDoNotCrossTalk`
Expected: PASS. This test is a **regression guard**: it would fail against the *old* shared-queue client (cross-talk), but Task 1's rewrite already routes by `requestId`, so it should pass now. Run it to confirm the new routing holds under concurrency. If it fails, that's a real defect in Task 1's routing — investigate via systematic-debugging before "fixing" the test.

- [ ] **Step 3: Run the whole JDBC suite once more**

Run: `./mvnw.cmd -pl minidb-jdbc -am test`
Expected: all green — `ArrowResultDecoderTest`, `PersistenceTest`, and both `ClientLifecycleTest` tests.

- [ ] **Step 4: Commit**

```bash
git add minidb-jdbc/src/test/java/com/minidb/jdbc/ClientLifecycleTest.java
git commit -m "test: concurrent statements do not cross-talk via shared connection"
```

---

## Self-Review

**1. Spec coverage:**
- Disconnect fast-fail → Task 1 Steps 1-4 (the `restartFailsOpenExecuteFast` test + `channelInactive` rewrite). ✓
- requestId routing / cross-talk fix → Task 1 Step 3 (router by `requestId`) + Task 2 (concurrency guard). ✓
- Race guard (put-after-clear orphan) → Task 1 Step 3 `execute()` re-checks `connected` after `put`; `channelInactive` clears `pending`. ✓
- Three-path ownership (router / failure / finally) → Task 1 Step 3: router `pending.remove`, `channelInactive`/`exceptionCaught` snapshot+clear, `execute` `finally` `pending.remove(id, fut)`. ✓
- Injectable timeout for tests → Task 1 Step 3 `MiniDbClient(long timeoutSeconds)` package-private ctor. ✓
- Server-side unchanged, protocol unchanged → Global Constraints. ✓
- Existing tests stay green → Task 1 Step 5, Task 2 Step 3. ✓
- `ClientResult` sealed interface unchanged → Task 1 Step 3 preserves it. ✓
- `isValid` tying to live channel, auto-reconnect, multi-channel/pool — explicitly out of scope per spec. ✓ (no task, correct)

**2. Placeholder scan:** Step 3's code block is a complete, end-to-end transcription: `ResponseCollector` declares an `arrowDecoder` field (a custom `@FunctionalInterface ArrowDecoder` whose `decode(byte[]) throws SQLException`, because `readArrow` declares a checked exception and so cannot target `java.util.function.Function`). It is wired to `MiniDbClient.this::readArrow` at the `connect()` call site (the `this::` inside the anonymous `ChannelInitializer` must resolve to the enclosing `MiniDbClient`, hence the qualified reference). The `ArrowBatch` branch calls `arrowDecoder.decode(b.data())` and fails the future on `SQLException`. No stubs, no "implement later", no "handle edge cases", no out-of-band fix instructions.

**3. Type consistency:**
- `pending` is `Map<Long, CompletableFuture<ClientResult>>` throughout (field, router, finally). ✓
- `nextRequestId` is `AtomicLong`, used with `.getAndIncrement()`. ✓ (was plain `long` with `++`; spec said `AtomicLong`.)
- `ResponseCollector` constructor: `(CompletableFuture<Void> handshake, Map<Long, CompletableFuture<ClientResult>> pending, Runnable onDisconnect, ArrowDecoder arrowDecoder)` — matches the `connect()` `new ResponseCollector(handshake, pending, MiniDbClient.this::markDisconnected, MiniDbClient.this::readArrow)` call site. ✓
- `ClientResult.Rows`/`.Update` records unchanged. ✓
- `markDisconnected()` returns `void`, matches `Runnable` target. ✓
- `failAllPending(String)` and the inline fan-out in `channelInactive`/`exceptionCaught` both complete futures with `SQLException`. ✓

No gaps, no placeholders, consistent types.
