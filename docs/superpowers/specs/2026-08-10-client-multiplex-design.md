# MiniDbClient: Multiplexed Single-Channel + requestId Routing

**Date:** 2026-08-10
**Status:** Design — approved (a)+(b)+multiplex, pending spec review

## Problem

`MiniDbClient` has two defects, both rooted in its single shared `BlockingQueue` and its
ignored `requestId`:

1. **Disconnect dead-hang (a):** When the server restarts/closes, Netty fires
   `channelInactive` on the client socket. `ResponseCollector` overrides only
   `channelRead0` and `exceptionCaught` — not `channelInactive`. If the server closes
   cleanly (no exception), `exceptionCaught` may not fire, so the queue stays empty.
   `execute()` then blocks in `poll(30s)` until a misleading `timeout waiting for server
   response` — the real cause (dead connection) is never surfaced and the caller waits 30s.

2. **Result cross-talk (b):** All responses for all statements go into one shared queue.
   `execute()` dispatches by `instanceof`, **ignoring `requestId`**. If two SQL statements run
   concurrently through the one shared connection (permitted by JDBC, though discouraged),
   thread A may consume thread B's `UpdateCount`/`ArrowBatch` and return the wrong result.

## Root cause

- The protocol already carries `requestId` on `ExecuteResponse`, `ArrowBatch`, and
  `UpdateCount` (see `MessageEncoder`). The wire format supports request-scoped dispatch;
  the client just never used it.
- `channelInactive` is unhandled → dead-connection events are dropped on the floor.

## Design: multiplexed single channel + requestId routing

Keep one TCP socket per `MiniDbConnection`. Each `execute()` gets a unique `requestId`;
responses are routed to per-request `CompletableFuture`s via a `requestId → future` map.
Concurrent statements interleave safely on the shared wire.

### Why single-channel multiplex (not per-statement channel)

- Solves cross-talk and disconnect with **no extra TCP connections or handshakes** per
  statement.
- Does not introduce server-side concurrency risk: Netty binds one `SessionHandler` per
  socket and runs that channel's events on a single worker thread, so requests on one
  socket are serialized server-side regardless of client-side concurrency. Multiplexing
  preserves that invariant — no new server-side `replaceBatches` races.
- Matches the streaming-protocol model (Postgres/MySQL).

## Components

### `MiniDbClient` (rewrite of `execute`/`poll`/`ResponseCollector`)

Fields:
- `Channel channel`, `EventLoopGroup group`, `BufferAllocator allocator` (unchanged)
- `AtomicLong nextRequestId` (was a plain `long`)
- `ConcurrentHashMap<Long, CompletableFuture<ClientResult>> pending` (replaces shared `BlockingQueue`)
- `CompletableFuture<Void> handshakeFuture` (one-shot, for `connect()`)
- `volatile boolean connected` (fast-fail on dead connection)

`connect(host, port)`:
- Open channel with `MessageDecoder`, `MessageEncoder`, `ResponseCollector`.
- `writeAndFlush(new Message.Handshake(Protocol.VERSION))`.
- Block on `handshakeFuture.get(TIMEOUT)`. On timeout/exception → close resources, throw.

`execute(sql)`:
- If `!connected` → throw `SQLException("connection is closed")` immediately.
- `long id = nextRequestId.incrementAndGet()`.
- Create `CompletableFuture<ClientResult> fut`, `pending.put(id, fut)`.
- **Race guard:** after `put`, re-check `connected`. If it flipped to false between the
  check and the `put`, the `channelInactive` clear may have already run and missed our
  entry. If `!connected` at this point, `pending.remove(id)`, fail `fut` with
  "connection closed", throw. (This closes the put-after-clear orphan window.)
- `channel.writeAndFlush(new Message.ExecuteRequest(id, sql))` — add a listener; if the
  write fails synchronously, `pending.remove(id)`, fail `fut`, throw.
- Block on `fut.get(TIMEOUT_SECONDS, ...)`; unwrap `ExecutionException` → `SQLException`.
- In a `finally`, `pending.remove(id)` only if it still equals `fut` (defensive — router
  removes on success, `channelInactive` removes on failure; the `finally` guards the
  write-failure and timeout paths). Use `pending.remove(id, fut)` for the atomic
  identity check.
- Returns the `ClientResult`.

**Pending-map ownership rule (resolves the overlap):** the `pending` entry is removed by
exactly one of: the router on a successful response (`pending.remove(requestId)`),
`channelInactive`/`exceptionCaught` on failure (which fail the future), or the `execute`
`finally` via `pending.remove(id, fut)` (identity-protected) as a backstop for the
write-failure/timeout paths. Because `remove(id, fut)` is atomic and identity-checked, a
`finally` running after the router already removed the entry is a harmless no-op, and a
`finally` racing a `channelInactive` clear is also a no-op (the entry is already gone).

`close()`:
- Mark `connected = false`.
- Send `Message.CloseRequest`, close channel, `group.shutdownGracefully()`, `allocator.close()`.
- Fail all `pending` futures with `SQLException("connection is closed")`.

### `ResponseCollector` (the actual fix for both defects)

`channelRead0(ctx, msg)`:
- `HandshakeAck` → `handshakeFuture.complete(null)`.
- `ExecuteResponse r` →
  - `fut = pending.remove(r.requestId())`
  - if `fut == null` → log and drop (late/orphan response).
  - if `r.ok()` → `fut.complete(...)` — but server never sends `ok`; on `!ok()` →
    `fut.completeExceptionally(new SQLException(r.error()))`.
- `UpdateCount u` → `pending.remove(u.requestId()).complete(new Update(u.count()))`.
- `ArrowBatch b` → minidb sends one batch with `lastBatch=true`; complete the future with
  the decoded root:
  `pending.remove(b.requestId()).complete(new Rows(readArrow(b.data())))`.
  (If multi-batch streaming is added later, accumulate until `lastBatch`.)

`channelInactive(ctx)` (NEW — fixes dead-hang):
- Mark `connected = false`.
- Snapshot `pending.values()`, clear `pending` (so any `put` that races in afterward
  sees `connected == false` and fast-fails via the execute race-guard rather than
  registering a live entry).
- For each future in the snapshot: `completeExceptionally(new SQLException("connection closed"))`.
- Fail `handshakeFuture` if still pending.

`exceptionCaught(ctx, cause)`:
- Same fan-out as `channelInactive`: mark `connected = false`, fail all pending futures
  with a `SQLException` wrapping `cause`, then `ctx.close()`.

### Server side — unchanged

- `SessionHandler`, `QueryExecutor`, `StorageManager`, `Message`/`MessageEncoder`/
  `MessageDecoder`: no changes. `requestId` is already threaded through.

### JDBC layer — unchanged signatures

- `MiniDbStatement`/`MiniDbPreparedStatement` already call `client.execute(sql)`. No
  signature changes. `MiniDbConnection`/`MiniDbDriver` unchanged.

## Semantics & edge cases

- **Server restart mid-statement:** `channelInactive` fires → the statement's future
  completes exceptionally with "connection closed" immediately. No 30s wait.
- **Server restart between statements:** next `execute()` sees `connected == false` →
  throws immediately. (Reconnect-on-failure is explicitly out of scope — caller must open
  a new `Connection`, matching JDBC norms.)
- **Concurrent statements on one connection:** each gets its own `requestId` and its own
  future. Responses routed by `requestId`. No cross-talk even if responses arrive
  interleaved.
- **Late/orphan response (unknown requestId):** logged and dropped; does not poison another
  request's future.
- **Timeout:** `execute()` still honors `TIMEOUT_SECONDS` (30s default). Add an
  injectable timeout (constructor arg) so tests can use a small value without global
  mutation. Disconnect fast-fail happens **before** the timeout.
- **Connection validity:** `MiniDbConnection.isValid(timeout)` currently returns `!closed`.
  This is unchanged in scope here — `closed` is only set on `close()`. A follow-up could
  tie `isValid` to `connected`, but that is out of scope for this fix.

## Testing (TDD)

1. **`restartFailsOpenExecuteFast`** (JDBC test): start server, connect, `close()` server,
   call `execute` on the now-dead connection, assert `SQLException` thrown in < 5s (well
   under the 30s timeout). This is the core dead-hang regression test.
2. **`concurrentStatementsDoNotCrossTalk`** (JDBC test): two threads, one running
   `SELECT ... ORDER BY` against table A, one running `INSERT`/`SELECT` against table B,
   concurrently on the same `Connection`. Assert each thread receives its own correct
   result (wrong result would be the cross-talk symptom). Use a `CountDownLatch` to start
   both threads together and a `CyclicBarrier`/latch to maximize overlap.
3. **Existing tests stay green:** `PersistenceTest` (server restart with reconnect — the
   *normal* restart path), `ArrowResultDecoderTest` (all e2e queries).

## Out of scope

- Per-statement or pooled channels (chose multiplexed single channel).
- Automatic reconnect on connection failure.
- Server-side per-query concurrency (not needed; one socket = one server thread).
- Tying `isValid` to the live channel state.
