# Server Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Log4j2 as the SLF4J 2.x provider for the server so the NOP warning disappears, and instrument key paths (lifecycle, SQL execution + timing, errors, storage flush).

**Architecture:** Add `log4j-slf4j2-impl` (runtime) as the SLF4J provider on the server module; ship a `log4j2.properties` with console + rolling-file appenders; replace the lone `System.out.println` and the swallowed `e.getMessage()` fallbacks with SLF4J loggers. Netty/Arrow internal logging is routed through Log4j2 automatically once the provider is on the runtime classpath.

**Tech Stack:** SLF4J 2.0.17 (already present), Log4j2 2.24.3 (new), `log4j-slf4j2-impl` (the SLF4J 2.x provider shipped by Log4j2), Maven, JUnit 5.

## Global Constraints

- Log4j2 version pinned to `2.24.3` via a new `${log4j.version}` property in the parent `pom.xml`.
- `log4j-slf4j2-impl` is `runtime` scope and only declared on the `minidb-server` module (it is the only module that runs as a process).
- All new loggers use `org.slf4j.Logger` / `org.slf4j.LoggerFactory` (never Log4j2's own API) — so app code stays portable across SLF4J providers.
- Default levels: `com.minidb` = INFO; `io.netty`, `org.apache.arrow`, `org.apache.calcite` = WARN; root = INFO.
- No behavior change to client error responses — on the server side, exceptions are now *logged* (WARN + stack trace) before the existing error response is sent.
- Build/test command for this module: `"E:\jdbc server\mvnw.cmd" test -pl minidb-server` (run from `E:\jdbc server`). Use `-pl minidb-server` WITHOUT `-am`; the `minidb-protocol` jar is already built/installed in the local repo, and `-am` triggers classloader issues in this reactor. NOTE: the repo path has a space — always quote it.
- Branch: work on a new branch `feat/server-logging` off the current branch.

---

## File Structure

| File | Responsibility |
|---|---|
| `pom.xml` (parent) | Pin `log4j.version`; declare `log4j-slf4j2-impl` in `dependencyManagement` (runtime) |
| `minidb-server/pom.xml` | Pull in `log4j-slf4j2-impl` (inherits version + scope from parent) |
| `minidb-server/src/main/resources/log4j2.properties` (new) | Console + rolling-file appenders, per-package levels |
| `minidb-server/src/main/java/com/minidb/server/Main.java` | Startup + shutdown logging; replaces `System.out.println` |
| `minidb-server/src/main/java/com/minidb/server/MiniDbServer.java` | Bind + close lifecycle logging |
| `minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java` | Per-query DEBUG (SQL) + INFO (rows/affected + timing) + WARN (errors with stack trace) |
| `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java` | `loadAll` INFO (count) + `flushTable` INFO |

No new production classes are created — only dependencies, a config resource, and logging calls inside existing classes.

---

## Task 1: Add the Log4j2 dependency + provider config

This task makes the NOP warning disappear and gives the server a logging backend. It is independently testable: after it, running the server should no longer print the SLF4J NOP warning.

**Files:**
- Modify: `E:\jdbc server\pom.xml` (parent — `<properties>` block around line 17-25, and `<dependencyManagement>` block around line 27-83)
- Modify: `E:\jdbc server\minidb-server\pom.xml` (the `<dependencies>` block, line 12-45)
- Create: `E:\jdbc server\minidb-server\src\main\resources\log4j2.properties`

**Interfaces:**
- Consumes: nothing.
- Produces: a runtime classpath on which `org.slf4j.LoggerFactory.getLogger(...)` resolves to a Log4j2-backed logger (visible to all later tasks). The config file `log4j2.properties` is auto-discovered by Log4j2 on the classpath.

- [ ] **Step 1: Pin the Log4j2 version and add dependency management to the parent pom**

In `E:\jdbc server\pom.xml`, add the version property inside the existing `<properties>` block (after the `junit.version` line, line 24):

```xml
    <log4j.version>2.24.3</log4j.version>
```

Then inside the parent's `<dependencyManagement><dependencies>` block, add this entry (place it right after the existing `junit-jupiter` managed dependency, before the `com.minidb:minidb-protocol` entry — around line 71):

```xml
      <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-slf4j2-impl</artifactId>
        <version>${log4j.version}</version>
        <scope>runtime</scope>
      </dependency>
```

- [ ] **Step 2: Declare the dependency on the server module**

In `E:\jdbc server\minidb-server\pom.xml`, inside the `<dependencies>` block, add (after the `calcite-server` dependency, before the `junit-jupiter` test dependency — around line 40):

```xml
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-slf4j2-impl</artifactId>
    </dependency>
```

No `<version>` or `<scope>` here — both are inherited from the parent's `dependencyManagement`.

- [ ] **Step 3: Create the log4j2.properties config file**

Create `E:\jdbc server\minidb-server\src\main\resources\log4j2.properties` with exactly:

```properties
# Log4j2 configuration for the MiniDB server.
# Picked up automatically from the classpath at startup.

# --- Appenders ---
appender.console.type = Console
appender.console.name = STDOUT
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss.SSS} %-5level [%t] %logger{1} - %msg%n

appender.file.type = RollingFile
appender.file.name = FILE
appender.file.fileName = ${sys:logFile:-logs/minidb.log}
appender.file.filePattern = ${sys:logFile:-logs/minidb.log}.%i
appender.file.layout.type = PatternLayout
appender.file.layout.pattern = %d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%t] %logger{1} - %msg%n
appender.file.policies.type = Policies
appender.file.policies.size.type = SizeBasedTriggeringPolicy
appender.file.policies.size.size = 10MB
appender.file.strategy.type = DefaultRolloverStrategy
appender.file.strategy.max = 7

# --- Loggers ---
logger.netty.name = io.netty
logger.netty.level = WARN
logger.arrow.name = org.apache.arrow
logger.arrow.level = WARN
logger.calcite.name = org.apache.calcite
logger.calcite.level = WARN
logger.minidb.name = com.minidb
logger.minidb.level = INFO

rootLogger.level = INFO
rootLogger.appenderRef.console.ref = STDOUT
rootLogger.appenderRef.file.ref = FILE
```

- [ ] **Step 4: Verify the build still compiles and tests pass**

Run from `E:\jdbc server`:
```bash
"E:\jdbc server\mvnw.cmd" test -pl minidb-server
```
Expected: BUILD SUCCESS, all 42 server tests pass (the dependency is additive; no code changes yet). The SLF4J NOP warning may still appear during the *test* run only if the provider isn't on the test classpath — that is acceptable here and confirmed by the build succeeding.

- [ ] **Step 5: Commit**

```bash
cd "E:\jdbc server"
git add pom.xml minidb-server/pom.xml minidb-server/src/main/resources/log4j2.properties
git commit -m "feat: add log4j2-slf4j2-impl provider and log4j2.properties config

Console + rolling-file (logs/minidb.log, 10MB, 7 files) appenders.
com.minidb at INFO; netty/arrow/calcite at WARN; root INFO."
```

---

## Task 2: Log server lifecycle (Main + MiniDbServer)

This task replaces the only `System.out.println` and adds startup/shutdown INFO logs. Independently testable: a server start emits the startup line; the lone `System.out.println` is gone.

**Files:**
- Modify: `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\Main.java` (full file is ~22 lines)
- Modify: `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\MiniDbServer.java` (full file is ~74 lines)

**Interfaces:**
- Consumes: the Log4j2 provider from Task 1 (so `LoggerFactory.getLogger` returns a real logger).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add a logger and lifecycle logs to Main.java**

Edit `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\Main.java`.

Add the SLF4J import after the existing `java.nio.file.Path` import (line 3):
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
```

Add a static logger field to the class (after `public final class Main {`):
```java
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
```

Replace the body of `main` so that the existing `System.out.println("MiniDB listening on port " + server.port());` becomes an INFO log, and a shutdown-hook INFO log is added. The final method should read:

```java
    public static void main(String[] args) throws Exception {
        int port = 8899;
        Path dataDir = Path.of("data");
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i])) {
                port = Integer.parseInt(args[++i]);
            } else if ("--data".equals(args[i])) {
                dataDir = Path.of(args[++i]);
            }
        }
        LOG.info("MiniDB starting on port {} with data dir {}", port, dataDir);
        MiniDbServer server = new MiniDbServer();
        server.start(port, dataDir);
        LOG.info("MiniDB listening on port {}", server.port());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("MiniDB shutting down");
            server.close();
        }));
        Thread.currentThread().join();
    }
```

(Note: the shutdown-hook lambda calls `server.close()` directly — the existing hook did `server::close`; moving it into a lambda lets the log precede the close. `server.close()` is fine to call from a hook thread.)

- [ ] **Step 2: Add a logger and lifecycle logs to MiniDbServer.java**

Edit `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\MiniDbServer.java`.

Add the SLF4J imports after the existing arrow/arrow-memory imports (after line 19, `import org.apache.arrow.memory.RootAllocator;`):
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
```

Add a static logger field to the class (after `public class MiniDbServer implements AutoCloseable {`):
```java
    private static final Logger LOG = LoggerFactory.getLogger(MiniDbServer.class);
```

In `start(int port, Path dataDir)`, after the line `channel = bootstrap.bind(port).sync().channel();`, add:
```java
        LOG.info("MiniDB server bound to port {}", port);
```

In `close()`, at the very start of the method (before `if (channel != null) {`), add:
```java
        LOG.info("MiniDB server closed");
```

- [ ] **Step 3: Verify the build compiles and tests pass**

Run from `E:\jdbc server`:
```bash
"E:\jdbc server\mvnw.cmd" test -pl minidb-server
```
Expected: BUILD SUCCESS, all 42 tests pass.

- [ ] **Step 4: Commit**

```bash
cd "E:\jdbc server"
git add minidb-server/src/main/java/com/minidb/server/Main.java minidb-server/src/main/java/com/minidb/server/MiniDbServer.java
git commit -m "feat: log server lifecycle (start, bound, close, shutdown)"
```

---

## Task 3: Log SQL execution + errors in SessionHandler

This is the highest-value task: every query gets a DEBUG (SQL) + INFO (rows/affected + timing) line, and errors get a WARN with stack trace instead of being swallowed.

**Files:**
- Modify: `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\netty\SessionHandler.java` (full file is ~67 lines)

**Interfaces:**
- Consumes: the Log4j2 provider from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the logger field and imports to SessionHandler**

Edit `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\netty\SessionHandler.java`.

Add the SLF4J imports after the existing imports (after line 11, the `ArrowStreamWriter` import):
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
```

Add a static logger field to the class (after `public class SessionHandler extends SimpleChannelInboundHandler<Message> {`):
```java
    private static final Logger LOG = LoggerFactory.getLogger(SessionHandler.class);
```

- [ ] **Step 2: Add timing + DEBUG/INFO/WARN to handleExecute**

In `handleExecute(ChannelHandlerContext ctx, Message.ExecuteRequest req)`, wrap the existing try body with timing and logging. Replace the existing method body (lines ~32-45) with:

```java
    private void handleExecute(ChannelHandlerContext ctx, Message.ExecuteRequest req) {
        LOG.debug("executing: {}", req.sql());
        long start = System.nanoTime();
        try {
            QueryResult result = executor.execute(req.sql());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            if (result instanceof QueryResult.Update update) {
                LOG.info("query ok: {} rows affected in {} ms", update.count(), elapsedMs);
                ctx.writeAndFlush(new Message.UpdateCount(req.requestId(), update.count()));
            } else if (result instanceof QueryResult.Rows rows) {
                LOG.info("query ok: {} rows returned in {} ms", rows.data().getRowCount(), elapsedMs);
                sendRows(ctx, req.requestId(), rows.data());
                rows.data().close();
            }
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            LOG.warn("query failed in {} ms: {}", elapsedMs, req.sql(), e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(req.requestId(), message));
        }
    }
```

Note: the `LOG.warn(...)` call passes `e` as the last (throwable) argument — SLF4J logs the full stack trace. The client-facing error response is unchanged.

- [ ] **Step 3: Add WARN to sendRows + exceptionCaught**

In `sendRows(ChannelHandlerContext ctx, long requestId, VectorSchemaRoot root)`, replace the `catch (Exception e)` block so it logs before responding:

```java
        } catch (Exception e) {
            LOG.warn("failed to send rows for request {}", requestId, e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
        }
```

In `exceptionCaught(ChannelHandlerContext ctx, Throwable cause)`, replace the silent close with a logged close:

```java
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("channel exception, closing", cause);
        ctx.close();
    }
```

- [ ] **Step 4: Verify the build compiles and tests pass**

Run from `E:\jdbc server`:
```bash
"E:\jdbc server\mvnw.cmd" test -pl minidb-server
```
Expected: BUILD SUCCESS, all 42 tests pass. (Note: `QueryExecutorTest` exercises the executor directly, not the Netty handler, so these handler changes don't affect test outcomes — but the module must still compile cleanly.)

- [ ] **Step 5: Commit**

```bash
cd "E:\jdbc server"
git add minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java
git commit -m "feat: log SQL execution (debug sql, info rows+timing, warn errors)"
```

---

## Task 4: Log storage lifecycle (loadAll + flushTable)

This task gives visibility into table load at startup and per-table flush at close.

**Files:**
- Modify: `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\storage\StorageManager.java` (full file is ~205 lines; `loadAll` is lines 44-56, `flushTable` is lines 127-156)

**Interfaces:**
- Consumes: the Log4j2 provider from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the logger field and imports to StorageManager**

Edit `E:\jdbc server\minidb-server\src\main\java\com\minidb\server\storage\StorageManager.java`.

Add the SLF4J import after the existing imports (after line 28, the `Field` import):
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
```

Add a static logger field to the class (after `public class StorageManager implements AutoCloseable {`):
```java
    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);
```

- [ ] **Step 2: Log table count in loadAll**

In `loadAll()`, add an INFO log of the loaded table count. The early-return when `dataDir` doesn't exist should log zero. Replace the method (lines 44-56) with:

```java
    public void loadAll() {
        if (!Files.exists(dataDir)) {
            LOG.info("loaded 0 table(s) (data dir absent)");
            return;
        }
        int count = 0;
        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(dataDir, "*.arrow")) {
            for (Path file : stream) {
                loadFile(file);
                count++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.info("loaded {} table(s)", count);
    }
```

- [ ] **Step 3: Log each table flush in flushTable**

In `flushTable(String tableName)`, add an INFO log after a successful flush. Place it immediately before the method's closing `}` of the outer `try` block (just after the inner `try-with-resources` for the writer closes — i.e., after the `}` that closes the `try (SeekableByteChannel channel = ...)` block, before the `catch (IOException e)`):

```java
            LOG.info("flushed table {}", tableName);
```

Concretely, the tail of `flushTable` should read:

```java
            try (SeekableByteChannel channel = Files.newByteChannel(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                VectorSchemaRoot sink = table.newBatchRoot();
                try (ArrowFileWriter writer = new ArrowFileWriter(sink, null, channel)) {
                    writer.start();
                    for (VectorSchemaRoot batch : table.batches()) {
                        ArrowRecordBatch recordBatch =
                                new VectorUnloader(batch).getRecordBatch();
                        new VectorLoader(sink).load(recordBatch);
                        recordBatch.close();
                        writer.writeBatch();
                    }
                    writer.end();
                } finally {
                    sink.close();
                }
            }
            LOG.info("flushed table {}", tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
```

- [ ] **Step 4: Verify the build compiles and tests pass**

Run from `E:\jdbc server`:
```bash
"E:\jdbc server\mvnw.cmd" test -pl minidb-server
```
Expected: BUILD SUCCESS, all 42 tests pass. (`StorageManagerTest` exercises `loadAll` and flush paths, confirming the new logs don't break behavior.)

- [ ] **Step 5: Commit**

```bash
cd "E:\jdbc server"
git add minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java
git commit -m "feat: log storage lifecycle (loaded table count, flushed table)"
```

---

## Task 5: Smoke-verify the startup warning is gone + finalize

This task confirms the end-to-end goal: the NOP warning no longer appears, and a real startup log line does.

**Files:**
- None modified (verification only).

**Interfaces:**
- Consumes: all of Tasks 1-4.

- [ ] **Step 1: Start the server and capture early output**

From `E:\jdbc server`, start the server (background, capture stdout+stderr to a file). The exec-maven-plugin config in `minidb-server/pom.xml` already wires `com.minidb.server.Main` with the `--add-opens` flags and `--port 8899`:

```bash
cd "E:\jdbc server"
"E:\jdbc server\mvnw.cmd" -pl minidb-server exec:exec > /tmp/minidb-startup.log 2>&1 &
SERVER_PID=$!
sleep 25
kill $SERVER_PID 2>/dev/null
cat /tmp/minidb-startup.log
```

(If `exec:exec` is unavailable or behaves oddly in this shell, fall back to running the built classes directly with java and the same `--add-opens` flags used in the surefire `argLine` in the parent pom. The goal is simply to observe startup output.)

- [ ] **Step 2: Confirm the SLF4J NOP warning is absent and a startup line is present**

Check the captured output. Expected:
- **No** `SLF4J(W): No SLF4J providers were found.` line.
- **No** `SLF4J(W): Defaulting to no-operation (NOP) logger implementation.` line.
- **A** line like `... INFO [main] Main - MiniDB starting on port 8899 with data dir data`.
- **A** line like `... INFO [main] MiniDbServer - MiniDB server bound to port 8899`.
- **A** line like `... INFO [main] Main - MiniDB listening on port 8899`.

If the NOP warning persists, the provider is not on the runtime classpath — re-check Task 1 steps 1-2 (the dependency must be `runtime` scope and declared on `minidb-server`, with version+scope inherited from the parent `dependencyManagement`).

- [ ] **Step 3: Confirm a rolling file was created**

Check that `logs/minidb.log` was created under the server working directory (`E:\jdbc server\logs\minidb.log`) and contains the same startup lines. Clean up:

```bash
cd "E:\jdbc server"
rm -rf logs
```

- [ ] **Step 4: Final full test run**

Run from `E:\jdbc server`:
```bash
"E:\jdbc server\mvnw.cmd" test -pl minidb-server
```
Expected: BUILD SUCCESS, all 42 tests pass.

- [ ] **Step 5: Commit any cleanup (if logs dir was accidentally tracked)**

```bash
cd "E:\jdbc server"
git status
# If logs/ or /tmp artifacts are tracked, add them to .gitignore:
# printf 'logs/\n' >> .gitignore
# git add .gitignore && git commit -m "chore: ignore logs/ dir"
```

(Only commit if there is something to commit. The `logs/` dir should not be committed — it is a runtime artifact.)

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Provider = Log4j2 via `log4j-slf4j2-impl` → Task 1. ✓
- Config format = `log4j2.properties` → Task 1. ✓
- Output = console + rolling file `logs/minidb.log` (10MB, 7 files) → Task 1. ✓
- Instrumentation: Main/MiniDbServer lifecycle → Task 2. ✓; SessionHandler SQL + timing + errors → Task 3. ✓; StorageManager load/flush → Task 4. ✓
- Levels: com.minidb INFO, netty/arrow/calcite WARN, root INFO → Task 1 config file. ✓
- Replace the `System.out.println` → Task 2 step 1. ✓
- Replace the swallowed `e.getMessage()` fallbacks with WARN + stack trace → Task 3 step 2 (handleExecute) + step 3 (sendRows). ✓
- `exceptionCaught` no longer silent → Task 3 step 3. ✓
- Testing: existing 42-test suite passes as the regression gate → each task's "verify" step; smoke check for the warning → Task 5. ✓

**Placeholder scan:** No TBD/TODO/"add error handling"/vague steps. Every code step contains the actual code or the exact replacement text.

**Type consistency:**
- Logger field name is consistently `LOG` across Main, MiniDbServer, SessionHandler, StorageManager. ✓
- Timing variable `elapsedMs` and `start` (nanoTime) used consistently in SessionHandler. ✓
- `LOG.warn(..., e)` passes the throwable as the last arg consistently. ✓
- The `update.count()` / `rows.data().getRowCount()` accessors match `QueryResult.Update` / `QueryResult.Rows` (seen earlier in SessionHandler's existing code). ✓
