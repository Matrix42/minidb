# Server Logging — Design

**Date:** 2026-08-10
**Status:** Approved (pending spec review)
**Branch:** `feat/server-logging`

## Problem

The server prints `SLF4J(W): No SLF4J providers were found. Defaulting to no-operation (NOP) logger implementation` on startup. `slf4j-api:2.0.17` is already on the classpath (transitive via `arrow-vector`), but there is no provider implementation, so all SLF4J calls — including Netty's internal logging — are silently dropped. The server itself has no logging at all: the only output is a single `System.out.println` in `Main`.

## Goal

Give the server a real logging backend so the NOP warning disappears, Netty logs properly, and key application paths emit structured, level-filterable log lines. Log4j2 as the SLF4J 2.x provider, configured via a `.properties` file.

## Decisions (confirmed with user)

| Decision | Choice |
|---|---|
| Provider | Log4j2, via `log4j-slf4j2-impl` (the SLF4J 2.x provider shipped by Log4j2) |
| Config format | `log4j2.properties` |
| Output | Console (stdout) + rolling file `logs/minidb.log` |
| Instrumentation scope | Key paths: server lifecycle, SQL execution, query errors, storage flush |
| Default levels | `com.minidb` = INFO; `io.netty` / `org.apache.arrow` / `org.apache.calcite` = WARN; root = INFO |

### Why `log4j-slf4j2-impl` (not a bridge)

SLF4J 2.0+ uses the `ServiceLoader` mechanism to find a provider. Log4j2 ships `log4j-slf4j2-impl`, which registers itself as that provider: every `LoggerFactory.getLogger(...)` call (including inside Netty and Arrow) is routed through the Log4j2 core. `log4j-to-slf4j` is the reverse direction (routes Log4j2 API calls to SLF4J) and is not what we want here. No `jul-to-slf4j` / `jcl-over-slf4j` needed — Netty already speaks SLF4J directly.

## Design

### 1. Dependencies

**Parent `pom.xml`** — add version property and `dependencyManagement` entry:

```xml
<properties>
  ...
  <log4j.version>2.24.3</log4j.version>
</properties>

<dependencyManagement>
  <dependencies>
    ...
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-slf4j2-impl</artifactId>
      <version>${log4j.version}</version>
      <scope>runtime</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

**`minidb-server/pom.xml`** — declare the dependency (inherits version + scope from parent):

```xml
<dependency>
  <groupId>org.apache.logging.log4j</groupId>
  <artifactId>log4j-slf4j2-impl</artifactId>
</dependency>
```

`runtime` scope: the server is the only module that runs as a process, so only it needs the provider on its runtime classpath. `log4j-slf4j2-impl` transitively brings in `log4j-core` and `log4j-api`.

### 2. Configuration — `minidb-server/src/main/resources/log4j2.properties`

```properties
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

Notes:
- `appender.file.fileName` uses `${sys:logFile:-logs/minidb.log}` so the path can be overridden via `-DlogFile=...` without editing the file; defaults to `logs/minidb.log` (relative to the server's working directory, created on demand by Log4j2).
- Rolling: 10MB per file, up to 7 archived files.
- `logger.minidb` at INFO means all `com.minidb.*` classes log at INFO unless they ask for DEBUG explicitly; the library loggers are capped at WARN so Netty/Arrow/Calcite noise doesn't drown app output.

### 3. Instrumentation

All new loggers use `org.slf4j.LoggerFactory.getLogger(...)`. Pattern is a `private static final Logger LOG = LoggerFactory.getLogger(<Class>.class);` field.

| File | What to log | Level |
|---|---|---|
| `Main.java` | Startup: "MiniDB starting on port {} with data dir {}", port, dataDir. Replace the existing `System.out.println("MiniDB listening on port " + server.port())` with an INFO log. Shutdown hook: INFO "MiniDB shutting down". | INFO |
| `MiniDbServer.java` | `start()`: INFO "MiniDB server bound to port {}" after `bind().sync()`. `close()`: INFO "MiniDB server closed". | INFO |
| `SessionHandler.channelRead0` | DEBUG "received {}" (the message kind, not full SQL). | DEBUG |
| `SessionHandler.handleExecute` | Before execute: DEBUG "executing: {}" (the SQL). After: INFO "query ok: {} rows affected/returned in {} ms" (affected count or row count + duration). On exception: WARN with the exception (full stack trace), in addition to sending the error response to the client. Replaces the current `e.getMessage()` fallback that hides the cause. | DEBUG/INFO/WARN |
| `SessionHandler.sendRows` | WARN on failure (with exception). | WARN |
| `SessionHandler.exceptionCaught` | WARN "channel exception, closing" with the cause (currently silently closes the channel). | WARN |
| `StorageManager.loadAll` | INFO "loaded {} table(s)" (count). | INFO |
| `StorageManager.flushTable` | INFO "flushed table {}" (name). | INFO |

Timing in `handleExecute`: capture `System.nanoTime()` before `executor.execute(...)`, compute elapsed ms after, format into the INFO "query ok" line. `nanoTime` is used (not `Date`/`System.currentTimeMillis` as a duration source) for monotonic correctness; the wall-clock timestamp still comes from the layout's `%d`.

### 4. Error handling

No behavior change to error *handling* — the client still receives `ExecuteResponse.error(requestId, message)` exactly as today. The only change is that on the server side the exception is now *logged at WARN with its stack trace* before the response is sent, instead of being swallowed. This makes query bugs (e.g. the earlier `IndexOutOfBounds` UPDATE bug, or the `RexInterpreter` VARCHAR comparison gap) visible in the server log.

### 5. Testing

No new unit test for log *content* (asserting on captured log lines is brittle). Instead:

- The existing `QueryExecutorTest` (9 tests, run under the surefire config) already exercises both the success path and the error path of `QueryExecutor`/`SessionHandler` logic. With the Log4j2 provider now on the test runtime classpath, these confirm logging through SLF4J does not throw and does not regress.
- A manual smoke check: run the server (`mvnw -pl minidb-server exec:exec` or the existing exec config) and confirm (a) the NOP warning is gone, (b) a startup INFO line appears, (c) connecting via the JDBC driver and running a query emits a "query ok" line. This is documented as a verification step, not an automated test.

## Files touched

- `pom.xml` — version property + `dependencyManagement` entry
- `minidb-server/pom.xml` — declare `log4j-slf4j2-impl`
- `minidb-server/src/main/resources/log4j2.properties` — new
- `minidb-server/src/main/java/com/minidb/server/Main.java` — logger + replace println
- `minidb-server/src/main/java/com/minidb/server/MiniDbServer.java` — logger
- `minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java` — logger + timing + WARN on errors
- `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java` — logger

## Out of scope

- Structured/JSON logging, MDC, tracing.
- Log shipping / remote appenders.
- Per-query log level control or a SQL `SET log_level` statement.
- Logging in `minidb-jdbc` (client) or `minidb-protocol` — only the server process is instrumented.
