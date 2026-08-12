# JDBC DatabaseMetaData 元数据查询 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `MiniDbDatabaseMetaData` 的 `getSchemas` / `getTables` / `getColumns` 返回符合 JDBC 规范、反映服务端 `MiniDbCatalog` 真实状态的 `ResultSet`,供 DBeaver/Squirrel 等 JDBC 工具列出 schema/表/列。

**Architecture:** 自底向上三层:(1) `minidb-protocol` 新增 3 条元数据请求消息(响应复用现有 `ArrowBatch`);(2) `minidb-server` 新增外挂式 `MetadataExecutor`,从 `MiniDbCatalog` 物化 Arrow 行,`SessionHandler` 加 3 个分支走现有 `sendRows`;(3) `minidb-jdbc` 在 `MiniDbClient` 加 3 个方法翻译 JDBC 调用为协议请求,`MiniDbDatabaseMetaData` 三个方法各建 Statement 包装结果。

**Tech Stack:** Java 17, Apache Arrow (VectorSchemaRoot/IPC), Netty, 自研 `minidb-protocol` wire 协议, JUnit 5 + `@TempDir` 集成测试。

## Global Constraints

- **JDK 17 必须**。构建用 `./mvnw.cmd`(bash 下直接跑,不要用 `mvnw.cmd`/`cmd //c`/`mvn`)。
- **改完代码就提交**,conventional commit 风格(`feat:`/`fix:`/`test:`/`refactor:`/`docs:`),不 amend,不 `--no-verify`。在 `master` 分支工作,小步提交。
- **`minidb-protocol` 是稳定核心模块,改动需极谨慎**——本计划只做纯增量(加 3 个 record + 3 个 type 常量 + 编解码各 3 分支),不改任何现有消息/分支。
- **现有 6 个物理算子和 `minidb-protocol` 尽量不改**——元数据走 `MetadataExecutor` 外挂,零侵入算子/规划器。
- **测试用 JUnit 5 + `@TempDir`**。断言关系/包含而非精确行集顺序(除非规范要求排序)。jdbc 模块 test-scope 依赖 `minidb-server`,集成测试照 `PersistenceTest` 模式(`MiniDbServer.start(0, tempDir)` + 真网络)。
- **Arrow `FieldVector` 只有 no-arg `allocateNew()`**;预设容量用 `setInitialCapacity(n); allocateNew();`,不是 `allocateNew(n)`。
- **所有元数据列用 `FieldType.nullable(...)`**(与现有 `ArrowTypes.field` 一致)。
- **类型映射对齐 `MiniDbResultSetMetaData`**:`ColumnType`→`java.sql.Types.*` 与 `MiniDbResultSetMetaData.getColumnType` 一致,`ColumnType`→TYPE_NAME 与 `ArrowTypes.toSqlTypeName` 一致。
- **`MetadataExecutor` 仅依赖 catalog(元数据全在 catalog)**,不依赖 storage/stats。

**设计 spec:** `docs/superpowers/specs/2026-08-12-jdbc-metadata-design.md`(本计划的所有决策出处,冲突时以 spec 为准)。

## File Structure

| 文件 | 责任 | 动作 |
|---|---|---|
| `minidb-protocol/src/main/java/com/minidb/protocol/MessageType.java` | 消息类型 byte 常量 | 加 3 常量 |
| `minidb-protocol/src/main/java/com/minidb/protocol/Message.java` | 消息 record 定义 | 加 3 record |
| `minidb-protocol/src/main/java/com/minidb/protocol/MessageEncoder.java` | 出站编码 | 加 3 分支 |
| `minidb-protocol/src/main/java/com/minidb/protocol/MessageDecoder.java` | 入站解码 | 加 3 分支 |
| `minidb-protocol/src/test/java/com/minidb/protocol/MetadataMessageCodecTest.java` | 编解码往返测试 | 新建 |
| `minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java` | 从 catalog 物化元数据 Arrow 行 | 新建 |
| `minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java` | wire 分发 | 加 3 分支 + 注入 `MetadataExecutor` |
| `minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java` | 物化逻辑单测 | 新建 |
| `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java` | 客户端协议调用 | 加 3 方法 + 公共 send/await |
| `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbDatabaseMetaData.java` | JDBC 元数据 API | 实现 3 方法 |
| `minidb-jdbc/src/test/java/com/minidb/jdbc/DatabaseMetaDataTest.java` | 端到端集成测试 | 新建 |

## 通用:LIKE→正则与 pattern 编解码

这些是多个任务共用的逻辑,先在这里固化,各任务按此实现。

### LIKE pattern→正则(spec 过滤语义节)

```java
private static java.util.regex.Pattern compileLike(String pattern) {
    if (pattern == null) return null;
    StringBuilder sb = new StringBuilder();
    for (char c : pattern.toCharArray()) {
        switch (c) {
            case '%' -> sb.append(".*");
            case '_' -> sb.append('.');
            default -> java.util.regex.Pattern.quote(String.valueOf(c)); // 不对,see below
        }
    }
    return java.util.regex.Pattern.compile(sb.toString(), java.util.regex.Pattern.DOTALL);
}
```

注意:`Pattern.quote` 返回字符串不能用于逐字符拼。正确实现:对每个非 `%`/`_` 字符,若它是正则元字符则 `\` 转义,否则原样追加。完整正确实现见 Task 5 Step 3 的代码块(那是权威实现)。`null`→`null`(调用方据此跳过过滤)。

### 协议 String pattern 的 null 编码(spec 协议节)

`MessageEncoder` 写 String 字段:`null`→`writeInt(-1)`(无字节);`""`→`writeInt(0)`(无字节);非空→`writeInt(len)`+字节。`MessageDecoder` 读:`int len = readInt(); if (len == -1) return null; byte[] b = new byte[len]; readBytes(b); return new String(b, UTF_8)`。types 数组:`null`→`writeInt(-1)`;空→`writeInt(0)`;非空→`writeInt(n)`+逐个字符串(每串用上面的 null 规则,但 types 元素一般非 null)。

---

### Task 1: 协议 — MessageType + Message record

**Files:**
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/MessageType.java`
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/Message.java`

**Interfaces:**
- Produces: `MessageType.SCHEMAS_REQUEST`/`TABLES_REQUEST`/`COLUMNS_REQUEST`(byte 常量 `0x12`/`0x13`/`0x14`);`Message.SchemasRequest(long, String)`、`Message.TablesRequest(long, String, String, String[])`、`Message.ColumnsRequest(long, String, String, String)`。

- [ ] **Step 1: 加 MessageType 常量**

在 `MessageType.java` 的 `UPDATE_COUNT` 之后加:

```java
    public static final byte SCHEMAS_REQUEST = 0x12;
    public static final byte TABLES_REQUEST = 0x13;
    public static final byte COLUMNS_REQUEST = 0x14;
```

- [ ] **Step 2: 加 Message record**

在 `Message.java` 的 `UpdateCount` record 之后(接口闭合 `}` 之前)加:

```java
    record SchemasRequest(long requestId, String schemaPattern) implements Message {
    }

    record TablesRequest(long requestId, String schemaPattern,
                         String tableNamePattern, String[] types) implements Message {
    }

    record ColumnsRequest(long requestId, String schemaPattern,
                          String tableNamePattern, String columnNamePattern) implements Message {
    }
```

- [ ] **Step 3: 编译验证**

Run: `./mvnw.cmd -pl minidb-protocol -am compile -q`
Expected: 无输出,退出码 0。

- [ ] **Step 4: 提交**

```bash
git add minidb-protocol/src/main/java/com/minidb/protocol/MessageType.java minidb-protocol/src/main/java/com/minidb/protocol/Message.java
git commit -m "feat: add metadata request message types and records to protocol"
```

---

### Task 2: 协议 — 编解码器(含往返测试)

**Files:**
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/MessageEncoder.java`
- Modify: `minidb-protocol/src/main/java/com/minidb/protocol/MessageDecoder.java`
- Test: `minidb-protocol/src/test/java/com/minidb/protocol/MetadataMessageCodecTest.java`

**Interfaces:**
- Consumes: Task 1 的 record 与 `MessageType` 常量。
- Produces: `MessageEncoder`/`MessageDecoder` 对 3 个新 record 的完整往返;`null` String 编码为 `-1` 长度前缀。
- 公共编码约定:`null` String→`-1` 前缀;`""`→`0` 前缀;非空→`len`+UTF-8 字节。

- [ ] **Step 1: 写往返测试**

创建 `minidb-protocol/src/test/java/com/minidb/protocol/MetadataMessageCodecTest.java`:

```java
package com.minidb.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MetadataMessageCodecTest {

    private Message roundTrip(Message msg) {
        ByteBuf buf = Unpooled.buffer();
        new MessageEncoder().encode(null, msg, buf);
        return new MessageDecoder().decode(null, buf, null);
    }

    @Test
    void schemasRequestRoundTrip() {
        Message out = roundTrip(new Message.SchemasRequest(7L, "pub%"));
        Message.SchemasRequest s = (Message.SchemasRequest) out;
        assertEquals(7L, s.requestId());
        assertEquals("pub%", s.schemaPattern());
    }

    @Test
    void schemasRequestNullPatternRoundTrip() {
        Message out = roundTrip(new Message.SchemasRequest(1L, null));
        assertNull(((Message.SchemasRequest) out).schemaPattern());
    }

    @Test
    void tablesRequestRoundTripWithNulls() {
        Message out = roundTrip(new Message.TablesRequest(2L, null, "t_%", null));
        Message.TablesRequest t = (Message.TablesRequest) out;
        assertEquals(2L, t.requestId());
        assertNull(t.schemaPattern());
        assertEquals("t_%", t.tableNamePattern());
        assertNull(t.types());
    }

    @Test
    void tablesRequestRoundTripWithTypes() {
        Message out = roundTrip(new Message.TablesRequest(3L, "s", "t", new String[]{"TABLE", "VIEW"}));
        Message.TablesRequest t = (Message.TablesRequest) out;
        assertEquals(3L, t.requestId());
        assertEquals("s", t.schemaPattern());
        assertArrayEquals(new String[]{"TABLE", "VIEW"}, t.types());
    }

    @Test
    void tablesRequestEmptyTypesRoundTrip() {
        Message out = roundTrip(new Message.TablesRequest(4L, null, null, new String[0]));
        Message.TablesRequest t = (Message.TablesRequest) out;
        assertEquals(0, t.types().length);
    }

    @Test
    void columnsRequestRoundTrip() {
        Message out = roundTrip(new Message.ColumnsRequest(5L, "public", "users", "%name%"));
        Message.ColumnsRequest c = (Message.ColumnsRequest) out;
        assertEquals(5L, c.requestId());
        assertEquals("public", c.schemaPattern());
        assertEquals("users", c.tableNamePattern());
        assertEquals("%name%", c.columnNamePattern());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd -pl minidb-protocol test -Dtest=MetadataMessageCodecTest -q`
Expected: 编译失败(`MessageEncoder` 还没处理新 record,`decode` 也没处理)或测试失败(unknown message)。

- [ ] **Step 3: 加编码器分支**

在 `MessageEncoder.encode` 的 `UpdateCount` 分支之后、`else throw` 之前加:

```java
        } else if (msg instanceof Message.SchemasRequest r) {
            out.writeByte(MessageType.SCHEMAS_REQUEST);
            byte[] p = bytes(r.schemaPattern());
            out.writeInt(8 + 4 + p.length);
            out.writeLong(r.requestId());
            out.writeInt(r.schemaPattern() == null ? -1 : p.length);
            if (p.length > 0) out.writeBytes(p);
        } else if (msg instanceof Message.TablesRequest r) {
            byte[] sp = bytes(r.schemaPattern());
            byte[] tp = bytes(r.tableNamePattern());
            int typesLen = r.types() == null ? -1 : r.types().length;
            int body = 8 + 4 + sp.length + 4 + tp.length + 4;
            byte[][] typeBytes = new byte[typesLen < 0 ? 0 : typesLen][];
            for (int i = 0; i < (typesLen < 0 ? 0 : typesLen); i++) {
                typeBytes[i] = bytes(r.types()[i]);
                body += 4 + typeBytes[i].length;
            }
            out.writeByte(MessageType.TABLES_REQUEST);
            out.writeInt(body);
            out.writeLong(r.requestId());
            out.writeInt(r.schemaPattern() == null ? -1 : sp.length);
            if (sp.length > 0) out.writeBytes(sp);
            out.writeInt(r.tableNamePattern() == null ? -1 : tp.length);
            if (tp.length > 0) out.writeBytes(tp);
            out.writeInt(typesLen);
            for (int i = 0; i < (typesLen < 0 ? 0 : typesLen); i++) {
                out.writeInt(typeBytes[i].length);
                if (typeBytes[i].length > 0) out.writeBytes(typeBytes[i]);
            }
        } else if (msg instanceof Message.ColumnsRequest r) {
            byte[] sp = bytes(r.schemaPattern());
            byte[] tp = bytes(r.tableNamePattern());
            byte[] cp = bytes(r.columnNamePattern());
            out.writeByte(MessageType.COLUMNS_REQUEST);
            out.writeInt(8 + 4 + sp.length + 4 + tp.length + 4 + cp.length);
            out.writeLong(r.requestId());
            out.writeInt(r.schemaPattern() == null ? -1 : sp.length);
            if (sp.length > 0) out.writeBytes(sp);
            out.writeInt(r.tableNamePattern() == null ? -1 : tp.length);
            if (tp.length > 0) out.writeBytes(tp);
            out.writeInt(r.columnNamePattern() == null ? -1 : cp.length);
            if (cp.length > 0) out.writeBytes(cp);
        }
```

并在 `MessageEncoder` 末尾加私有 helper:

```java
    private static byte[] bytes(String s) {
        return s == null ? new byte[0] : s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
```

注意:`body` 长度字段语义——`MessageDecoder` 用它决定读多少字节后才回到帧头,必须精确。上述 `body` 是除 `type(1)`+`len(4)` 外的净负载字节数,与现有 `ExecuteRequest`(`8 + 4 + sql.length`)一致。

- [ ] **Step 4: 加解码器分支**

在 `MessageDecoder.decodePayload` 的 `UPDATE_COUNT` 分支之后、`default` 之前加:

```java
            case MessageType.SCHEMAS_REQUEST -> {
                long requestId = in.readLong();
                int pLen = in.readInt();
                String pattern = readNullableString(in, pLen);
                return new Message.SchemasRequest(requestId, pattern);
            }
            case MessageType.TABLES_REQUEST -> {
                long requestId = in.readLong();
                int spLen = in.readInt();
                String schemaPattern = readNullableString(in, spLen);
                int tpLen = in.readInt();
                String tablePattern = readNullableString(in, tpLen);
                int typesLen = in.readInt();
                String[] types;
                if (typesLen < 0) {
                    types = null;
                } else {
                    types = new String[typesLen];
                    for (int i = 0; i < typesLen; i++) {
                        int tLen = in.readInt();
                        types[i] = readNullableString(in, tLen);
                    }
                }
                return new Message.TablesRequest(requestId, schemaPattern, tablePattern, types);
            }
            case MessageType.COLUMNS_REQUEST -> {
                long requestId = in.readLong();
                int spLen = in.readInt();
                String schemaPattern = readNullableString(in, spLen);
                int tpLen = in.readInt();
                String tablePattern = readNullableString(in, tpLen);
                int cpLen = in.readInt();
                String columnPattern = readNullableString(in, cpLen);
                return new Message.ColumnsRequest(requestId, schemaPattern, tablePattern, columnPattern);
            }
```

并在 `MessageDecoder` 末尾加私有 helper:

```java
    private static String readNullableString(ByteBuf in, int len) {
        if (len < 0) return null;
        if (len == 0) return "";
        byte[] b = new byte[len];
        in.readBytes(b);
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }
```

注意:`MessageDecoder.decode` 的 `decodePayload` 是 `switch`(看现有代码用了箭头 `->`),`out.add(decodePayload(type, in))` 已消费 `len` 字节。`len` 帧头由 `decode` 读取校验,`decodePayload` 只管按字段顺序读净负载。

- [ ] **Step 5: 跑测试确认通过**

Run: `./mvnw.cmd -pl minidb-protocol test -Dtest=MetadataMessageCodecTest -q`
Expected: 全部 6 个测试 PASS。

- [ ] **Step 6: 全量编译已有 protocol 测试不回归**

Run: `./mvnw.cmd -pl minidb-protocol test -q`
Expected: 全绿(新测试 + 任何已有 protocol 测试)。

- [ ] **Step 7: 提交**

```bash
git add minidb-protocol/src/main/java/com/minidb/protocol/MessageEncoder.java minidb-protocol/src/main/java/com/minidb/protocol/MessageDecoder.java minidb-protocol/src/test/java/com/minidb/protocol/MetadataMessageCodecTest.java
git commit -m "feat: encode/decode metadata request messages with null-safe string fields"
```

---

### Task 3: 服务端 — MetadataExecutor(getSchemas + 单测)

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java`

**Interfaces:**
- Consumes: `MiniDbCatalog.schemaNames()` → `List<String>`;`MiniDbCatalog.DEFAULT_SCHEMA="public"`。
- Produces: `new MetadataExecutor(MiniDbCatalog catalog, BufferAllocator allocator)`;`VectorSchemaRoot schemas(String schemaPattern)`(2 列:`TABLE_SCHEM` VARCHAR、`TABLE_CAT` VARCHAR 全 null)。

- [ ] **Step 1: 写 getSchemas 单测**

创建 `minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java`:

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataExecutorTest {

    @Test
    void schemasReturnsAllSortedWhenPatternNull() {
        try (RootAllocator alloc = new RootAllocator();
             MiniDbCatalog cat = new MiniDbCatalog()) {
            cat.createSchema("beta");
            cat.createSchema("alpha");
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.schemas(null)) {
                assertEquals(3, root.getRowCount()); // alpha, beta, public
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                assertEquals("alpha", new String(schem.get(0)));
                assertEquals("beta", new String(schem.get(1)));
                assertEquals("public", new String(schem.get(2)));
                assertTrue(root.getVector("TABLE_CAT").isNull(0));
            }
        }
    }

    @Test
    void schemasFilterByLikePattern() {
        try (RootAllocator alloc = new RootAllocator();
             MiniDbCatalog cat = new MiniDbCatalog()) {
            cat.createSchema("prod1");
            cat.createSchema("prod2");
            cat.createSchema("test");
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.schemas("prod%")) {
                assertEquals(2, root.getRowCount());
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                assertEquals("prod1", new String(schem.get(0)));
                assertEquals("prod2", new String(schem.get(1)));
            }
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd -pl minidb-server test -Dtest=MetadataExecutorTest -q`
Expected: 编译失败(`MetadataExecutor` 不存在)。

- [ ] **Step 3: 写 MetadataExecutor 骨架 + schemas + LIKE helper**

创建 `minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java`:

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class MetadataExecutor {

    private static final ArrowType VARCHAR = ArrowType.Utf8.INSTANCE;

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;

    public MetadataExecutor(MiniDbCatalog catalog, BufferAllocator allocator) {
        this.catalog = catalog;
        this.allocator = allocator;
    }

    public VectorSchemaRoot schemas(String schemaPattern) {
        Pattern like = compileLike(schemaPattern);
        List<String> matched = new ArrayList<>();
        for (String s : catalog.schemaNames()) {
            if (like == null || like.matcher(s).matches()) {
                matched.add(s);
            }
        }
        matched.sort(String::compareTo);
        VarCharVector schem = new VarCharVector("TABLE_SCHEM", allocator);
        VarCharVector cat = new VarCharVector("TABLE_CAT", allocator);
        schem.setInitialCapacity(matched.size());
        cat.setInitialCapacity(matched.size());
        schem.allocateNew();
        cat.allocateNew();
        for (int i = 0; i < matched.size(); i++) {
            schem.setSafe(i, matched.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        schem.setValueCount(matched.size());
        cat.setValueCount(matched.size());
        return VectorSchemaRoot.of(schem, cat);
    }

    static Pattern compileLike(String pattern) {
        if (pattern == null) return null;
        StringBuilder sb = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else if ("\\.[]{}()*+?^$|".indexOf(c) >= 0) {
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        return Pattern.compile(sb.toString(), Pattern.DOTALL);
    }
}
```

注意:`catalog.schemaNames()` 返回小写 key(catalog 用 `toLowerCase` 存),故 pattern 直接匹配小写串即可,无需大小写不敏感处理。`TABLE_CAT` 全 null:`cat.setValueCount(n)` 后未 `set` 的位即 null(VarCharVector 默认全 null)。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd -pl minidb-server test -Dtest=MetadataExecutorTest -q`
Expected: 2 个测试 PASS。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java
git commit -m "feat: MetadataExecutor.schemas materializes catalog schemas via LIKE filter"
```

---

### Task 4: 服务端 — MetadataExecutor.tables + columns + 单测

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java`

**Interfaces:**
- Consumes: `MiniDbCatalog.schemaNames()` / `tableNames(schema)` / `getTable(schema, table)` → `TableSchema`(含 `schemaName()`/`name()`/`columns()` → `List<ColumnMeta>`,`ColumnMeta.name()`/`type()`)。`ColumnType` 枚举。`ArrowTypes.toSqlTypeName(ColumnType)`。
- Produces: `VectorSchemaRoot tables(String schemaPattern, String tableNamePattern, String[] types)`(10 列);`VectorSchemaRoot columns(String schemaPattern, String tableNamePattern, String columnNamePattern)`(24 列)。

- [ ] **Step 1: 加 tables 测试**

在 `MetadataExecutorTest.java` 加:

```java
    @Test
    void tablesReturnsAllTablesAcrossSchemas() throws Exception {
        try (RootAllocator alloc = new RootAllocator();
             MiniDbCatalog cat = new MiniDbCatalog()) {
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "users",
                    java.util.List.of(new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER))));
            cat.createSchema("other");
            cat.createTable(new com.minidb.server.catalog.TableSchema("other", "t",
                    java.util.List.of(new com.minidb.server.catalog.ColumnMeta("a", com.minidb.server.catalog.ColumnType.BIGINT))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.tables(null, null, null)) {
                assertEquals(2, root.getRowCount());
                VarCharVector name = (VarCharVector) root.getVector("TABLE_NAME");
                VarCharVector schem = (VarCharVector) root.getVector("TABLE_SCHEM");
                VarCharVector type = (VarCharVector) root.getVector("TABLE_TYPE");
                // sorted by schema then table: other/t, public/users
                assertEquals("t", new String(name.get(0)));
                assertEquals("other", new String(schem.get(0)));
                assertEquals("TABLE", new String(type.get(0)));
                assertEquals("users", new String(name.get(1)));
                assertEquals("public", new String(schem.get(1)));
            }
        }
    }

    @Test
    void tablesFilterBySchemaAndType() throws Exception {
        try (RootAllocator alloc = new RootAllocator();
             MiniDbCatalog cat = new MiniDbCatalog()) {
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "u",
                    java.util.List.of(new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.tables("public", null, new String[]{"VIEW"})) {
                assertEquals(0, root.getRowCount()); // VIEW matches nothing
            }
            try (VectorSchemaRoot root = exec.tables("public", null, new String[]{"TABLE"})) {
                assertEquals(1, root.getRowCount());
            }
        }
    }
```

- [ ] **Step 2: 加 columns 测试**

在 `MetadataExecutorTest.java` 加:

```java
    @Test
    void columnsReturnsAllColumnsWithOrdinalAndType() throws Exception {
        try (RootAllocator alloc = new RootAllocator();
             MiniDbCatalog cat = new MiniDbCatalog()) {
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "users",
                    java.util.List.of(
                            new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER),
                            new com.minidb.server.catalog.ColumnMeta("name", com.minidb.server.catalog.ColumnType.VARCHAR))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.columns(null, null, null)) {
                assertEquals(2, root.getRowCount());
                VarCharVector col = (VarCharVector) root.getVector("COLUMN_NAME");
                VarCharVector typeName = (VarCharVector) root.getVector("TYPE_NAME");
                org.apache.arrow.vector.IntVector dataType =
                        (org.apache.arrow.vector.IntVector) root.getVector("DATA_TYPE");
                org.apache.arrow.vector.IntVector ordinal =
                        (org.apache.arrow.vector.IntVector) root.getVector("ORDINAL_POSITION");
                assertEquals("id", new String(col.get(0)));
                assertEquals("INTEGER", new String(typeName.get(0)));
                assertEquals(java.sql.Types.INTEGER, dataType.get(0));
                assertEquals(1, ordinal.get(0));
                assertEquals("name", new String(col.get(1)));
                assertEquals(2, ordinal.get(1));
            }
        }
    }

    @Test
    void columnsFilterByLikeColumnName() throws Exception {
        try (RootAllocator alloc = new RootAllocator();
             MiniDbCatalog cat = new MiniDbCatalog()) {
            cat.createTable(new com.minidb.server.catalog.TableSchema("public", "users",
                    java.util.List.of(
                            new com.minidb.server.catalog.ColumnMeta("id", com.minidb.server.catalog.ColumnType.INTEGER),
                            new com.minidb.server.catalog.ColumnMeta("username", com.minidb.server.catalog.ColumnType.VARCHAR))));
            MetadataExecutor exec = new MetadataExecutor(cat, alloc);
            try (VectorSchemaRoot root = exec.columns(null, null, "%name%")) {
                assertEquals(1, root.getRowCount());
                VarCharVector col = (VarCharVector) root.getVector("COLUMN_NAME");
                assertEquals("username", new String(col.get(0)));
            }
        }
    }
```

- [ ] **Step 3: 跑测试确认失败**

Run: `./mvnw.cmd -pl minidb-server test -Dtest=MetadataExecutorTest -q`
Expected: 编译失败(`tables`/`columns` 方法不存在)。

- [ ] **Step 4: 实现 tables + 类型映射 helper**

在 `MetadataExecutor.java` 加 import:

```java
import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.TableSchema;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import java.sql.Types;
```

加方法:

```java
    public VectorSchemaRoot tables(String schemaPattern, String tableNamePattern, String[] types) {
        if (!acceptsType(types)) {
            return emptyRoot(tableFields());
        }
        Pattern schemaLike = compileLike(schemaPattern);
        Pattern tableLike = compileLike(tableNamePattern);
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        List<String[]> rows = new ArrayList<>(); // [schema, table]
        for (String schema : schemas) {
            if (schemaLike != null && !schemaLike.matcher(schema).matches()) continue;
            for (String table : catalog.tableNames(schema)) {
                if (tableLike != null && !tableLike.matcher(table).matches()) continue;
                rows.add(new String[]{schema, table});
            }
        }
        return buildTablesRoot(rows);
    }

    private boolean acceptsType(String[] types) {
        if (types == null || types.length == 0) return true;
        for (String t : types) {
            if (t != null && t.equalsIgnoreCase("TABLE")) return true;
        }
        return false;
    }

    private VectorSchemaRoot buildTablesRoot(List<String[]> rows) {
        int n = rows.size();
        VarCharVector cat = vc("TABLE_CAT", n);
        VarCharVector schem = vc("TABLE_SCHEM", n);
        VarCharVector name = vc("TABLE_NAME", n);
        VarCharVector type = vc("TABLE_TYPE", n);
        VarCharVector remarks = vc("REMARKS", n);
        VarCharVector typeCat = vc("TYPE_CAT", n);
        VarCharVector typeSchem = vc("TYPE_SCHEM", n);
        VarCharVector typeName = vc("TYPE_NAME", n);
        VarCharVector selfRef = vc("SELF_REFERENCING_COL_NAME", n);
        VarCharVector refGen = vc("REF_GENERATION", n);
        for (int i = 0; i < n; i++) {
            schem.setSafe(i, rows.get(i)[0].getBytes(java.nio.charset.StandardCharsets.UTF_8));
            name.setSafe(i, rows.get(i)[1].getBytes(java.nio.charset.StandardCharsets.UTF_8));
            type.setSafe(i, "TABLE".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        for (VarCharVector v : new VarCharVector[]{cat, schem, name, type, remarks, typeCat, typeSchem, typeName, selfRef, refGen}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(cat, schem, name, type, remarks, typeCat, typeSchem, typeName, selfRef, refGen);
    }

    private List<Field> tableFields() {
        return java.util.List.of(
                field("TABLE_CAT"), field("TABLE_SCHEM"), field("TABLE_NAME"),
                field("TABLE_TYPE"), field("REMARKS"), field("TYPE_CAT"),
                field("TYPE_SCHEM"), field("TYPE_NAME"),
                field("SELF_REFERENCING_COL_NAME"), field("REF_GENERATION"));
    }

    private VarCharVector vc(String name, int capacity) {
        VarCharVector v = new VarCharVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }

    private static Field field(String name) {
        return new Field(name, FieldType.nullable(VARCHAR), java.util.List.of());
    }
```

并加 `emptyRoot` helper(用于空结果,保证列结构正确):

```java
    private VectorSchemaRoot emptyRoot(List<Field> fields) {
        VectorSchemaRoot root = VectorSchemaRoot.of(fields.stream()
                .map(f -> {
                    FieldVector v = f.createVector(allocator);
                    v.allocateNew();
                    return v;
                })
                .toArray(FieldVector[]::new));
        root.setRowCount(0);
        return root;
    }
```

- [ ] **Step 5: 实现 columns + 类型映射 helper**

在 `MetadataExecutor.java` 加:

```java
    public VectorSchemaRoot columns(String schemaPattern, String tableNamePattern, String columnNamePattern) {
        Pattern schemaLike = compileLike(schemaPattern);
        Pattern tableLike = compileLike(tableNamePattern);
        Pattern colLike = compileLike(columnNamePattern);
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        List<Row> rows = new ArrayList<>();
        for (String schema : schemas) {
            if (schemaLike != null && !schemaLike.matcher(schema).matches()) continue;
            for (String table : catalog.tableNames(schema)) {
                if (tableLike != null && !tableLike.matcher(table).matches()) continue;
                TableSchema ts = catalog.getTable(schema, table);
                List<ColumnMeta> cols = ts.columns();
                for (int idx = 0; idx < cols.size(); idx++) {
                    ColumnMeta col = cols.get(idx);
                    if (colLike != null && !colLike.matcher(col.name()).matches()) continue;
                    rows.add(new Row(schema, table, col, idx + 1));
                }
            }
        }
        return buildColumnsRoot(rows);
    }

    private record Row(String schema, String table, ColumnMeta column, int ordinal) {}

    private VectorSchemaRoot buildColumnsRoot(List<Row> rows) {
        int n = rows.size();
        VarCharVector tableCat = vc("TABLE_CAT", n);
        VarCharVector tableSchem = vc("TABLE_SCHEM", n);
        VarCharVector tableName = vc("TABLE_NAME", n);
        VarCharVector colName = vc("COLUMN_NAME", n);
        IntVector dataType = intVec("DATA_TYPE", n);
        VarCharVector typeName = vc("TYPE_NAME", n);
        IntVector colSize = intVec("COLUMN_SIZE", n);
        IntVector bufLen = intVec("BUFFER_LENGTH", n);
        IntVector decDigits = intVec("DECIMAL_DIGITS", n);
        IntVector numPrecRadix = intVec("NUM_PREC_RADIX", n);
        IntVector nullable = intVec("NULLABLE", n);
        VarCharVector remarks = vc("REMARKS", n);
        VarCharVector colDef = vc("COLUMN_DEF", n);
        IntVector sqlDataType = intVec("SQL_DATA_TYPE", n);
        IntVector sqlDateTimeSub = intVec("SQL_DATETIME_SUB", n);
        IntVector charOctetLen = intVec("CHAR_OCTET_LENGTH", n);
        IntVector ordinal = intVec("ORDINAL_POSITION", n);
        VarCharVector isNullable = vc("IS_NULLABLE", n);
        VarCharVector scopeCat = vc("SCOPE_CATALOG", n);
        VarCharVector scopeSchem = vc("SCOPE_SCHEMA", n);
        VarCharVector scopeTable = vc("SCOPE_TABLE", n);
        SmallIntVector sourceDataType = smallInt("SOURCE_DATA_TYPE", n);
        VarCharVector isAutoInc = vc("IS_AUTOINCREMENT", n);
        VarCharVector isGenCol = vc("IS_GENERATEDCOLUMN", n);
        for (int i = 0; i < n; i++) {
            Row r = rows.get(i);
            tableSchem.setSafe(i, r.schema().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            tableName.setSafe(i, r.table().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            colName.setSafe(i, r.column().name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            dataType.setSafe(i, sqlType(r.column().type()));
            typeName.setSafe(i, ArrowTypes.toSqlTypeName(r.column().type()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            colSize.setSafe(i, 0);
            bufLen.setSafe(i, 0);
            decDigits.setSafe(i, 0);
            if (isIntegerType(r.column().type())) numPrecRadix.setSafe(i, 10);
            nullable.setSafe(i, 1); // columnNullable
            ordinal.setSafe(i, r.ordinal());
            isNullable.setSafe(i, "YES".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            isAutoInc.setSafe(i, "NO".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            isGenCol.setSafe(i, "NO".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        for (VarCharVector v : new VarCharVector[]{tableCat, tableSchem, tableName, colName, typeName,
                remarks, colDef, isNullable, scopeCat, scopeSchem, scopeTable, isAutoInc, isGenCol}) {
            v.setValueCount(n);
        }
        for (IntVector v : new IntVector[]{dataType, colSize, bufLen, decDigits, numPrecRadix,
                nullable, sqlDataType, sqlDateTimeSub, charOctetLen, ordinal}) {
            v.setValueCount(n);
        }
        sourceDataType.setValueCount(n);
        return VectorSchemaRoot.of(tableCat, tableSchem, tableName, colName, dataType, typeName,
                colSize, bufLen, decDigits, numPrecRadix, nullable, remarks, colDef,
                sqlDataType, sqlDateTimeSub, charOctetLen, ordinal, isNullable,
                scopeCat, scopeSchem, scopeTable, sourceDataType, isAutoInc, isGenCol);
    }

    private IntVector intVec(String name, int capacity) {
        IntVector v = new IntVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }

    private SmallIntVector smallInt(String name, int capacity) {
        SmallIntVector v = new SmallIntVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }

    private static int sqlType(ColumnType type) {
        return switch (type) {
            case INTEGER -> Types.INTEGER;
            case BIGINT -> Types.BIGINT;
            case DOUBLE -> Types.DOUBLE;
            case VARCHAR -> Types.VARCHAR;
            case BOOLEAN -> Types.BOOLEAN;
            case DATE -> Types.DATE;
            case TIMESTAMP -> Types.TIMESTAMP;
        };
    }

    private static boolean isIntegerType(ColumnType type) {
        return type == ColumnType.INTEGER || type == ColumnType.BIGINT;
    }
```

注意:`NUM_PREC_RADIX` 对非整数类型留 null(`setValueCount` 后未 `setSafe` 即 null)。`SmallIntVector` import 为 `org.apache.arrow.vector.SmallIntVector`。

- [ ] **Step 6: 跑测试确认通过**

Run: `./mvnw.cmd -pl minidb-server test -Dtest=MetadataExecutorTest -q`
Expected: 全部测试 PASS(2 个 schemas + 2 个 tables + 2 个 columns)。

- [ ] **Step 7: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/MetadataExecutor.java minidb-server/src/test/java/com/minidb/server/exec/MetadataExecutorTest.java
git commit -m "feat: MetadataExecutor.tables/columns materialize catalog with JDBC column structure"
```

---

### Task 5: 服务端 — SessionHandler 接入

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java`

**Interfaces:**
- Consumes: Task 2 的 `Message.SchemasRequest`/`TablesRequest`/`ColumnsRequest`;Task 3-4 的 `MetadataExecutor.{schemas,tables,columns}`;现有 `sendRows(ctx, requestId, root)`。
- Produces: `SessionHandler` 识别 3 个新消息并回 `ArrowBatch`。

- [ ] **Step 1: 加分支到 channelRead0**

`SessionHandler` 构造器加 `MetadataExecutor` 字段。修改构造器与 `channelRead0`:

构造器从:
```java
    private final QueryExecutor executor;
    private String currentSchema = MiniDbCatalog.DEFAULT_SCHEMA;

    public SessionHandler(QueryExecutor executor) {
        this.executor = executor;
    }
```
改为:
```java
    private final QueryExecutor executor;
    private final MetadataExecutor metadata;
    private String currentSchema = MiniDbCatalog.DEFAULT_SCHEMA;

    public SessionHandler(QueryExecutor executor, MetadataExecutor metadata) {
        this.executor = executor;
        this.metadata = metadata;
    }
```

`channelRead0` 在 `CloseRequest` 分支之前(或 `ExecuteRequest` 之后)加 3 个分支:

```java
        } else if (msg instanceof Message.SchemasRequest req) {
            handleMetadata(ctx, req.requestId(), () -> metadata.schemas(req.schemaPattern()));
        } else if (msg instanceof Message.TablesRequest req) {
            handleMetadata(ctx, req.requestId(), () -> metadata.tables(
                    req.schemaPattern(), req.tableNamePattern(), req.types()));
        } else if (msg instanceof Message.ColumnsRequest req) {
            handleMetadata(ctx, req.requestId(), () -> metadata.columns(
                    req.schemaPattern(), req.tableNamePattern(), req.columnNamePattern()));
        }
```

加 import:
```java
import com.minidb.server.exec.MetadataExecutor;
import java.util.function.Supplier;
import org.apache.arrow.vector.VectorSchemaRoot;
```

加私有方法:

```java
    private void handleMetadata(ChannelHandlerContext ctx, long requestId,
                                java.util.function.Supplier<VectorSchemaRoot> supplier) {
        long start = System.nanoTime();
        try {
            VectorSchemaRoot root = supplier.get();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            LOG.info("metadata ok: {} rows in {} ms", root.getRowCount(), elapsedMs);
            sendRows(ctx, requestId, root);
            root.close();
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            LOG.warn("metadata failed in {} ms", elapsedMs, e);
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.writeAndFlush(Message.ExecuteResponse.error(requestId, message));
        }
    }
```

- [ ] **Step 2: 修 MiniDbServer 传参**

`MiniDbServer.start` 创建 `SessionHandler` 处从 `new SessionHandler(executor)` 改为 `new SessionHandler(executor, new MetadataExecutor(catalog, allocator))`。`catalog` 是 `MiniDbServer` 字段,`allocator` 已在 `start` 里初始化。

- [ ] **Step 3: 编译**

Run: `./mvnw.cmd -pl minidb-server -am compile -q`
Expected: 无输出,退出码 0。

- [ ] **Step 4: 跑服务端全量测试不回归**

Run: `./mvnw.cmd -pl minidb-server test -q`
Expected: 全绿(`MetadataExecutorTest` + 已有服务端测试)。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java minidb-server/src/main/java/com/minidb/server/MiniDbServer.java
git commit -m "feat: SessionHandler dispatches metadata requests via MetadataExecutor"
```

---

### Task 6: 客户端 — MiniDbClient 加 3 方法 + 公共 send/await

**Files:**
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java`

**Interfaces:**
- Consumes: Task 1-2 的 3 个 `Message` record;现有 `readArrow`、`pending`、`nextRequestId`、`connected`、`channel`、`timeoutSeconds`。
- Produces: `VectorSchemaRoot schemas(String schemaPattern)`、`VectorSchemaRoot tables(String, String, String[])`、`VectorSchemaRoot columns(String, String, String)`,各抛 `SQLException`(同 `execute`)。

- [ ] **Step 1: 先不写单测(客户端逻辑由 Task 8 端到端覆盖),直接实现**

在 `MiniDbClient.java` 加 import:
```java
import org.apache.arrow.vector.VectorSchemaRoot;
```

加 3 个 public 方法(在 `execute` 之后):

```java
    public VectorSchemaRoot schemas(String schemaPattern) throws SQLException {
        return sendMetadata(new Message.SchemasRequest(allocateRequestId(), schemaPattern));
    }

    public VectorSchemaRoot tables(String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        return sendMetadata(new Message.TablesRequest(allocateRequestId(),
                schemaPattern, tableNamePattern, types));
    }

    public VectorSchemaRoot columns(String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        return sendMetadata(new Message.ColumnsRequest(allocateRequestId(),
                schemaPattern, tableNamePattern, columnNamePattern));
    }

    private long allocateRequestId() throws SQLException {
        if (!connected) {
            throw new SQLException("connection is closed");
        }
        return nextRequestId.getAndIncrement();
    }

    private VectorSchemaRoot sendMetadata(Message req) throws SQLException {
        long id = req instanceof Message.SchemasRequest r ? r.requestId()
                : req instanceof Message.TablesRequest t ? t.requestId()
                : ((Message.ColumnsRequest) req).requestId();
        CompletableFuture<ClientResult> fut = new CompletableFuture<>();
        pending.put(id, fut);
        if (!connected) {
            pending.remove(id, fut);
            throw new SQLException("connection is closed");
        }
        try {
            channel.writeAndFlush(req).sync();
        } catch (Exception e) {
            pending.remove(id, fut);
            throw new SQLException("failed to send request", e);
        }
        try {
            ClientResult result = fut.get(timeoutSeconds, TimeUnit.SECONDS);
            if (result instanceof ClientResult.Rows rows) {
                return rows.data();
            }
            throw new SQLException("unexpected result type for metadata request");
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

注意:`ResponseCollector` 对 `ArrowBatch` 已 `f.complete(new ClientResult.Rows(arrowDecoder.decode(...)))`,对 `ExecuteResponse.error` 已 `f.completeExceptionally(new SQLException(r.error()))`,故 `sendMetadata` 收到 Rows 即解码后的 `VectorSchemaRoot`,收到 error 即抛 `SQLException`。无需改 `ResponseCollector`。

- [ ] **Step 2: 编译**

Run: `./mvnw.cmd -pl minidb-jdbc -am compile -q`
Expected: 无输出,退出码 0。

- [ ] **Step 3: 提交**

```bash
git add minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbClient.java
git commit -m "feat: MiniDbClient sends metadata requests and returns decoded VectorSchemaRoot"
```

---

### Task 7: 客户端 — MiniDbDatabaseMetaData 实现 3 方法

**Files:**
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbDatabaseMetaData.java`

**Interfaces:**
- Consumes: Task 6 的 `MiniDbClient.{schemas,tables,columns}`;`MiniDbConnection.client()`(包级访问);`MiniDbResultSet(MiniDbStatement, VectorSchemaRoot)`。
- Produces: `getSchemas()`、`getSchemas(String, String)`、`getTables(String, String, String, String[])`、`getColumns(String, String, String, String)` 各返回 `ResultSet`。

- [ ] **Step 1: 实现 4 个方法(替换抛异常的桩)**

把 `getTables`、`getSchemas()`、`getColumns`、`getSchemas(String, String)` 四个方法的 `throw new SQLFeatureNotSupportedException()` 替换为:

```java
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        MiniDbStatement stmt = (MiniDbStatement) connection.createStatement();
        return new MiniDbResultSet(stmt, connection.client().tables(schemaPattern, tableNamePattern, types));
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        MiniDbStatement stmt = (MiniDbStatement) connection.createStatement();
        return new MiniDbResultSet(stmt, connection.client().columns(schemaPattern, tableNamePattern, columnNamePattern));
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        MiniDbStatement stmt = (MiniDbStatement) connection.createStatement();
        return new MiniDbResultSet(stmt, connection.client().schemas(schemaPattern));
    }
```

注意:`connection` 字段是 `MiniDbConnection`,`client()` 是包级方法(同包 `com.minidb.jdbc`)。`catalog` 入参按 spec 决策忽略。`MiniDbResultSet` 构造器是包级 `MiniDbResultSet(MiniDbStatement, VectorSchemaRoot)`。

- [ ] **Step 2: 编译**

Run: `./mvnw.cmd -pl minidb-jdbc -am compile -q`
Expected: 无输出,退出码 0。

- [ ] **Step 3: 提交**

```bash
git add minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbDatabaseMetaData.java
git commit -m "feat: MiniDbDatabaseMetaData getSchemas/getTables/getColumns delegate to client"
```

---

### Task 8: 端到端集成测试

**Files:**
- Test: `minidb-jdbc/src/test/java/com/minidb/jdbc/DatabaseMetaDataTest.java`

**Interfaces:**
- Consumes: Task 5-7 的完整链路;`MiniDbServer.start(0, Path)` + `port()`;`DriverManager.getConnection("jdbc:minidb://127.0.0.1:" + port)`;SQL `CREATE SCHEMA`/`CREATE TABLE`。

- [ ] **Step 1: 写集成测试**

创建 `minidb-jdbc/src/test/java/com/minidb/jdbc/DatabaseMetaDataTest.java`:

```java
package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseMetaDataTest {

    private static final String SELECT = "select";

    @Test
    void getSchemasListsPublicAndCustomSchema() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA other");
            DatabaseMetaData md = c.getMetaData();
            Set<String> schemas = new HashSet<>();
            try (ResultSet rs = md.getSchemas()) {
                while (rs.next()) {
                    schemas.add(rs.getString("TABLE_SCHEM"));
                }
            }
            assertTrue(schemas.contains("public"));
            assertTrue(schemas.contains("other"));
        } finally {
            server.close();
        }
    }

    @Test
    void getTablesAndColumnsRoundTrip() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE public.users (id INTEGER, name VARCHAR)");
            s.execute("CREATE SCHEMA other");
            s.execute("CREATE TABLE other.t (a BIGINT, b BOOLEAN)");
            DatabaseMetaData md = c.getMetaData();

            Set<String> tables = new HashSet<>();
            try (ResultSet rs = md.getTables(null, null, null, null)) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
                    assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                }
            }
            assertTrue(tables.contains("public.users"));
            assertTrue(tables.contains("other.t"));

            try (ResultSet rs = md.getTables(null, "public", null, null)) {
                assertTrue(rs.next());
                assertEquals("users", rs.getString("TABLE_NAME"));
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getTables(null, null, null, new String[]{"VIEW"})) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getColumns(null, null, "users", null)) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString("COLUMN_NAME"));
                assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                assertEquals(java.sql.Types.INTEGER, rs.getInt("DATA_TYPE"));
                assertEquals(1, rs.getInt("ORDINAL_POSITION"));
                assertTrue(rs.next());
                assertEquals("name", rs.getString("COLUMN_NAME"));
                assertEquals(2, rs.getInt("ORDINAL_POSITION"));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void getColumnsFilterByLikeColumnName() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE public.users (id INTEGER, username VARCHAR)");
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getColumns(null, null, null, "%name%")) {
                assertTrue(rs.next());
                assertEquals("username", rs.getString("COLUMN_NAME"));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }
}
```

- [ ] **Step 2: 跑集成测试确认通过**

Run: `./mvnw.cmd -pl minidb-jdbc test -Dtest=DatabaseMetaDataTest -q`
Expected: 全部 3 个测试 PASS。若失败,看 `server.close()` 前是否正确、协议编解码是否往返、`MetadataExecutor` 列名是否与断言一致。

- [ ] **Step 3: 全量测试不回归**

Run: `./mvnw.cmd test -q`
Expected: 全绿。注:`minidb-jdbc` 的部分已有测试若因环境 `NoClassDefFoundError` 失败(CLAUDE.md 坑 12),与服务端改动无关,不算回归——但要确认本次新增的 3 个测试 PASS。

- [ ] **Step 4: 提交**

```bash
git add minidb-jdbc/src/test/java/com/minidb/jdbc/DatabaseMetaDataTest.java
git commit -m "test: end-to-end integration tests for getSchemas/getTables/getColumns"
```

---

### Task 9: 收尾 — CLAUDE.md 更新与全量验证

**Files:**
- Modify: `.claude/CLAUDE.md`

- [ ] **Step 1: 全量构建+测试**

Run: `./mvnw.cmd test -q`
Expected: 全绿(同 Task 8 Step 3 的环境说明)。

- [ ] **Step 2: 更新 CLAUDE.md**

在 `## 踩过的坑(经验教训)` 末尾加:

```markdown
19. **JDBC 元数据走专用协议消息**——`getSchemas`/`getTables`/`getColumns` 不复用 `ExecuteRequest`+伪SQL,而是 `minidb-protocol` 的 `SchemasRequest`/`TablesRequest`/`ColumnsRequest` 三条独立消息(响应复用 `ArrowBatch`)。服务端 `MetadataExecutor`(外挂,持 `catalog+allocator`,不依赖 storage/stats)从 `MiniDbCatalog` 物化 Arrow 行,`SessionHandler.handleMetadata` 走现有 `sendRows`。`TABLE_CAT` 恒 null(MiniDB 无 catalog 概念,`getCatalog()=null`);`NULLABLE` 恒 1(列全可空);`getColumns` 24 列完整 JDBC 规范,无语义列填默认(`COLUMN_SIZE=0`/`NUM_PREC_RADIX=10`仅整数/`IS_NULLABLE="YES"` 等)。LIKE 过滤(`_`/`%`)在 `MetadataExecutor.compileLike` 转正则,`null` pattern 跳过过滤。
```

在 `### 关键类` 的 `**网络:**` 节 `SessionHandler` 描述后加一句:`MetadataExecutor`(外挂,`catalog+allocator`)服务 `getSchemas`/`getTables`/`getColumns` 协议请求,`SessionHandler.handleMetadata` 走 `sendRows`。

- [ ] **Step 3: 提交**

```bash
git add .claude/CLAUDE.md
git commit -m "docs: document JDBC metadata protocol messages and MetadataExecutor in CLAUDE.md"
```

## Self-Review (写计划后自查)

1. **Spec 覆盖**:
   - 协议 3 消息+null 编码 → Task 1-2 ✓
   - MetadataExecutor getSchemas/tables/columns + 2/10/24 列 → Task 3-4 ✓
   - SessionHandler 接入 → Task 5 ✓
   - 客户端 3 方法 + 公共 send/await → Task 6 ✓
   - MiniDbDatabaseMetaData 4 方法(含 `getSchemas()` 无参重载)→ Task 7 ✓
   - 端到端测试(含 getSchemas、getTables 各过滤、getColumns 各过滤)→ Task 8 ✓
   - 错误处理(复用 ExecuteResponse.error + try-catch)→ Task 5 `handleMetadata` + Task 6 `sendMetadata` ✓
   - 资源管理(`root.close()` 照 Rows 路径)→ Task 5 ✓
   - 类型映射对齐 MiniDbResultSetMetaData → Task 4 `sqlType` + spec 表 ✓
   - LIKE→正则 + null/""/通配 → Task 3 `compileLike` + Task 4 过滤调用 ✓
   - 排序(schema/table/ordinal)→ Task 3-4 ✓
   - CLAUDE.md 文档 → Task 9 ✓

2. **占位符扫描**:无 TBD/TODO;每步含实际代码或确切命令。

3. **类型一致性**:
   - `MetadataExecutor` 构造器 `(MiniDbCatalog, BufferAllocator)` 在 Task 3、5 一致 ✓
   - `schemas`/`tables`/`columns` 签名在 Task 3-4(定义)、Task 5(调用)、Task 6-7(客户端翻译)一致 ✓
   - `MiniDbClient.schemas(String)`/`tables(String,String,String[])`/`columns(String,String,String)` 在 Task 6(定义)、Task 7(调用)一致 ✓
   - `SessionHandler(QueryExecutor, MetadataExecutor)` 在 Task 5 定义、`MiniDbServer.start` 调用一致 ✓
   - Message record 字段名(`schemaPattern`/`tableNamePattern`/`columnNamePattern`/`types`/`requestId`)在 Task 1-2、5-6 一致 ✓
   - `compileLike(String)→Pattern` 在 Task 3 定义、Task 4 调用一致 ✓
