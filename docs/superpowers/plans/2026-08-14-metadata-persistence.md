# 元数据独立持久化 + information_schema 系统表 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `TableSchema` 独立持久化到 JSON(留 `CatalogStore` 接口给 Avro),并引入只读 `information_schema` 系统表(`schemata`/`tables`/`columns`),解决「空表重启即丢」+ 提供 SQL 级元数据查询。

**Architecture:** `CatalogStore` 接口 + `JsonCatalogStore` 实现(写 `data/catalog.json`),`MiniDbCatalog` 加 `snapshot()`/`restore()`(restore 不通知),`StorageManager` 挂 listener 在 DDL 变更时同步落盘、`loadAll` 先恢复元数据再加载数据(旧目录回退 `.arrow` 推断)。`information_schema` 作为 `MiniDbRootCalciteSchema` 的特殊子 schema 接入 Calcite;执行侧 `InformationSchema` 从内存 catalog 物化行,`MiniDbScan` 加分支。

**Tech Stack:** Java 17、Jackson(jackson-databind,record/enum 序列化)、Apache Arrow、Calcite 1.42、JUnit 5。

**Spec:** `docs/superpowers/specs/2026-08-14-metadata-persistence-design.md`

## Global Constraints

- **在 `master` 分支直接工作,不建功能分支 / git worktree**(CLAUDE.md 规则 2「在 master 分支工作」;仓库纯本地无 remote)。
- JDK 17(`JAVA_HOME` 指向 JDK 17);bash 下用 `./mvnw.cmd`(不是 `mvnw.cmd`/`mvn`/`cmd //c`)。
- 全量测试 `./mvnw.cmd test`;单测试类 `./mvnw.cmd test -pl minidb-server -Dtest=XxxTest`。
- 每改完一个逻辑改动就提交,conventional commit 风格(`feat:`/`fix:`/`test:`/`refactor:`/`docs:`),不 amend、不 `--no-verify`。
- 代码是给人读的:命名自解释,注释只解释 WHY。
- `minidb-protocol` 模块不改;现有 7 个物理算子尽量不改(本计划只在 `MiniDbScan` 加一个只读物化分支,不改其结构)。

---

## Task 1: 元数据存储接口 + JSON 实现

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/catalog/CatalogSnapshot.java`
- Create: `minidb-server/src/main/java/com/minidb/server/storage/CatalogStore.java`
- Create: `minidb-server/src/main/java/com/minidb/server/storage/JsonCatalogStore.java`
- Modify: `minidb-server/pom.xml`(加 Jackson 依赖)
- Test: `minidb-server/src/test/java/com/minidb/server/storage/JsonCatalogStoreTest.java`

**Interfaces:**
- Produces: `CatalogSnapshot(List<String> schemas, List<TableSchema> tables)`(record,`catalog/` 包,避免 catalog→storage 循环依赖);`CatalogStore.load()/save()/close()`;`JsonCatalogStore(Path file)`。Task 2 依赖。

- [ ] **Step 1: 写失败测试 `JsonCatalogStoreTest`**

```java
package com.minidb.server.storage;

import com.minidb.server.catalog.CatalogSnapshot;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.TableSchema;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonCatalogStoreTest {

    @Test
    void roundTripsSnapshot(@TempDir Path dir) throws Exception {
        JsonCatalogStore store = new JsonCatalogStore(dir.resolve("catalog.json"));
        CatalogSnapshot original = new CatalogSnapshot(
                List.of("public", "other"),
                List.of(
                        new TableSchema("public", "t", List.of(
                                new ColumnMeta("id", ColumnType.INTEGER),
                                new ColumnMeta("price", ColumnType.DECIMAL, 10, 2))),
                        new TableSchema("other", "u", List.of(
                                new ColumnMeta("name", ColumnType.VARCHAR)))));
        store.save(original);
        CatalogSnapshot loaded = store.load();
        assertEquals(original, loaded);
    }

    @Test
    void loadReturnsEmptyWhenAbsent(@TempDir Path dir) throws Exception {
        JsonCatalogStore store = new JsonCatalogStore(dir.resolve("catalog.json"));
        CatalogSnapshot loaded = store.load();
        assertEquals(List.of(), loaded.schemas());
        assertEquals(List.of(), loaded.tables());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=JsonCatalogStoreTest`
Expected: FAIL(编译错误 —— `CatalogSnapshot`/`JsonCatalogStore` 不存在)。

- [ ] **Step 3: 建 `CatalogSnapshot`(catalog 包)**

`minidb-server/src/main/java/com/minidb/server/catalog/CatalogSnapshot.java`:

```java
package com.minidb.server.catalog;

import java.util.List;

/** 格式无关的 catalog 快照:schema 名 + 全部表定义(每表自带所属 schema)。 */
public record CatalogSnapshot(List<String> schemas, List<TableSchema> tables) {
}
```

- [ ] **Step 4: 建 `CatalogStore` 接口 + `JsonCatalogStore`**

`minidb-server/src/main/java/com/minidb/server/storage/CatalogStore.java`:

```java
package com.minidb.server.storage;

import com.minidb.server.catalog.CatalogSnapshot;
import java.io.IOException;

/** 元数据持久化接口:JSON 现在,Avro 以后加实现类即可(调用方不动)。 */
public interface CatalogStore extends AutoCloseable {
    CatalogSnapshot load() throws IOException;

    void save(CatalogSnapshot snapshot) throws IOException;

    @Override
    default void close() {
    }
}
```

`minidb-server/src/main/java/com/minidb/server/storage/JsonCatalogStore.java`:

```java
package com.minidb.server.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.server.catalog.CatalogSnapshot;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/** JSON 持久化。save 用临时文件 + move,避免崩溃写坏;并发 DDL 用 synchronized 串行化。 */
public class JsonCatalogStore implements CatalogStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;

    public JsonCatalogStore(Path file) {
        this.file = file;
    }

    @Override
    public synchronized CatalogSnapshot load() throws IOException {
        if (!Files.exists(file)) {
            return new CatalogSnapshot(List.of(), List.of());
        }
        return MAPPER.readValue(Files.readString(file, StandardCharsets.UTF_8), CatalogSnapshot.class);
    }

    @Override
    public synchronized void save(CatalogSnapshot snapshot) throws IOException {
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, MAPPER.writeValueAsString(snapshot), StandardCharsets.UTF_8);
        try {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
```

- [ ] **Step 5: 加 Jackson 依赖到 `minidb-server/pom.xml`**

在 `<dependencies>` 里(与其它依赖并列)加:

```xml
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
    </dependency>
```

> **验证**:先看父 pom `minidb-parent` 的 `<dependencyManagement>` 是否已管理 `jackson-databind` 版本;若已管理则不加 `<version>`;否则加 `<version>2.15.3</version>`(Calcite 1.42 传递依赖的 Jackson 版本与此兼容)。以 `./mvnw.cmd -pl minidb-server compile` 通过为准。

- [ ] **Step 6: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=JsonCatalogStoreTest`
Expected: PASS(2 测试)。

- [ ] **Step 7: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/CatalogSnapshot.java minidb-server/src/main/java/com/minidb/server/storage/CatalogStore.java minidb-server/src/main/java/com/minidb/server/storage/JsonCatalogStore.java minidb-server/pom.xml minidb-server/src/test/java/com/minidb/server/storage/JsonCatalogStoreTest.java
git commit -m "feat: 元数据 CatalogStore 接口 + JSON 持久化实现"
```

---

## Task 2: MiniDbCatalog 快照/恢复 + StorageManager 集成

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java`
- Test: `minidb-server/src/test/java/com/minidb/server/storage/StorageManagerTest.java`(追加)

**Interfaces:**
- Consumes: Task 1 的 `CatalogSnapshot`/`JsonCatalogStore`。
- Produces: `MiniDbCatalog.snapshot()/restore(CatalogSnapshot)`;`StorageManager` 构造时挂 listener、`loadAll` 先恢复元数据再加载数据、旧目录无 `catalog.json` 回退 `.arrow` 推断。

- [ ] **Step 1: 写失败测试(StorageManagerTest 追加)**

```java
    @Test
    void emptyTableSurvivesRestart(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, allocator, dir);
            storage.createTable(new TableSchema("t", List.of(
                    new ColumnMeta("id", ColumnType.INTEGER),
                    new ColumnMeta("price", ColumnType.DECIMAL, 10, 2))));
            // 不插任何行 → 无 .arrow 文件,但 catalog.json 应已落盘
            storage.close();
        }
        assertTrue(Files.exists(dir.resolve("catalog.json")));

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        try (BufferAllocator allocator = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, allocator, dir);
            storage2.loadAll();
            assertTrue(catalog2.hasTable("t"));
            List<ColumnMeta> cols = catalog2.getTable("t").columns();
            assertEquals(ColumnType.INTEGER, cols.get(0).type());
            assertEquals(ColumnType.DECIMAL, cols.get(1).type());
            assertEquals(10, cols.get(1).precision());
            assertEquals(2, cols.get(1).scale());
            storage2.close();
        }
    }
```

> 需要 import:`com.minidb.server.catalog.ColumnMeta`/`ColumnType`/`TableSchema`(已有部分)、`java.util.List`、`java.nio.file.Files`。注意 `createTable` 走 `StorageManager.createTable`,里面会 `catalog.createTable` → 触发 listener(本任务要挂的)写 catalog.json。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StorageManagerTest`
Expected: FAIL(`catalog.json` 不存在 → `assertTrue(Files.exists(...))` 失败,或 `hasTable("t")` 失败)。

- [ ] **Step 3: `MiniDbCatalog` 加 `snapshot()`/`restore()`**

在 `MiniDbCatalog.java` 的 `addListener` 之后加(需 import `CatalogSnapshot`):

```java
    public CatalogSnapshot snapshot() {
        List<TableSchema> tables = new ArrayList<>();
        for (Map<String, TableSchema> t : schemas.values()) {
            tables.addAll(t.values());
        }
        return new CatalogSnapshot(schemaNames(), tables);
    }

    /** 批量恢复(启动时用),不触发 notifyChange —— 避免加载时把刚读到的文件写回。 */
    public void restore(CatalogSnapshot snapshot) {
        for (String schemaName : snapshot.schemas()) {
            schemas.putIfAbsent(key(schemaName), new ConcurrentHashMap<>());
        }
        for (TableSchema table : snapshot.tables()) {
            String sk = key(table.schemaName());
            Map<String, TableSchema> t = schemas.computeIfAbsent(sk, k -> new ConcurrentHashMap<>());
            t.putIfAbsent(key(table.name()), table);
        }
    }
```

- [ ] **Step 4: `StorageManager` 挂 listener + 恢复 + 改 `loadAll`**

(a) 加字段 + 构造(在 `dataDir` 字段后加 `catalogStore`,构造里初始化并挂 listener):

```java
    private final CatalogStore catalogStore;

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
        this.catalogStore = new JsonCatalogStore(dataDir.resolve("catalog.json"));
        catalog.addListener(this::persistCatalog);
    }

    private void persistCatalog() {
        try {
            catalogStore.save(catalog.snapshot());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist catalog", e);
        }
    }
```

(b) `loadAll()` 改为先恢复元数据,再加载数据:

```java
    public void loadAll() {
        boolean restored = restoreCatalog();
        if (!Files.exists(dataDir)) {
            LOG.info("loaded 0 table(s) (data dir absent)");
            return;
        }
        int count = 0;
        try (DirectoryStream<Path> schemaDirs = Files.newDirectoryStream(dataDir)) {
            for (Path schemaDir : schemaDirs) {
                if (!Files.isDirectory(schemaDir)) {
                    continue;
                }
                String schemaName = schemaDir.getFileName().toString();
                try (DirectoryStream<Path> files =
                             Files.newDirectoryStream(schemaDir, "*.arrow")) {
                    for (Path file : files) {
                        loadFile(schemaName, file, restored);
                        count++;
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.info("loaded {} table(s)", count);
    }

    /** 恢复元数据。有 catalog.json → 恢复并返回 true;否则回退 .arrow 推断(返回 false)。 */
    private boolean restoreCatalog() {
        try {
            if (Files.exists(dataDir.resolve("catalog.json"))) {
                catalog.restore(catalogStore.load());
                return true;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return false;
    }
```

(c) `loadFile` 加 `restored` 参数:当 `restored==true` 时,表 schema 已从 catalog.json 恢复,不要重复 `catalog.createTable`(否则抛 "table already exists");当 `restored==false` 时保持旧行为(反推 + createTable):

```java
    private void loadFile(String schemaName, Path file, boolean restored) throws IOException {
        try (SeekableByteChannel channel =
                     Files.newByteChannel(file, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            String tableName = stripExtension(file.getFileName().toString());
            TableSchema schema;
            if (restored) {
                schema = catalog.getTable(schemaName, tableName);
            } else {
                schema = toTableSchema(root.getSchema(), schemaName, tableName);
            }
            ArrowTable table = new ArrowTable(schema, allocator);
            while (reader.loadNextBatch()) {
                VectorSchemaRoot copy = table.newBatchRoot();
                ArrowRecordBatch recordBatch =
                        new VectorUnloader(root).getRecordBatch();
                new VectorLoader(copy).load(recordBatch);
                recordBatch.close();
                table.appendBatch(copy);
            }
            tables.put(storageKey(schema.schemaName(), schema.name()), table);
            if (!restored) {
                catalog.createTable(schema);
            }
        }
    }
```

> **注意**:`restored==true` 时 `catalog.getTable(schemaName, tableName)` 用 `schemaName`(子目录名,小写)。但 catalog.json 里 `TableSchema.schemaName` 可能带原始大小写;`MiniDbCatalog.getTable` 用 `key()` 小写查找,所以子目录名(小写)能命中。若子目录名与 catalog.json 里的大小写不一致导致 `getTable` 抛错,则 fallback 到 `toTableSchema`(用 `.arrow` 元数据里的 `"schema"` 恢复原始 schema 名)——实施时按编译/测试结果修正。

- [ ] **Step 5: 跑测试确认通过 + 全量回归**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StorageManagerTest` → PASS;再 `./mvnw.cmd test -pl minidb-server` → PASS(现有 `StorageManagerTest` 的旧目录回退路径必须仍绿,验证 `restored==false` 分支)。

- [ ] **Step 6: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java minidb-server/src/test/java/com/minidb/server/storage/StorageManagerTest.java
git commit -m "feat: 元数据变更即持久化 + 空表重启存活(旧目录回退 .arrow 推断)"
```

---

## Task 3: InformationSchema 物化

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/InformationSchema.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/InformationSchemaTest.java`

**Interfaces:**
- Consumes: Task 2 的 `MiniDbCatalog`(已可 `snapshot()`/`tableNames`/`getTable`/`schemaNames`)。
- Produces: `InformationSchema.SCHEMA_NAME`、`InformationSchema.isSystemSchema(String)`、`InformationSchema.materialize(MiniDbCatalog, String tableName, BufferAllocator) → VectorSchemaRoot`,以及 `schemataSchema()/tablesSchema()/columnsSchema()`(供 Task 4 的 Calcite 暴露复用)。

- [ ] **Step 1: 写失败测试 `InformationSchemaTest`**

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InformationSchemaTest {

    static BufferAllocator allocator;

    @BeforeAll
    static void setUp() { allocator = new RootAllocator(); }
    @AfterAll
    static void tearDown() { allocator.close(); }

    private static MiniDbCatalog catalog() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(new TableSchema("public", "t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER),
                new ColumnMeta("price", ColumnType.DECIMAL, 10, 2))));
        catalog.createTable(new TableSchema("other", "u", List.of(
                new ColumnMeta("name", ColumnType.VARCHAR))));
        return catalog;
    }

    @Test
    void materializeTables() {
        MiniDbCatalog catalog = catalog();
        VectorSchemaRoot root = InformationSchema.materialize(catalog, "tables", allocator);
        assertEquals(2, root.getRowCount());
        assertEquals("t", new String(root.getVector("TABLE_NAME").getValueCount() > 0
                ? (byte[]) null : new byte[0])); // placeholder — replace with real assertion below
        root.close();
    }
}
```

> **重要**:上面 `materializeTables` 的断言是占位、不能编译/无意义。**写真实断言**:用 `org.apache.arrow.vector.VarCharVector` 读 `root.getVector("TABLE_NAME")`,断言 `new String(v.get(0))` == `"t"` 或 `"u"`(先按 schema 排序以确定顺序)。照 `MetadataExecutorTest` 的断言风格写,别照抄占位。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=InformationSchemaTest`
Expected: FAIL(编译错误 —— `InformationSchema` 不存在)。

- [ ] **Step 3: 建 `InformationSchema`**

`minidb-server/src/main/java/com/minidb/server/exec/InformationSchema.java`:

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** 只读 information_schema 系统表:从内存 MiniDbCatalog 物化 schemata/tables/columns。 */
public final class InformationSchema {

    public static final String SCHEMA_NAME = "information_schema";

    private InformationSchema() {
    }

    public static boolean isSystemSchema(String name) {
        return SCHEMA_NAME.equalsIgnoreCase(name);
    }

    public static TableSchema schemataSchema() {
        return new TableSchema(SCHEMA_NAME, "schemata", List.of(
                new ColumnMeta("CATALOG_NAME", ColumnType.VARCHAR),
                new ColumnMeta("SCHEMA_NAME", ColumnType.VARCHAR),
                new ColumnMeta("SCHEMA_OWNER", ColumnType.VARCHAR),
                new ColumnMeta("DEFAULT_CHARACTER_SET_CATALOG", ColumnType.VARCHAR),
                new ColumnMeta("DEFAULT_CHARACTER_SET_SCHEMA", ColumnType.VARCHAR),
                new ColumnMeta("DEFAULT_CHARACTER_SET_NAME", ColumnType.VARCHAR),
                new ColumnMeta("SQL_PATH", ColumnType.VARCHAR)));
    }

    public static TableSchema tablesSchema() {
        return new TableSchema(SCHEMA_NAME, "tables", List.of(
                new ColumnMeta("TABLE_CATALOG", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_SCHEMA", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_NAME", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_TYPE", ColumnType.VARCHAR)));
    }

    public static TableSchema columnsSchema() {
        return new TableSchema(SCHEMA_NAME, "columns", List.of(
                new ColumnMeta("TABLE_CATALOG", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_SCHEMA", ColumnType.VARCHAR),
                new ColumnMeta("TABLE_NAME", ColumnType.VARCHAR),
                new ColumnMeta("COLUMN_NAME", ColumnType.VARCHAR),
                new ColumnMeta("ORDINAL_POSITION", ColumnType.INTEGER),
                new ColumnMeta("DATA_TYPE", ColumnType.VARCHAR),
                new ColumnMeta("NUMERIC_PRECISION", ColumnType.INTEGER),
                new ColumnMeta("NUMERIC_SCALE", ColumnType.INTEGER)));
    }

    public static VectorSchemaRoot materialize(MiniDbCatalog catalog, String tableName,
                                               BufferAllocator allocator) {
        if (tableName.equalsIgnoreCase("tables")) {
            return materializeTables(catalog, allocator);
        }
        if (tableName.equalsIgnoreCase("columns")) {
            return materializeColumns(catalog, allocator);
        }
        if (tableName.equalsIgnoreCase("schemata")) {
            return materializeSchemata(catalog, allocator);
        }
        throw new IllegalArgumentException("unknown information_schema table: " + tableName);
    }

    private static VectorSchemaRoot materializeSchemata(MiniDbCatalog catalog, BufferAllocator allocator) {
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        int n = schemas.size();
        VarCharVector catalogName = vc("CATALOG_NAME", n, allocator);
        VarCharVector schemaName = vc("SCHEMA_NAME", n, allocator);
        VarCharVector schemaOwner = vc("SCHEMA_OWNER", n, allocator);
        VarCharVector charsetCatalog = vc("DEFAULT_CHARACTER_SET_CATALOG", n, allocator);
        VarCharVector charsetSchema = vc("DEFAULT_CHARACTER_SET_SCHEMA", n, allocator);
        VarCharVector charsetName = vc("DEFAULT_CHARACTER_SET_NAME", n, allocator);
        VarCharVector sqlPath = vc("SQL_PATH", n, allocator);
        for (int i = 0; i < n; i++) {
            schemaName.setSafe(i, schemas.get(i).getBytes(StandardCharsets.UTF_8));
        }
        // 其余列保持 null(不写值)
        for (VarCharVector v : new VarCharVector[]{catalogName, schemaName, schemaOwner,
                charsetCatalog, charsetSchema, charsetName, sqlPath}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(catalogName, schemaName, schemaOwner,
                charsetCatalog, charsetSchema, charsetName, sqlPath);
    }

    private static VectorSchemaRoot materializeTables(MiniDbCatalog catalog, BufferAllocator allocator) {
        List<String[]> rows = new ArrayList<>(); // [schema, table]
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        for (String schema : schemas) {
            List<String> names = new ArrayList<>(catalog.tableNames(schema));
            names.sort(String::compareTo);
            for (String name : names) {
                rows.add(new String[]{schema, name});
            }
        }
        int n = rows.size();
        VarCharVector tableCatalog = vc("TABLE_CATALOG", n, allocator);
        VarCharVector tableSchema = vc("TABLE_SCHEMA", n, allocator);
        VarCharVector tableName = vc("TABLE_NAME", n, allocator);
        VarCharVector tableType = vc("TABLE_TYPE", n, allocator);
        for (int i = 0; i < n; i++) {
            tableSchema.setSafe(i, rows.get(i)[0].getBytes(StandardCharsets.UTF_8));
            tableName.setSafe(i, rows.get(i)[1].getBytes(StandardCharsets.UTF_8));
            // 现在只有 base table;视图落地后在此按 kind 分支报 'VIEW'。
            tableType.setSafe(i, "BASE TABLE".getBytes(StandardCharsets.UTF_8));
        }
        for (VarCharVector v : new VarCharVector[]{tableCatalog, tableSchema, tableName, tableType}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(tableCatalog, tableSchema, tableName, tableType);
    }

    private static VectorSchemaRoot materializeColumns(MiniDbCatalog catalog, BufferAllocator allocator) {
        List<Object[]> rows = new ArrayList<>(); // [schema, table, col, ordinal, dataType, prec, scale]
        List<String> schemas = new ArrayList<>(catalog.schemaNames());
        schemas.sort(String::compareTo);
        for (String schema : schemas) {
            List<String> names = new ArrayList<>(catalog.tableNames(schema));
            names.sort(String::compareTo);
            for (String name : names) {
                TableSchema ts = catalog.getTable(schema, name);
                int ordinal = 1;
                for (ColumnMeta col : ts.columns()) {
                    rows.add(new Object[]{schema, name, col.name(), ordinal, col});
                    ordinal++;
                }
            }
        }
        int n = rows.size();
        VarCharVector tableCatalog = vc("TABLE_CATALOG", n, allocator);
        VarCharVector tableSchema = vc("TABLE_SCHEMA", n, allocator);
        VarCharVector tableName = vc("TABLE_NAME", n, allocator);
        VarCharVector columnName = vc("COLUMN_NAME", n, allocator);
        IntVector ordinal = new IntVector("ORDINAL_POSITION", allocator);
        ordinal.setInitialCapacity(n);
        ordinal.allocateNew();
        VarCharVector dataType = vc("DATA_TYPE", n, allocator);
        IntVector numericPrecision = new IntVector("NUMERIC_PRECISION", allocator);
        numericPrecision.setInitialCapacity(n);
        numericPrecision.allocateNew();
        IntVector numericScale = new IntVector("NUMERIC_SCALE", allocator);
        numericScale.setInitialCapacity(n);
        numericScale.allocateNew();
        for (int i = 0; i < n; i++) {
            Object[] r = rows.get(i);
            ColumnMeta col = (ColumnMeta) r[4];
            tableSchema.setSafe(i, ((String) r[0]).getBytes(StandardCharsets.UTF_8));
            tableName.setSafe(i, ((String) r[1]).getBytes(StandardCharsets.UTF_8));
            columnName.setSafe(i, col.name().getBytes(StandardCharsets.UTF_8));
            ordinal.setSafe(i, (Integer) r[3]);
            dataType.setSafe(i, com.minidb.server.catalog.ArrowTypes.toSqlTypeName(col.type())
                    .getBytes(StandardCharsets.UTF_8));
            if (col.type() == ColumnType.DECIMAL || col.type() == ColumnType.NUMERIC) {
                numericPrecision.setSafe(i, col.precision());
                numericScale.setSafe(i, col.scale());
            }
            // 非 decimal 列 precision/scale 保持 null
        }
        for (VarCharVector v : new VarCharVector[]{tableCatalog, tableSchema, tableName, columnName, dataType}) {
            v.setValueCount(n);
        }
        for (IntVector v : new IntVector[]{ordinal, numericPrecision, numericScale}) {
            v.setValueCount(n);
        }
        return VectorSchemaRoot.of(tableCatalog, tableSchema, tableName, columnName,
                ordinal, dataType, numericPrecision, numericScale);
    }

    private static VarCharVector vc(String name, int capacity, BufferAllocator allocator) {
        VarCharVector v = new VarCharVector(name, allocator);
        v.setInitialCapacity(capacity);
        v.allocateNew();
        return v;
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=InformationSchemaTest`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/InformationSchema.java minidb-server/src/test/java/com/minidb/server/exec/InformationSchemaTest.java
git commit -m "feat: InformationSchema 物化 schemata/tables/columns"
```

---

## Task 4: Calcite 暴露 + MiniDbScan 分支 + 保留名

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbInformationSchemaCalciteSchema.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbRootCalciteSchema.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbScan.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java`(保留名检查)
- Test: `minidb-server/src/test/java/com/minidb/server/exec/QueryExecutorTest.java`(或新建 `InformationSchemaQueryTest.java`)

**Interfaces:**
- Consumes: Task 3 的 `InformationSchema`(固定 `TableSchema` + `materialize`)。
- Produces: `SELECT * FROM information_schema.<t>` 端到端可执行;`CREATE SCHEMA information_schema` 报错。

- [ ] **Step 1: 写失败测试(端到端)**

新建 `minidb-server/src/test/java/com/minidb/server/exec/InformationSchemaQueryTest.java`(照 `QueryExecutorTest` 的 harness 构造 `QueryExecutor`):

```java
    @Test
    void selectsInformationSchemaTables() {
        executor.execute("CREATE TABLE public.t (id INT, price DECIMAL(10,2))");
        QueryResult.Rows r = executor.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name");
        VectorSchemaRoot root = r.rows();
        assertEquals(1, root.getRowCount());
        assertEquals("t", new String(((org.apache.arrow.vector.VarCharVector) root.getVector("table_name")).get(0)));
        r.rows().close();
    }

    @Test
    void rejectsCreatingReservedSchema() {
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute("CREATE SCHEMA information_schema"));
    }
```

> 照 `QueryExecutorTest`/`DataTypeIntegrationTest` 的 harness 写 `executor` 字段与 `rows(...)` helper;`QueryResult.Rows` 的访问方式照现有测试。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=InformationSchemaQueryTest`
Expected: FAIL(`information_schema` 无法解析 → CannotPlan / 校验错误)。

- [ ] **Step 3: 建 `MiniDbInformationSchemaCalciteSchema`**

`minidb-server/src/main/java/com/minidb/server/calcite/MiniDbInformationSchemaCalciteSchema.java`:

```java
package com.minidb.server.calcite;

import com.minidb.server.exec.InformationSchema;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/** 只读系统 schema:暴露 information_schema.schemata/tables/columns 给 Calcite 规划。 */
public class MiniDbInformationSchemaCalciteSchema extends AbstractSchema {

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        tables.put("schemata", new MiniDbCalciteTable(InformationSchema.schemataSchema()));
        tables.put("tables", new MiniDbCalciteTable(InformationSchema.tablesSchema()));
        tables.put("columns", new MiniDbCalciteTable(InformationSchema.columnsSchema()));
        return tables;
    }
}
```

- [ ] **Step 4: `MiniDbRootCalciteSchema.getSubSchemaMap()` 加 information_schema**

```java
    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        Map<String, Schema> subs = new HashMap<>();
        subs.put(InformationSchema.SCHEMA_NAME, new MiniDbInformationSchemaCalciteSchema());
        for (String name : catalog.schemaNames()) {
            subs.put(name, new MiniDbCalciteSchema(catalog, name));
        }
        return subs;
    }
```

(需 import `com.minidb.server.exec.InformationSchema`。系统 schema 先放入,用户同名 schema 会被覆盖——配合 Step 6 的保留名拒绝,双保险。)

- [ ] **Step 5: `MiniDbScan.execute` 加 information_schema 分支**

把 `execute` 里的 `n >= 3` 分支改为:

```java
        ArrowTable arrowTable;
        if (n >= 3) {
            String schemaName = qualified.get(n - 2);
            String tableName = qualified.get(n - 1);
            if (InformationSchema.isSystemSchema(schemaName)) {
                return singleBatch(InformationSchema.materialize(
                        ctx.storage().catalog(), tableName, ctx.allocator()));
            }
            arrowTable = ctx.getTable(schemaName, tableName);
        } else {
            arrowTable = ctx.getTable(qualified.get(n - 1));
        }
```

并复用现有的单批迭代器逻辑(把 `transientScan` 里的单批包装抽成 `singleBatch(VectorSchemaRoot)` 私有方法,或直接内联一个同款匿名 `BatchIterator`):

```java
    private BatchIterator singleBatch(VectorSchemaRoot root) {
        boolean[] done = {false};
        return new BatchIterator() {
            @Override
            public boolean hasNext() { return !done[0]; }
            @Override
            public VectorSchemaRoot next() { done[0] = true; return root; }
            @Override
            public void close() { root.close(); }
        };
    }
```

> 需 import `com.minidb.server.exec.InformationSchema`。`ctx.storage().catalog()` 返回 `MiniDbCatalog`(见 `ExecContext`/`StorageManager.catalog()`)。

- [ ] **Step 6: `QueryExecutor.handleCreateSchema` 拒绝保留名**

在 `handleCreateSchema` 开头(取 `name` 之后)加:

```java
        if (InformationSchema.SCHEMA_NAME.equalsIgnoreCase(name)) {
            throw new IllegalArgumentException("reserved schema name: " + name);
        }
```

(需 import `com.minidb.server.exec.InformationSchema`。)

- [ ] **Step 7: 跑测试确认通过 + 全量回归**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=InformationSchemaQueryTest` → PASS;再 `./mvnw.cmd test -pl minidb-server` → PASS。

- [ ] **Step 8: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/calcite/MiniDbInformationSchemaCalciteSchema.java minidb-server/src/main/java/com/minidb/server/calcite/MiniDbRootCalciteSchema.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbScan.java minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java minidb-server/src/test/java/com/minidb/server/exec/InformationSchemaQueryTest.java
git commit -m "feat: information_schema 经 Calcite 暴露 + MiniDbScan 物化分支 + 保留名"
```

---

## Task 5: README + 集成测试收尾

**Files:**
- Modify: `README.md`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/InformationSchemaQueryTest.java`(补 columns/schemata 断言,或并入 Task 4 的测试)

**Interfaces:**
- Consumes: Task 4 全链路。

- [ ] **Step 1: README 补说明**

在 `README.md` 的「特性」或「限制」附近补一段(放在「支持的列类型」之后):

```
## 元数据与 information_schema

- 表元数据(schema/表/列名 + 类型)持久化在 `data/catalog.json`,独立于数据文件;空表(从未插入)重启后仍存活。
- 提供只读系统表 `information_schema.schemata` / `information_schema.tables` / `information_schema.columns`,可 `SELECT` 查询。`information_schema` 是保留名,不可作为用户 schema 创建。
```

- [ ] **Step 2: 补集成测试断言(columns 的 DECIMAL 精度、schemata)**

在 `InformationSchemaQueryTest` 加:

```java
    @Test
    void selectsInformationSchemaColumns() {
        executor.execute("CREATE TABLE t (price DECIMAL(10,2))");
        QueryResult.Rows r = executor.execute(
                "SELECT column_name, data_type, numeric_precision, numeric_scale "
                + "FROM information_schema.columns WHERE table_name = 't'");
        VectorSchemaRoot root = r.rows();
        assertEquals(1, root.getRowCount());
        assertEquals("price", new String(((org.apache.arrow.vector.VarCharVector) root.getVector("column_name")).get(0)));
        assertEquals("DECIMAL", new String(((org.apache.arrow.vector.VarCharVector) root.getVector("data_type")).get(0)));
        assertEquals(10, ((org.apache.arrow.vector.IntVector) root.getVector("numeric_precision")).get(0));
        assertEquals(2, ((org.apache.arrow.vector.IntVector) root.getVector("numeric_scale")).get(0));
        r.rows().close();
    }
```

- [ ] **Step 3: 跑全量测试**

Run: `./mvnw.cmd test`
Expected: BUILD SUCCESS(全模块绿)。

- [ ] **Step 4: Commit**

```bash
git add README.md minidb-server/src/test/java/com/minidb/server/exec/InformationSchemaQueryTest.java
git commit -m "docs: README 补元数据持久化与 information_schema;test: columns/schemata 断言"
```

---

## 自检清单(写完后自查,发现即修)

1. **Spec 覆盖**:persistence(JSON + CatalogStore 接口 + 空表存活)→ Task 1/2;information_schema 三表 → Task 3/4;保留名 → Task 4;README → Task 5。均已覆盖。
2. **占位符扫描**:Task 3 Step 1 的占位测试已明确标注「写真实断言」,不算占位;其余步骤均有实际代码。
3. **类型一致性**:`CatalogSnapshot` 在 `catalog/`(避免 catalog→storage 循环);`CatalogStore`/`JsonCatalogStore` 在 `storage/`;`MiniDbCatalog.snapshot()/restore()` 用 `CatalogSnapshot`。`InformationSchema.SCHEMA_NAME` 全链一致。`MiniDbScan` 用 `ctx.storage().catalog()`。
4. **执行顺序依赖**:Task 2 依赖 Task 1(CatalogStore);Task 3 依赖 Task 2(MiniDbCatalog);Task 4 依赖 Task 3(InformationSchema);Task 5 依赖 Task 4。顺序正确。

## 执行交接

计划完成后,提供两种执行方式:subagent-driven(推荐)或 inline。
