# Default Schema + CREATE/DROP SCHEMA + 限定名解析 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 引入 schema 概念:所有表默认属于 `public` schema;支持 `CREATE SCHEMA` / `DROP SCHEMA` DDL、`schema.table` 限定名解析、`USE SCHEMA` 切换当前 schema(每连接隔离);JDBC `ResultSetMetaData.getSchemaName()` 返回正确 schema 名。零协议改动、零物理算子改动。

**Architecture:** `MiniDbCatalog` 内部存储改为 `schemaName → tableName → TableSchema` 两级 map,构造时自动创建 `public`。`TableSchema` 增加 `schemaName` 字段。Calcite schema 树改为 root → `minidb`(容器)→ 各 schema 子节点(每个 `MiniDbCalciteSchema` 实例只暴露一个 schema 的表);`CalciteContext` 的 catalog reader 搜索路径变为 `[minidb, currentSchema]`,使 unqualified 名解析到当前 schema、`schema.table` 解析到指定 schema。`USE SCHEMA` 在 `QueryExecutor.execute` 顶部字符串前缀拦截(同 EXPLAIN/ANALYZE 模式),返回一个特殊结果让 `SessionHandler` 更新自己的 per-channel `currentSchema` 字段——避免共享 `QueryExecutor` 单例时的跨连接污染。`currentSchema` 作为参数流经 `QueryExecutor.execute` → `Planner.plan` → `CalciteContext.planInCluster`,不落在任何共享成员字段上。存储持久化改为子目录 `data/<schema>/<table>.arrow`,跨 schema 同名表不再冲突。`ArrowTable` 构造 Arrow `Schema` 时附加 `{"schema" → schemaName}` metadata,经 Arrow IPC 流到客户端,`MiniDbResultSetMetaData.getSchemaName()` 从 `root.getSchema().getCustomMetadata()` 读取。

**Tech Stack:** Java 17, Apache Calcite 1.42.0(parser `SqlDdlParserImpl`,`SqlCreateSchema`/`SqlDropSchema` 在 `org.apache.calcite.sql.ddl`), Apache Arrow(列式 + IPC,Schema customMetadata 跨网络保留), Netty, JUnit 5(`@TempDir` + `RootAllocator`)。

## Global Constraints

- JDK 17 required; `JAVA_HOME` 指向 JDK 17。
- 构建/测试:bash 下直接 `./mvnw.cmd test`(不是 `mvnw.cmd`、不是 `cmd //c`、不是 `mvn`)。单模块:`./mvnw.cmd test -pl minidb-server`。单类:`./mvnw.cmd test -pl minidb-server -Dtest=QueryExecutorTest`。编译:`./mvnw.cmd -pl minidb-server -am compile -q`。
- 服务端运行需 JVM opens:`--add-opens=java.base/java.nio=ALL-UNNAMED`、`--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED`、`--add-opens=java.base/sun.nio.ch=ALL-UNNAMED`。
- 无新外部依赖。
- `minidb-protocol` 模块**不动**(零 wire 改动)。
- 现有 6 个物理算子文件(`MiniDbScan`/`MiniDbFilter`/`MiniDbProject`/`MiniDbSort`/`MiniDbValues`/`MiniDbModify`)**不动**。
- 客户端(`minidb-jdbc`)不引用任何服务端类;除 `MiniDbResultSetMetaData.getSchemaName()` 外,客户端改动仅限测试。
- 默认 schema 名恒为 `"public"`(大小写不敏感,内部小写存储)。
- schema 名、表名一律大小写不敏感(内部 `toLowerCase(Locale.ROOT)` 作 key)。
- 列类型限定:INTEGER/BIGINT/DOUBLE/VARCHAR/BOOLEAN/DATE/TIMESTAMP(现有 `ArrowTypes`)。
- 测试断言关系/比例/相等性,选择率单元测试除外(1e-9 delta)。
- 提交:一个任务一个 commit,conventional commit 风格(`feat:`/`fix:`/`test:`/`refactor:`/`docs:`),不要 amend,不要 `--no-verify`。
- 在 `master` 分支工作,小步提交。

## 关键技术决策(偏离 spec 字面之处)

spec 字面把 `currentSchema` 放在 `CalciteContext` 成员字段上。但 `QueryExecutor` 是所有连接共享的单例(`MiniDbServer` 构造一次,每个 `SessionHandler` 都引用同一个),`CalciteContext` 是 `QueryExecutor` 的 final 成员。若 `currentSchema` 是 `CalciteContext` 的可变字段,一个客户端 `USE SCHEMA other` 会影响所有并发客户端。同理 `Planner` 内部 `new CalciteContext(catalog)` 也是共享的。

**修正:** `currentSchema` 不落在任何共享对象的成员字段上,而是作为方法参数流经调用链:
- `QueryExecutor.execute(sql, currentSchema)` — 每次调用带上调用方的 currentSchema。
- `Planner.plan(sql, currentSchema)` — 透传。
- `CalciteContext.planInCluster(sql, cluster, currentSchema)` — 用它构造 catalog reader 搜索路径 `[SCHEMA_NAME, currentSchema]`。
- `CalciteContext` 仍保留无参 `plan(sql)` 便捷重载(默认 `"public"`),供旧测试和 EXPLAIN 路径用。
- `SessionHandler` 持有 per-channel `private String currentSchema = "public";` 字段,`USE SCHEMA` 返回一个带新 schema 名的 `QueryResult.UseSchema`(新增 sealed 分支),`SessionHandler` 据此更新自己的字段。

存储持久化:spec 未指定跨 schema 同名表的文件名策略。现有 `fileName(name)=name.toLowerCase()+".arrow"` 会让 `public.users` 和 `other.users` 互相覆盖。**采用子目录方案:** `data/<schema>/<table>.arrow` 和 `data/<schema>/<table>.stats`。`loadAll` 遍历两级目录。不兼容旧扁平 `data/<table>.arrow`(纯本地仓库、新功能,文档说明即可)。

---

## File Structure

### 新建文件(main,均 under `minidb-server/src/main/java/com/minidb/server/`)

- `calcite/MiniDbRootCalciteSchema.java` — 根容器 schema,挂在 root 的 `"minidb"` 名下;`getSubSchemaMap()` 返回 catalog 中所有 schema(每个一个 `MiniDbCalciteSchema` 子节点)。承载 schema 树的"容器"层。
- `calcite/MiniDbCalciteSchema.java` — **改造**(见下),变为"单 schema 实例":构造接 `(MiniDbCatalog catalog, String schemaName)`,`getTableMap()` 只返回该 schema 下的表。

### 新建文件(test,均 under `minidb-server/src/test/java/com/minidb/server/`)

- `catalog/MiniDbCatalogSchemaTest.java` — schema 创建/删除/默认 public/跨 schema 同名表/大小写不敏感。
- `exec/SchemaDdlTest.java` — `QueryExecutor` 处理 `CREATE SCHEMA`/`DROP SCHEMA`/`USE SCHEMA`/限定名建表查询(纯服务端,不经网络)。
- `storage/SchemaStorageTest.java` — 跨 schema 同名表持久化到子目录、reload 后隔离完好。

### 改造文件(main)

- `catalog/TableSchema.java` — record 增加 `schemaName` 字段(首位):`record TableSchema(String schemaName, String name, List<ColumnMeta> columns)`。新增便捷工厂 `public TableSchema(String name, List<ColumnMeta> columns)` 委托到 `new TableSchema("public", name, columns)`,避免改所有现有调用点(但调用点仍要逐步迁移到显式 schema,见各任务)。
- `catalog/MiniDbCatalog.java` — 内部存储改两级 map;新增 `createSchema`/`dropSchema`/`schemaNames`/`tablesIn(schemaName)`;现有 `getTable`/`hasTable`/`tableNames`/`createTable`/`dropTable` 委托到 `"public"`(向后兼容)同时新增带 schema 参数重载。
- `calcite/MiniDbCalciteSchema.java` — 见上,改单 schema 实例。
- `calcite/CalciteContext.java` — `planInCluster` 增加 `currentSchema` 参数;`buildCatalogReader` 搜索路径 `[SCHEMA_NAME, currentSchema]`;新增 `setCurrentSchema` **不**加(刻意:避免共享状态);保留 `plan(sql)` 默认 public 重载。
- `plan/Planner.java` — `plan(sql)` 增加 `plan(sql, currentSchema)` 重载,透传给 `CalciteContext.planInCluster`。
- `storage/StorageManager.java` — `createTable`/`dropTable`/`getTable`/`markDirty`/`truncateTable`/`flushTable` 全部 schema 感知(key 改为 `schema.table`);文件名改子目录 `data/<schema>/<table>.arrow`;`loadAll` 两级遍历;新增 `dropSchema(name)` 级联删表;`toTableSchema` 从 Arrow schema metadata 恢复 schemaName。
- `storage/ArrowTable.java` — 构造 Arrow `Schema` 时附 `Map.of("schema", schema.schemaName())` metadata。
- `exec/QueryExecutor.java` — `execute(sql, currentSchema)` 主路径;`USE SCHEMA` 前缀拦截返回 `QueryResult.UseSchema`;`SqlCreateSchema`/`SqlDropSchema` 分发;`handleCreate`/`handleDrop`/`handleTruncate` 用 `SqlIdentifier.names` 分解 schema+table。
- `exec/QueryResult.java` — sealed 接口新增 `UseSchema(String schemaName)` 分支。
- `netty/SessionHandler.java` — 持有 per-channel `currentSchema` 字段;`handleExecute` 调 `executor.execute(sql, currentSchema)`;收到 `QueryResult.UseSchema` 时更新字段并回 `UpdateCount(0)`。
- `exec/ExplainExecutor.java` — `explain`/`analyze` 路径透传 currentSchema(见 Task 9,轻量改动:构造时接 currentSchema 或方法参数)。**需先读该文件确认其与 Planner/CalciteContext 的耦合点。**

### 改造文件(test,迁移构造器调用)

- `catalog/MiniDbCatalogTest.java`、`calcite/CalciteContextTest.java`、`plan/PlannerTest.java`、`storage/StorageManagerTest.java`、`storage/ArrowTableTest.java`、`stats/StatsManagerTest.java` — 把 `new TableSchema(name, cols)` 调用迁移到显式 `new TableSchema("public", name, cols)` 或保留无参工厂(取决于是否要测 schema 字段)。

### 改造文件(client)

- `minidb-jdbc/.../MiniDbResultSetMetaData.java` — `getSchemaName(column)` 从 `root.getSchema().getCustomMetadata()` 读 `"schema"` 键,缺省 `""`。

### 新增客户端测试

- `minidb-jdbc/.../SchemaMetadataTest.java` — 端到端:建表→查询→`ResultSetMetaData.getSchemaName()` 返回 `"public"`。

---

## Task Dependency Order

- **Task 1**(TableSchema 加 schemaName + 便捷工厂):基础数据类型,所有后续任务依赖。向后兼容(工厂委托 public),所以现有测试不改也能编译通过。
- **Task 2**(MiniDbCatalog 两级 map):依赖 Task 1。仍向后兼容(public 委托)。
- **Task 3**(ArrowTable 附 metadata):依赖 Task 1。
- **Task 4**(StorageManager 子目录 + schema 感知 + dropSchema):依赖 Task 1/3。这是第一个破坏向后兼容的任务(文件名格式变)。
- **Task 5**(MiniDbCalciteSchema 单 schema 实例 + RootCalciteSchema):依赖 Task 2。
- **Task 6**(CalciteContext currentSchema 参数 + 搜索路径 + Planner 透传):依赖 Task 5。
- **Task 7**(QueryResult.UseSchema + QueryExecutor DDL + USE SCHEMA + 限定名):依赖 Task 1/2/4/6。核心集成。
- **Task 8**(SessionHandler per-channel currentSchema):依赖 Task 7。
- **Task 9**(ExplainExecutor 透传 currentSchema):依赖 Task 6/7。
- **Task 10**(MiniDbResultSetMetaData.getSchemaName + 端到端测试):依赖 Task 3/7。
- **Task 11**(迁移现有测试的 TableSchema 构造器 + 全量回归):收尾。

每个任务结束都编译 + 跑相关测试 + commit。

---

### Task 1: TableSchema 增加 schemaName 字段

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/TableSchema.java`
- Test: `minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogTest.java`(已有,本任务只加一个测试方法)

**Interfaces:**
- Produces: `record TableSchema(String schemaName, String name, List<ColumnMeta> columns)`,以及便捷工厂 `TableSchema(String name, List<ColumnMeta> columns)`(委托 `new TableSchema("public", name, columns)`)。访问器:`schemaName()`、`name()`、`columns()`、`column(String)`、`columnIndex(String)`(后两者不变)。

- [ ] **Step 1: 写失败测试**

在 `MiniDbCatalogTest.java` 末尾(`columnLookupByName` 之后)加:

```java
@Test
void schemaNameDefaultsToPublicViaConvenienceFactory() {
    TableSchema schema = new TableSchema("t1", List.of(
            new ColumnMeta("id", ColumnType.INTEGER)));
    assertEquals("public", schema.schemaName());
    assertEquals("t1", schema.name());
}

@Test
void explicitSchemaNameStored() {
    TableSchema schema = new TableSchema("other", "t1", List.of(
            new ColumnMeta("id", ColumnType.INTEGER)));
    assertEquals("other", schema.schemaName());
    assertEquals("t1", schema.name());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbCatalogTest`
Expected: 编译失败(`TableSchema` 无 3 参构造器、无 `schemaName()` 访问器)。

- [ ] **Step 3: 最小实现**

把 `TableSchema.java` 改为:

```java
package com.minidb.server.catalog;

import java.util.List;

public record TableSchema(String schemaName, String name, List<ColumnMeta> columns) {

    public TableSchema(String name, List<ColumnMeta> columns) {
        this("public", name, columns);
    }

    public ColumnMeta column(String name) {
        for (ColumnMeta c : columns) {
            if (c.name().equalsIgnoreCase(name)) {
                return c;
            }
        }
        throw new IllegalArgumentException(
                "no column " + name + " in table " + this.name);
    }

    public int columnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "no column " + name + " in table " + this.name);
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbCatalogTest`
Expected: PASS(全部,含原有用例——无参工厂委托 public,旧测试不受影响)。

- [ ] **Step 5: 全量编译确认无回归**

Run: `./mvnw.cmd -pl minidb-server -am compile -q`
Expected: 成功(所有 `new TableSchema(name, cols)` 调用走新的 2 参工厂)。

- [ ] **Step 6: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/TableSchema.java \
        minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogTest.java
git commit -m "feat: add schemaName field to TableSchema with public default factory"
```

---

### Task 2: MiniDbCatalog 两级 schema 存储

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java`
- Test: `minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogSchemaTest.java`(新建)

**Interfaces:**
- Consumes: `TableSchema.schemaName()`(Task 1)。
- Produces:
  - `void createSchema(String name)` — 创建空 schema,已存在抛 `IllegalArgumentException`。
  - `void dropSchema(String name)` — 删除 schema,级联删其下所有表;不存在抛;`public` 不可删(抛 `IllegalArgumentException`)。
  - `List<String> schemaNames()` — 含 `"public"`,小写,无序保证。
  - `void createTable(TableSchema schema)` — 按 `schema.schemaName()` 存入对应 schema;schema 不存在则抛;同 schema 同名表抛。
  - `void dropTable(String schemaName, String tableName)` — schema 感知删除。
  - `TableSchema getTable(String schemaName, String tableName)` — schema 感知取;不存在抛。
  - `boolean hasTable(String schemaName, String tableName)`。
  - `List<String> tableNames(String schemaName)` — 该 schema 下所有表名。
  - 向后兼容重载(委托 `"public"`):`dropTable(String)`、`getTable(String)`、`hasTable(String)`、`tableNames()`、`createTable(TableSchema)`(已上)。

- [ ] **Step 1: 写失败测试**

创建 `MiniDbCatalogSchemaTest.java`:

```java
package com.minidb.server.catalog;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MiniDbCatalogSchemaTest {

    private TableSchema table(String schema, String name) {
        return new TableSchema(schema, name, List.of(
                new ColumnMeta("id", ColumnType.INTEGER)));
    }

    @Test
    void publicSchemaExistsByDefault() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertTrue(catalog.schemaNames().contains("public"));
    }

    @Test
    void createSchemaAppearsInList() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createDuplicateSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        assertThrows(IllegalArgumentException.class,
                () -> catalog.createSchema("other"));
    }

    @Test
    void schemaNamesCaseInsensitive() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("Other");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createTableInNamedSchema() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(table("other", "t"));
        assertTrue(catalog.hasTable("other", "t"));
        assertEquals("other", catalog.getTable("other", "t").schemaName());
        assertEquals(List.of("t"), catalog.tableNames("other"));
    }

    @Test
    void sameTableNameInDifferentSchemasCoexist() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(table("public", "users"));
        catalog.createTable(table("other", "users"));
        assertTrue(catalog.hasTable("public", "users"));
        assertTrue(catalog.hasTable("other", "users"));
    }

    @Test
    void createTableInMissingSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class,
                () -> catalog.createTable(table("ghost", "t")));
    }

    @Test
    void dropSchemaCascadesTables() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        catalog.createTable(table("other", "t1"));
        catalog.createTable(table("other", "t2"));
        catalog.dropSchema("other");
        assertFalse(catalog.schemaNames().contains("other"));
        assertFalse(catalog.hasTable("other", "t1"));
    }

    @Test
    void dropPublicSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class, () -> catalog.dropSchema("public"));
    }

    @Test
    void dropMissingSchemaThrows() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        assertThrows(IllegalArgumentException.class, () -> catalog.dropSchema("ghost"));
    }

    @Test
    void legacyPublicDelegatesStillWork() {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createTable(new TableSchema("t", List.of(
                new ColumnMeta("id", ColumnType.INTEGER))));
        assertTrue(catalog.hasTable("t"));
        assertEquals("public", catalog.getTable("t").schemaName());
        assertEquals(1, catalog.tableNames().size());
        catalog.dropTable("t");
        assertFalse(catalog.hasTable("t"));
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbCatalogSchemaTest`
Expected: 编译失败(新方法不存在)。

- [ ] **Step 3: 最小实现**

把 `MiniDbCatalog.java` 改为:

```java
package com.minidb.server.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MiniDbCatalog {

    public static final String DEFAULT_SCHEMA = "public";

    private final Map<String, Map<String, TableSchema>> schemas = new ConcurrentHashMap<>();

    public MiniDbCatalog() {
        schemas.put(DEFAULT_SCHEMA, new ConcurrentHashMap<>());
    }

    public void createSchema(String name) {
        String k = key(name);
        if (schemas.putIfAbsent(k, new ConcurrentHashMap<>()) != null) {
            throw new IllegalArgumentException("schema already exists: " + name);
        }
        notifyChange();
    }

    public void dropSchema(String name) {
        String k = key(name);
        if (k.equals(DEFAULT_SCHEMA)) {
            throw new IllegalArgumentException("cannot drop default schema: " + name);
        }
        if (schemas.remove(k) == null) {
            throw new IllegalArgumentException("schema not found: " + name);
        }
        notifyChange();
    }

    public List<String> schemaNames() {
        return new ArrayList<>(schemas.keySet());
    }

    public void createTable(TableSchema schema) {
        String sk = key(schema.schemaName());
        Map<String, TableSchema> tables = schemas.get(sk);
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schema.schemaName());
        }
        String tk = key(schema.name());
        if (tables.putIfAbsent(tk, schema) != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        notifyChange();
    }

    public void dropTable(String schemaName, String tableName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        if (tables.remove(key(tableName)) == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        notifyChange();
    }

    public TableSchema getTable(String schemaName, String tableName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        TableSchema schema = tables.get(key(tableName));
        if (schema == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        return schema;
    }

    public boolean hasTable(String schemaName, String tableName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        return tables != null && tables.containsKey(key(tableName));
    }

    public List<String> tableNames(String schemaName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        List<String> names = new ArrayList<>();
        for (TableSchema schema : tables.values()) {
            names.add(schema.name());
        }
        return names;
    }

    // ---- 向后兼容重载:委托到 public schema ----

    public void dropTable(String name) {
        dropTable(DEFAULT_SCHEMA, name);
    }

    public TableSchema getTable(String name) {
        return getTable(DEFAULT_SCHEMA, name);
    }

    public boolean hasTable(String name) {
        return hasTable(DEFAULT_SCHEMA, name);
    }

    public List<String> tableNames() {
        return tableNames(DEFAULT_SCHEMA);
    }

    public void addListener(Runnable listener) {
        listeners.add(listener);
    }

    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    private void notifyChange() {
        for (Runnable listener : listeners) {
            listener.run();
        }
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbCatalogSchemaTest,MiniDbCatalogTest`
Expected: PASS(新测试 + 旧测试,旧测试走 public 委托)。

- [ ] **Step 5: 全量编译**

Run: `./mvnw.cmd -pl minidb-server -am compile -q`
Expected: 成功。

- [ ] **Step 6: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java \
        minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogSchemaTest.java
git commit -m "feat: schema-aware two-level storage in MiniDbCatalog with cascade drop"
```

---

### Task 3: ArrowTable 附加 schema metadata

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/storage/ArrowTable.java`
- Test: `minidb-server/src/test/java/com/minidb/server/storage/ArrowTableTest.java`(加测试方法)

**Interfaces:**
- Consumes: `TableSchema.schemaName()`(Task 1)。
- Produces: `ArrowTable.arrowSchema()` 返回的 `org.apache.arrow.vector.types.pojo.Schema` 其 `getCustomMetadata()` 含 `{"schema" → schemaName}`。下游 `StorageManager`(Task 4)和 `MiniDbResultSetMetaData`(Task 10)依赖此约定。

- [ ] **Step 1: 写失败测试**

在 `ArrowTableTest.java` 的 `closeReleasesMemory` 之后加:

```java
@Test
void arrowSchemaCarriesSchemaMetadata() {
    ArrowTable t = new ArrowTable(new TableSchema("other", "t", List.of(
            new ColumnMeta("id", ColumnType.INTEGER))), allocator);
    try {
        java.util.Map<String, String> meta = t.arrowSchema().getCustomMetadata();
        assertNotNull(meta);
        assertEquals("other", meta.get("schema"));
    } finally {
        t.close();
    }
}

@Test
void arrowSchemaMetadataDefaultsToPublic() {
    // table field setUp 用了无参 TableSchema(name, cols) → public
    java.util.Map<String, String> meta = table.arrowSchema().getCustomMetadata();
    assertNotNull(meta);
    assertEquals("public", meta.get("schema"));
}
```

(需在 `ArrowTableTest` 顶部 import `org.apache.calcite...` 否——不需要;只需 `MiniDbCatalog`?不,只需 `TableSchema`/`ColumnMeta`/`ColumnType`,已 import。`assertNotNull` 需 import:`import static org.junit.jupiter.api.Assertions.assertNotNull;` 和 `assertEquals` 已有。)

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=ArrowTableTest`
Expected: 两个新测试失败(`meta` 为 null 或不含 `"schema"`)。

- [ ] **Step 3: 最小实现**

在 `ArrowTable.java` 构造器里改 `arrowSchema` 构造:

```java
package com.minidb.server.storage;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowTable implements AutoCloseable {

    public static final int MAX_BATCH_ROWS = 4096;

    private final TableSchema schema;
    private final BufferAllocator allocator;
    private final Schema arrowSchema;
    private final List<VectorSchemaRoot> batches = new CopyOnWriteArrayList<>();

    public ArrowTable(TableSchema schema, BufferAllocator allocator) {
        this.schema = schema;
        this.allocator = allocator;
        List<Field> fields = new ArrayList<>();
        for (ColumnMeta column : schema.columns()) {
            fields.add(ArrowTypes.field(column));
        }
        this.arrowSchema = new Schema(fields,
                Map.of("schema", schema.schemaName()));
    }

    // 其余方法不变(schema()/arrowSchema()/newBatchRoot()/appendBatch/
    // replaceBatches/batches()/rowCount()/clear()/close())
}
```

(只改构造器中 `this.arrowSchema = ...` 这一行 + import `Map`;其余方法保持原样。)

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=ArrowTableTest`
Expected: PASS(含新测试)。

- [ ] **Step 5: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/storage/ArrowTable.java \
        minidb-server/src/test/java/com/minidb/server/storage/ArrowTableTest.java
git commit -m "feat: attach schema name as Arrow Schema metadata in ArrowTable"
```

---

### Task 4: StorageManager 子目录持久化 + schema 感知 + dropSchema

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java`
- Test: `minidb-server/src/test/java/com/minidb/server/storage/SchemaStorageTest.java`(新建)

**Interfaces:**
- Consumes: `TableSchema.schemaName()`(Task 1)、`ArrowTable` metadata(Task 3)、`MiniDbCatalog.createSchema`/`dropSchema`/`getTable(schema,table)`/`hasTable(schema,table)`(Task 2)。
- Produces:
  - `ArrowTable createTable(TableSchema schema)` — schema 感知(已存在,但内部 key 和文件名变)。
  - `void dropTable(String schemaName, String tableName)` — schema 感知;旧 `dropTable(String)` 委托 public。
  - `ArrowTable getTable(String schemaName, String tableName)` — schema 感知;旧 `getTable(String)` 委托 public。
  - `void markDirty(String schemaName, String tableName)` — schema 感知;旧 `markDirty(String)` 委托 public。
  - `void truncateTable(String schemaName, String tableName)` — schema 感知;旧委托 public。
  - `void dropSchema(String name)` — 级联删除该 schema 下所有表(调 `ArrowTable.close()` + 删文件 + `catalog.dropSchema`);public 不可删(由 catalog 抛)。
  - 持久化路径:`data/<schema>/<table>.arrow`。`loadAll` 两级遍历。`toTableSchema` 从 Arrow schema metadata 恢复 schemaName(缺省 public)。

**关键实现注意:**
- `tables` map 的 key 改为 `schema.table`(小写)。
- `dirty` set 同理。
- `StatsManager.dropStats` 仍按 table 名——需把 stats 文件也放子目录。**但 StatsManager 是独立类,本任务不改它**;为避免破坏 stats 持久化,本任务让 `StorageManager` 暴露 `Path tableDir(String schemaName)` 供 StatsManager 后续适配(或 Task 11 统一处理)。**决策:本任务保持 StatsManager 调用签名不变(`dropStats(tableName)`/`markStale(tableName)`),但 stats 文件路径暂仍用扁平 `data/<table>.stats`——这会导致跨 schema 同名表 stats 冲突。为避免此缺陷,本任务同时把 `StorageManager.markDirty` 里调 `statsManager.markStale` 时传的 key 改为 `schema.table` 形式,`dropTable` 里 `dropStats` 同理;StatsManager 内部 key 仍是字符串,无需改其代码,只是 key 语义变为 `schema.table`。** stats 文件名仍是 `<key>.stats` = `schema.table.stats`,放 `data/` 下(不分子目录,因为 StatsManager 不感知 schema)。这能避免冲突且 StatsManager 零改动。
- `fileName(schema, table)` = `schema.toLowerCase() + "/" + table.toLowerCase() + ".arrow"`,`resolve` 时 `dataDir.resolve("public/users.arrow")` 会自动建中间目录?不——`Files.newByteChannel` 不会建父目录。`flushTable` 里已有 `Files.createDirectories(dataDir)`,需改为 `Files.createDirectories(file.getParent())`。
- `loadAll`:遍历 `dataDir` 下每个子目录(以及直接 `.arrow`?不——新格式只在子目录下),每个子目录名是 schema 名,遍历其中 `*.arrow`。**不兼容旧扁平格式**(文档说明)。

- [ ] **Step 1: 写失败测试**

创建 `SchemaStorageTest.java`:

```java
package com.minidb.server.storage;

import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaStorageTest {

    private TableSchema schema(String schema, String name) {
        return new TableSchema(schema, name, List.of(
                new ColumnMeta("id", ColumnType.INTEGER)));
    }

    private void insertRow(StorageManager storage, String schema, String table, int id) {
        ArrowTable t = storage.getTable(schema, table);
        VectorSchemaRoot root = t.newBatchRoot();
        root.allocateNew();
        ((IntVector) root.getVector(0)).setSafe(0, id);
        root.setRowCount(1);
        t.appendBatch(root);
    }

    @Test
    void sameTableNameInDifferentSchemasPersistSeparately(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("public", "users"));
            storage.createTable(schema("other", "users"));
            insertRow(storage, "public", "users", 1);
            insertRow(storage, "other", "users", 2);
            storage.markDirty("public", "users");
            storage.markDirty("other", "users");
            storage.close();
        }

        assertTrue(Files.exists(dir.resolve("public").resolve("users.arrow")));
        assertTrue(Files.exists(dir.resolve("other").resolve("users.arrow")));

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        catalog2.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, a, dir);
            storage2.loadAll();
            assertEquals(1, storage2.getTable("public", "users").rowCount());
            assertEquals(1, storage2.getTable("other", "users").rowCount());
            IntVector pv = (IntVector) storage2.getTable("public", "users")
                    .batches().get(0).getVector(0);
            IntVector ov = (IntVector) storage2.getTable("other", "users")
                    .batches().get(0).getVector(0);
            assertEquals(1, pv.get(0));
            assertEquals(2, ov.get(0));
            storage2.close();
        }
    }

    @Test
    void dropSchemaCascadeDeletesTableFiles(@TempDir Path dir) throws Exception {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("other", "t1"));
            storage.createTable(schema("other", "t2"));
            insertRow(storage, "other", "t1", 1);
            storage.markDirty("other", "t1");
            storage.flushDirty();
            assertTrue(Files.exists(dir.resolve("other").resolve("t1.arrow")));
            storage.dropSchema("other");
            assertFalse(Files.exists(dir.resolve("other").resolve("t1.arrow")));
            assertFalse(catalog.hasTable("other", "t1"));
            storage.close();
        }
    }

    @Test
    void loadAllRestoresSchemaNameFromMetadata(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        catalog.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            storage.createTable(schema("other", "t"));
            storage.close();
        }

        MiniDbCatalog catalog2 = new MiniDbCatalog();
        catalog2.createSchema("other");
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage2 = new StorageManager(catalog2, a, dir);
            storage2.loadAll();
            assertEquals("other",
                    storage2.getTable("other", "t").schema().schemaName());
            storage2.close();
        }
    }

    @Test
    void dropPublicSchemaThrows(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        try (BufferAllocator a = new RootAllocator()) {
            StorageManager storage = new StorageManager(catalog, a, dir);
            org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalArgumentException.class, () -> storage.dropSchema("public"));
            storage.close();
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SchemaStorageTest`
Expected: 编译失败(`getTable(schema,table)` 等新签名、`dropSchema` 不存在)。

- [ ] **Step 3: 最小实现**

把 `StorageManager.java` 改为(完整文件,因为多处改动):

```java
package com.minidb.server.storage;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.stats.StatsManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;
    private final Path dataDir;
    private final Map<String, ArrowTable> tables = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();
    private volatile StatsManager statsManager;

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
    }

    public void setStatsManager(StatsManager statsManager) {
        this.statsManager = statsManager;
    }

    public MiniDbCatalog catalog() {
        return catalog;
    }

    public void loadAll() {
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
                        loadFile(schemaName, file);
                        count++;
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.info("loaded {} table(s)", count);
    }

    private void loadFile(String schemaName, Path file) throws IOException {
        try (SeekableByteChannel channel =
                     Files.newByteChannel(file, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            String tableName = stripExtension(file.getFileName().toString());
            TableSchema schema = toTableSchema(root.getSchema(), schemaName, tableName);
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
            catalog.createTable(schema);
        }
    }

    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot < 0 ? fileName : fileName.substring(0, dot);
    }

    public ArrowTable getTable(String schemaName, String tableName) {
        ArrowTable table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        return table;
    }

    public ArrowTable getTable(String name) {
        return getTable(MiniDbCatalog.DEFAULT_SCHEMA, name);
    }

    public ArrowTable createTable(TableSchema schema) {
        ArrowTable table = new ArrowTable(schema, allocator);
        String sk = storageKey(schema.schemaName(), schema.name());
        if (tables.putIfAbsent(sk, table) != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        catalog.createTable(schema);
        return table;
    }

    public void dropTable(String schemaName, String tableName) {
        String sk = storageKey(schemaName, tableName);
        ArrowTable table = tables.remove(sk);
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        if (statsManager != null) {
            statsManager.dropStats(sk);
        }
        catalog.dropTable(schemaName, tableName);
        table.close();
        dirty.remove(sk);
        try {
            Files.deleteIfExists(dataDir.resolve(fileName(schemaName, tableName)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void dropTable(String name) {
        dropTable(MiniDbCatalog.DEFAULT_SCHEMA, name);
    }

    public void dropSchema(String schemaName) {
        String skPrefix = key(schemaName) + ".";
        List<String> toDrop = new ArrayList<>();
        for (String k : tables.keySet()) {
            if (k.startsWith(skPrefix)) {
                toDrop.add(k);
            }
        }
        for (String k : toDrop) {
            ArrowTable table = tables.remove(k);
            if (table != null) {
                table.close();
            }
            if (statsManager != null) {
                statsManager.dropStats(k);
            }
            dirty.remove(k);
        }
        catalog.dropSchema(schemaName); // throws for public / missing — do last
        try {
            Path schemaDir = dataDir.resolve(key(schemaName));
            if (Files.exists(schemaDir)) {
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(schemaDir)) {
                    for (Path p : ds) {
                        Files.deleteIfExists(p);
                    }
                }
                Files.deleteIfExists(schemaDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void truncateTable(String schemaName, String tableName) {
        ArrowTable table = tables.get(storageKey(schemaName, tableName));
        if (table == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        table.clear();
        markDirty(schemaName, tableName);
    }

    public void truncateTable(String name) {
        truncateTable(MiniDbCatalog.DEFAULT_SCHEMA, name);
    }

    public void markDirty(String schemaName, String tableName) {
        String sk = storageKey(schemaName, tableName);
        dirty.add(sk);
        if (statsManager != null) {
            statsManager.markStale(sk);
        }
    }

    public void markDirty(String tableName) {
        markDirty(MiniDbCatalog.DEFAULT_SCHEMA, tableName);
    }

    public void flushDirty() {
        for (String sk : List.copyOf(dirty)) {
            flushTable(sk);
            dirty.remove(sk);
        }
    }

    private void flushTable(String sk) {
        ArrowTable table = tables.get(sk);
        if (table == null) {
            return;
        }
        String[] parts = sk.split("\\.");
        String schemaName = parts[0];
        String tableName = parts[1];
        try {
            Path file = dataDir.resolve(fileName(schemaName, tableName));
            Files.createDirectories(file.getParent());
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
            LOG.info("flushed table {}", sk);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        flushDirty();
        for (ArrowTable table : tables.values()) {
            table.close();
        }
        tables.clear();
    }

    private static String storageKey(String schemaName, String tableName) {
        return key(schemaName) + "." + key(tableName);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static String fileName(String schemaName, String tableName) {
        return key(schemaName) + "/" + key(tableName) + ".arrow";
    }

    private static TableSchema toTableSchema(
            org.apache.arrow.vector.types.pojo.Schema arrowSchema,
            String schemaName, String tableName) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            columns.add(new ColumnMeta(field.getName(), toColumnType(field.getType())));
        }
        String resolvedSchema = schemaName;
        Map<String, String> meta = arrowSchema.getCustomMetadata();
        if (meta != null && meta.containsKey("schema")) {
            resolvedSchema = meta.get("schema");
        }
        return new TableSchema(resolvedSchema, tableName, columns);
    }

    private static ColumnType toColumnType(ArrowType type) {
        switch (type.getTypeID()) {
            case Int: {
                ArrowType.Int intType = (ArrowType.Int) type;
                return intType.getBitWidth() == 32 ? ColumnType.INTEGER : ColumnType.BIGINT;
            }
            case FloatingPoint:
                return ColumnType.DOUBLE;
            case Utf8:
                return ColumnType.VARCHAR;
            case Bool:
                return ColumnType.BOOLEAN;
            case Date:
                return ColumnType.DATE;
            case Timestamp:
                return ColumnType.TIMESTAMP;
            default:
                throw new IllegalArgumentException(
                        "unsupported arrow type in file: " + type);
        }
    }
}
```

**注意:** `StatsManager.dropStats(sk)` 和 `markStale(sk)` 现在收到的 key 是 `"schema.table"`。StatsManager 内部 `key()` 会再 `toLowerCase` 一次(幂等,无害),文件名变成 `schema.table.stats`,放 `data/` 下。这避免了冲突且 StatsManager 零代码改动。但 `StatsManager.analyze(table)` 仍按裸表名调 `storage.getTable(table)`——**这在 schema 场景下会找 public。需在 Task 11 修正 StatsManager 的 analyze 路径,或在本任务确认 EXPLAIN ANALYZE 的 analyze 调用链。** 本任务先不动 StatsManager.analyze(它经 `analyzeAll` → `storage.catalog().tableNames()` 只返回 public 表,功能不破坏,只是 stats 对非 public 表不可用——可接受作为分阶段交付)。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SchemaStorageTest,StorageManagerTest`
Expected: `SchemaStorageTest` PASS。`StorageManagerTest` 可能失败——它用旧 `storage.markDirty("t")` + `dir.resolve("t.arrow")` 断言文件路径。**这是预期的破坏**:旧测试断言扁平文件名,新格式是 `public/t.arrow`。**修正方式:** 把 Task 4 的 commit 包含对 `StorageManagerTest` 的路径断言更新(见 Step 5)。

- [ ] **Step 5: 更新 StorageManagerTest 适配新路径**

在 `StorageManagerTest.java` 中,所有 `dir.resolve("t.arrow")` 改为 `dir.resolve("public").resolve("t.arrow")`。具体 3 处:`createFlushReloadKeepsData`(行 46)、`dropTableDeletesFile`(行 71、73)。`dropTableDeletesFile` 里 `storage.dropTable("t")` 后断言 `assertFalse(Files.exists(dir.resolve("public").resolve("t.arrow")))`。其余逻辑不变(`storage.getTable("t")` 仍走 public 委托)。

同时 `StorageManagerTest.createFlushReloadKeepsData` 第一段 `storage.close()` 后断言 `assertTrue(Files.exists(dir.resolve("public").resolve("t.arrow")))`。

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SchemaStorageTest,StorageManagerTest`
Expected: 全 PASS。

- [ ] **Step 6: 全量编译 + 服务端测试回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 现有测试可能仍有 failures(如 `StatsManagerTest` 用 `storage.markDirty("t")` + reload,stats 文件名变 `public.t.stats` 但测试断言路径?——`StatsManagerTest` 不直接断言文件路径,只调 `stats.tableStats("t")`。`StatsManager.loadAll` 用 `storage.catalog().hasTable(tableName)` 判断,`hasTable(String)` 委托 public,而 stats 文件名现在是 `public.t.stats`,`stripExtension` 得 `public.t`,`hasTable("public.t")` → false,所以 stats 不会被加载!**这是回归。**

**修正:** 在 `StatsManager.loadAll` 里,文件名是 `public.t.stats`,`stripExtension` 得 `public.t`,需把它拆成 schema+table 再 `hasTable(schema, table)`。**但这要改 StatsManager,属于 Task 11。** 为让 Task 4 不留红色回归,本任务同时做 StatsManager 的最小适配:把 `StatsManager.loadAll` 的 `hasTable(tableName)` 改为解析 `schema.table` key。**决策:把 StatsManager 适配并入本任务 Step 7,因为它紧耦合于文件名格式变更。**

- [ ] **Step 7: 适配 StatsManager 的 key 语义**

在 `StatsManager.java`:
- `loadAll`:`stripExtension` 得到的 `tableName` 形如 `public.t`。改为:
  ```java
  String fileName = stripExtension(file.getFileName().toString());
  String[] parts = fileName.split("\\.", 2);
  String schema = parts.length == 2 ? parts[0] : MiniDbCatalog.DEFAULT_SCHEMA;
  String table = parts.length == 2 ? parts[1] : fileName;
  if (storage.catalog().hasTable(schema, table)) {
      TableStats ts = read(file);
      if (ts != null) {
          tables.put(key(fileName), ts);  // key 用完整 "schema.table"
      }
  }
  ```
- `tableStats(String table)`:`tables.get(key(table))`——调用方传 `"public.t"` 或 `"t"`?为兼容两种,优先查 `key(table)`,miss 时再查 `key("public." + table)`。**简化:** 统一约定 stats 的 key 恒为 `schema.table`。但 `StatsManagerTest` 调 `stats.tableStats("t")`(裸名)。**修正:** `tableStats` 兼容裸名(默认 public):
  ```java
  public TableStats tableStats(String table) {
      TableStats ts = tables.get(key(table));
      if (ts == null && !table.contains(".")) {
          ts = tables.get(key(MiniDbCatalog.DEFAULT_SCHEMA + "." + table));
      }
      return ts;
  }
  ```
  同理 `markStale` 和 `dropStats` 也要兼容裸名(委托 public)。给三者都加同样的 fallback helper:
  ```java
  private String resolveKey(String table) {
      if (table.contains(".")) return key(table);
      return key(MiniDbCatalog.DEFAULT_SCHEMA + "." + table);
  }
  ```
  并把 `tables.get/put/remove(key(table))` 改为 `resolveKey(table)`。`persist`/`statsFile`/`analyze` 里的 `key(table)` 也改 `resolveKey(table)`。`analyze(String table)` 里 `storage.getTable(table)` 改 `storage.getTable(MiniDbCatalog.DEFAULT_SCHEMA, table)`(analyze 当前只支持 public,非 public 表的 stats 收集留 Task 11)。

  需 import `com.minidb.server.catalog.MiniDbCatalog`。

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StatsManagerTest`
Expected: PASS。

- [ ] **Step 8: 全量服务端测试**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 全 PASS(含 CalciteContextTest/PlannerTest/ExplainExecutorTest 等——它们用 public 表,走委托路径,不受影响)。

- [ ] **Step 9: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java \
        minidb-server/src/main/java/com/minidb/server/stats/StatsManager.java \
        minidb-server/src/test/java/com/minidb/server/storage/SchemaStorageTest.java \
        minidb-server/src/test/java/com/minidb/server/storage/StorageManagerTest.java
git commit -m "feat: schema-aware storage with per-schema subdirectories and cascade drop"
```

---

### Task 5: MiniDbCalciteSchema 单 schema 实例 + RootCalciteSchema

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteSchema.java`
- Create: `minidb-server/src/main/java/com/minidb/server/calcite/MiniDbRootCalciteSchema.java`
- Test: `minidb-server/src/test/java/com/minidb/server/calcite/CalciteContextTest.java`(已有,加测试)

**Interfaces:**
- Consumes: `MiniDbCatalog.schemaNames()`/`tableNames(schema)`/`getTable(schema,table)`(Task 2)。
- Produces:
  - `MiniDbRootCalciteSchema(MiniDbCatalog catalog)` — 挂在 root 的 `"minidb"` 名下;`getSubSchemaMap()` 返回 `{schemaName → MiniDbCalciteSchema(catalog, schemaName)}` 对每个 catalog schema。
  - `MiniDbCalciteSchema(MiniDbCatalog catalog, String schemaName)` — `getTableMap()` 返回该 schema 下所有表(`MiniDbCalciteTable`)。

- [ ] **Step 1: 写失败测试**

在 `CalciteContextTest.java` 末尾加(需 `import com.minidb.server.catalog.ColumnType;` 已有):

```java
@Test
void qualifiedNameResolvesAcrossSchemas() {
    MiniDbCatalog catalog = new MiniDbCatalog();
    catalog.createSchema("other");
    catalog.createTable(new TableSchema("public", "t", List.of(
            new ColumnMeta("id", ColumnType.INTEGER))));
    catalog.createTable(new TableSchema("other", "t", List.of(
            new ColumnMeta("id", ColumnType.INTEGER))));
    CalciteContext ctx = new CalciteContext(catalog);
    // 默认 currentSchema=public,unqualified t → public.t
    RelRoot r1 = ctx.plan("SELECT id FROM t");
    assertNotNull(r1.rel);
    // 限定名 other.t
    RelRoot r2 = ctx.plan("SELECT id FROM other.t");
    assertNotNull(r2.rel);
    assertEquals(List.of("id"), r2.rel.getRowType().getFieldNames());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=CalciteContextTest`
Expected: `qualifiedNameResolvesAcrossSchemas` 失败(Calcite 找不到 `other.t`,因为当前 `MiniDbCalciteSchema` 把所有表扁平暴露、无子 schema)。

- [ ] **Step 3: 实现 MiniDbCalciteSchema(单 schema)+ MiniDbRootCalciteSchema**

`MiniDbCalciteSchema.java`:

```java
package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

public class MiniDbCalciteSchema extends AbstractSchema {

    private final MiniDbCatalog catalog;
    private final String schemaName;

    public MiniDbCalciteSchema(MiniDbCatalog catalog, String schemaName) {
        this.catalog = catalog;
        this.schemaName = schemaName.toLowerCase(Locale.ROOT);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        for (String name : catalog.tableNames(schemaName)) {
            TableSchema ts = catalog.getTable(schemaName, name);
            tables.put(name, new MiniDbCalciteTable(ts));
        }
        return tables;
    }
}
```

`MiniDbRootCalciteSchema.java`:

```java
package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;

public class MiniDbRootCalciteSchema extends AbstractSchema {

    private final MiniDbCatalog catalog;

    public MiniDbRootCalciteSchema(MiniDbCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        Map<String, Schema> subs = new HashMap<>();
        for (String name : catalog.schemaNames()) {
            subs.put(name, new MiniDbCalciteSchema(catalog, name));
        }
        return subs;
    }
}
```

- [ ] **Step 4: 改 CalciteContext 用 RootCalciteSchema(在 Task 6 一并改搜索路径)**

本步骤先只让 `CalciteContext.createRootSchema` 挂 `MiniDbRootCalciteSchema` 替代旧 `MiniDbCalciteSchema`,但搜索路径仍是 `List.of(SCHEMA_NAME)`——这样 unqualified `t` 解析会失败(因为 `t` 现在在 `minidb.public.t` 下而非 `minidb.t`)。**所以本 Task 5 的测试此时仍会失败,需 Task 6 改搜索路径后才通过。** 为保持每任务可独立验证,把"改 createRootSchema"也放进 Task 5,搜索路径临时改 `List.of(SCHEMA_NAME, "public")`(硬编码),Task 6 再参数化为 currentSchema。

在 `CalciteContext.java`:
- import `MiniDbRootCalciteSchema`。
- `createRootSchema()`:`rootSchema.add(SCHEMA_NAME, new MiniDbRootCalciteSchema(catalog));`
- `buildCatalogReader`:`List.of(SCHEMA_NAME, "public")`(临时)。
- 删除对 `MiniDbCalciteSchema(catalog)` 旧单参构造的引用(已无)。

- [ ] **Step 5: 跑测试**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=CalciteContextTest`
Expected: `qualifiedNameResolvesAcrossSchemas` PASS(`other.t` 走子 schema,unqualified `t` 走搜索路径 `minidb → public`)。原有 4 个测试也 PASS(`t` 在 public 下,搜索路径含 public)。

Run: `./mvnw.cmd test -pl minidb-server -Dtest=PlannerTest`
Expected: PASS(planner 内部 `new CalciteContext`,同样搜索路径含 public,`t` 解析正常)。

- [ ] **Step 6: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/calcite/MiniDbCalciteSchema.java \
        minidb-server/src/main/java/com/minidb/server/calcite/MiniDbRootCalciteSchema.java \
        minidb-server/src/main/java/com/minidb/server/calcite/CalciteContext.java \
        minidb-server/src/test/java/com/minidb/server/calcite/CalciteContextTest.java
git commit -m "feat: split Calcite schema into root container + per-schema instances"
```

---

### Task 6: CalciteContext currentSchema 参数 + Planner 透传

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/calcite/CalciteContext.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/Planner.java`
- Test: `minidb-server/src/test/java/com/minidb/server/calcite/CalciteContextTest.java`(加测试)

**Interfaces:**
- Produces:
  - `CalciteContext.plan(String sql)` — 保留,委托 `planInCluster(sql, cluster, "public")` via `plan(sql)` 路径。**实际上 `plan(sql)` 不接 cluster,它自己建 cluster 再调 `planInCluster`。** 新签名:`RelRoot plan(String sql)`(默认 public)和 `RelRoot plan(String sql, String currentSchema)`。
  - `CalciteContext.planInCluster(String sql, RelOptCluster cluster, String currentSchema)` — 用 currentSchema 构造搜索路径。旧 `planInCluster(String, RelOptCluster)` 委托默认 public。
  - `Planner.plan(String sql)`(默认 public)和 `Planner.plan(String sql, String currentSchema)`。

- [ ] **Step 1: 写失败测试**

在 `CalciteContextTest.java` 加:

```java
@Test
void currentSchemaSwitchesUnqualifiedResolution() {
    MiniDbCatalog catalog = new MiniDbCatalog();
    catalog.createSchema("other");
    catalog.createTable(new TableSchema("public", "t", List.of(
            new ColumnMeta("id", ColumnType.INTEGER))));
    catalog.createTable(new TableSchema("other", "t", List.of(
            new ColumnMeta("id", ColumnType.INTEGER),
            new ColumnMeta("x", ColumnType.VARCHAR))));
    CalciteContext ctx = new CalciteContext(catalog);
    // currentSchema=other 时,unqualified t → other.t(有 x 列)
    RelRoot r = ctx.plan("SELECT x FROM t", "other");
    assertNotNull(r.rel);
    assertEquals(List.of("x"), r.rel.getRowType().getFieldNames());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=CalciteContextTest`
Expected: 编译失败(`plan(String, String)` 不存在)。

- [ ] **Step 3: 实现 CalciteContext currentSchema 参数**

在 `CalciteContext.java`:
- `plan(String sql)` 改为委托:`return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA);` 但 `plan(sql)` 当前自己建 cluster。重构:
  ```java
  public RelRoot plan(String sql) {
      return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA);
  }

  public RelRoot plan(String sql, String currentSchema) {
      HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
      SqlTypeFactoryImpl typeFactory =
              new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
      return planInCluster(sql, cluster, currentSchema);
  }
  ```
- `planInCluster(String sql, RelOptCluster cluster)` → 委托 `planInCluster(sql, cluster, MiniDbCatalog.DEFAULT_SCHEMA)`。
- 新 `planInCluster(String sql, RelOptCluster cluster, String currentSchema)`:
  ```java
  public RelRoot planInCluster(String sql, RelOptCluster cluster, String currentSchema) {
      SqlNode parsed = parse(sql);
      SqlTypeFactoryImpl typeFactory =
              (SqlTypeFactoryImpl) cluster.getTypeFactory();
      CalciteCatalogReader catalogReader = buildCatalogReader(typeFactory, currentSchema);
      SqlValidator validator = SqlValidatorUtil.newValidator(
              SqlStdOperatorTable.instance(), catalogReader, typeFactory,
              SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
      SqlNode validated = validator.validate(parsed);
      SqlToRelConverter converter = new SqlToRelConverter(
              null, validator, catalogReader, cluster,
              StandardConvertletTable.INSTANCE,
              SqlToRelConverter.config());
      return converter.convertQuery(validated, false, true);
  }
  ```
- `buildCatalogReader(typeFactory)` → `buildCatalogReader(typeFactory, currentSchema)`:
  ```java
  private CalciteCatalogReader buildCatalogReader(
          SqlTypeFactoryImpl typeFactory, String currentSchema) {
      return new CalciteCatalogReader(
              CalciteSchema.from(createRootSchema()),
              List.of(SCHEMA_NAME, currentSchema.toLowerCase(Locale.ROOT)),
              typeFactory,
              new CalciteConnectionConfigImpl(new Properties()));
  }
  ```
- import `com.minidb.server.catalog.MiniDbCatalog`、`java.util.Locale`。

- [ ] **Step 4: 实现 Planner.plan(sql, currentSchema)**

在 `Planner.java`:
  ```java
  public RelNode plan(String sql) {
      return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA);
  }

  public RelNode plan(String sql, String currentSchema) {
      VolcanoPlanner planner = new VolcanoPlanner();
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      for (org.apache.calcite.plan.RelOptRule rule : MiniDbRules.ALL) {
          planner.addRule(rule);
      }
      SqlTypeFactoryImpl typeFactory =
              new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

      RelRoot root = calcite.planInCluster(sql, cluster, currentSchema);
      RelNode logical = root.rel;
      RelNode converted = planner.changeTraits(logical,
              logical.getTraitSet().replace(MiniDbConvention.INSTANCE));
      planner.setRoot(converted);
      RelNode best = planner.findBestExp();
      if (!(best instanceof MiniDbRel)) {
          throw new IllegalStateException(
                  "planner produced non-physical root: " + best);
      }
      return best;
  }
  ```
- import `com.minidb.server.catalog.MiniDbCatalog`。

- [ ] **Step 5: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=CalciteContextTest,PlannerTest`
Expected: PASS(含新 `currentSchemaSwitchesUnqualifiedResolution`)。

- [ ] **Step 6: 全量服务端测试回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 全 PASS。

- [ ] **Step 7: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/calcite/CalciteContext.java \
        minidb-server/src/main/java/com/minidb/server/plan/Planner.java \
        minidb-server/src/test/java/com/minidb/server/calcite/CalciteContextTest.java
git commit -m "feat: thread currentSchema through CalciteContext and Planner for name resolution"
```

---

### Task 7: QueryResult.UseSchema + QueryExecutor DDL + USE SCHEMA + 限定名

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/QueryResult.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/SchemaDdlTest.java`(新建)

**Interfaces:**
- Consumes: Task 1(TableSchema schemaName)、Task 2(catalog schema API)、Task 4(storage schema API)、Task 6(Planner.plan(sql, currentSchema))。Calcite `org.apache.calcite.sql.ddl.SqlCreateSchema`/`SqlDropSchema`(字段 `name: SqlIdentifier`、`ifNotExists`/`ifExists`)。
- Produces:
  - `QueryResult.UseSchema(String schemaName)` — sealed 分支,表示 USE SCHEMA 切换。
  - `QueryExecutor.execute(String sql, String currentSchema)` — 主路径;旧 `execute(String sql)` 委托默认 public。
  - USE SCHEMA 前缀拦截:返回 `QueryResult.UseSchema(name)`(不抛错;schema 不存在时抛 `IllegalArgumentException`)。
  - `SqlCreateSchema` → `storage`/`catalog.createSchema`(若 ifNotExists 且已存在则静默)。
  - `SqlDropSchema` → `storage.dropSchema`(级联);ifExists 缺失且不存在则抛。
  - `handleCreate(SqlCreateTable, currentSchema)`:`name.names` 分解;简单名 → currentSchema,复合名 → names.get(0) 为 schema、names.get(size-1) 为 table。
  - `handleDrop`/`handleTruncate` 同理。

**前缀拦截顺序(在 `execute` 顶部,沿用现有 EXPLAIN/ANALYZE 模式):**
1. `ANALYZE`(exact)→ `ANALYZE <table>`
2. `EXPLAIN ANALYZE `
3. `EXPLAIN `
4. `USE SCHEMA <name>`(新)— 注意必须 exact 匹配 `USE SCHEMA ` 前缀(不匹配 `USE ` 单独,因为 spec 只支持 `USE SCHEMA`)。

- [ ] **Step 1: 写失败测试**

创建 `SchemaDdlTest.java`:

```java
package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDdlTest {

    @TempDir
    Path dataDir;
    BufferAllocator allocator;
    MiniDbCatalog catalog;
    StorageManager storage;
    StatsManager stats;
    QueryExecutor executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        catalog = new MiniDbCatalog();
        storage = new StorageManager(catalog, allocator, dataDir);
        stats = new StatsManager(storage, allocator, dataDir);
        storage.setStatsManager(stats);
        executor = new QueryExecutor(catalog, storage, allocator, stats);
    }

    @AfterEach
    void tearDown() {
        storage.close();
        allocator.close();
    }

    @Test
    void createSchemaMakesItVisible() {
        executor.execute("CREATE SCHEMA other", "public");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createSchemaIfNotExistsIsIdempotent() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE SCHEMA IF NOT EXISTS other", "public");
        assertTrue(catalog.schemaNames().contains("other"));
    }

    @Test
    void createDuplicateSchemaThrows() {
        executor.execute("CREATE SCHEMA other", "public");
        assertThrows(Exception.class,
                () -> executor.execute("CREATE SCHEMA other", "public"));
    }

    @Test
    void dropSchemaCascades() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("DROP SCHEMA other", "public");
        org.junit.jupiter.api.Assertions.assertFalse(
                catalog.schemaNames().contains("other"));
    }

    @Test
    void dropSchemaIfExistsMissingIsNoop() {
        executor.execute("DROP SCHEMA IF EXISTS ghost", "public");
        // no exception
    }

    @Test
    void useSchemaReturnsUseSchemaResult() {
        executor.execute("CREATE SCHEMA other", "public");
        QueryResult r = executor.execute("USE SCHEMA other", "public");
        assertTrue(r instanceof QueryResult.UseSchema);
        assertEquals("other", ((QueryResult.UseSchema) r).schemaName());
    }

    @Test
    void useSchemaMissingThrows() {
        assertThrows(Exception.class,
                () -> executor.execute("USE SCHEMA ghost", "public"));
    }

    @Test
    void createTableUnqualifiedUsesCurrentSchema() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE t (id INTEGER)", "other");
        assertTrue(catalog.hasTable("other", "t"));
        assertEquals("other", catalog.getTable("other", "t").schemaName());
    }

    @Test
    void createTableQualifiedNamesSchema() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        assertTrue(catalog.hasTable("other", "t"));
    }

    @Test
    void dropTableQualified() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("DROP TABLE other.t", "public");
        org.junit.jupiter.api.Assertions.assertFalse(
                catalog.hasTable("other", "t"));
    }

    @Test
    void selectQualifiedRoundtrips() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("INSERT INTO other.t VALUES (5)", "public");
        QueryResult r = executor.execute("SELECT id FROM other.t", "public");
        assertTrue(r instanceof QueryResult.Rows);
        assertEquals(1, ((QueryResult.Rows) r).data().getRowCount());
        ((QueryResult.Rows) r).data().close();
    }

    @Test
    void useSchemaThenUnqualifiedSelect() {
        executor.execute("CREATE SCHEMA other", "public");
        executor.execute("CREATE TABLE other.t (id INTEGER)", "public");
        executor.execute("INSERT INTO other.t VALUES (7)", "other");
        QueryResult r = executor.execute("SELECT id FROM t", "other");
        assertTrue(r instanceof QueryResult.Rows);
        assertEquals(7, ((org.apache.arrow.vector.IntVector)
                ((QueryResult.Rows) r).data().getVector(0)).get(0));
        ((QueryResult.Rows) r).data().close();
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SchemaDdlTest`
Expected: 编译失败(`execute(String,String)`、`QueryResult.UseSchema` 不存在)。

- [ ] **Step 3: 先读 QueryResult.java 确认其结构**

Run: `./mvnw.cmd -pl minidb-server -am compile -q` 不会读文件。**用 Read 工具读 `exec/QueryResult.java`** 确认 sealed 接口结构,然后加 `UseSchema` 分支。

`QueryResult.java` 当前(从 CLAUDE.md 推断)是 `sealed interface QueryResult permits Rows, Update`。改为 `permits Rows, Update, UseSchema`,新增:

```java
record UseSchema(String schemaName) implements QueryResult { }
```

(具体步骤:Read `QueryResult.java` → Edit 加 `UseSchema`。)

- [ ] **Step 4: 实现 QueryExecutor.execute(sql, currentSchema) + DDL**

完整改造 `QueryExecutor.java`。关键点:

```java
package com.minidb.server.exec;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.plan.MiniDbModify;
import com.minidb.server.plan.MiniDbRel;
import com.minidb.server.plan.Planner;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlTruncateTable;

public class QueryExecutor {

    private final MiniDbCatalog catalog;
    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final Planner planner;
    private final CalciteContext calcite;
    private final StatsManager stats;

    public QueryExecutor(MiniDbCatalog catalog, StorageManager storage,
                         BufferAllocator allocator, StatsManager stats) {
        this.catalog = catalog;
        this.storage = storage;
        this.allocator = allocator;
        this.stats = stats;
        this.planner = new Planner(catalog);
        this.calcite = new CalciteContext(catalog);
    }

    public QueryResult execute(String sql) {
        return execute(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public QueryResult execute(String sql, String currentSchema) {
        String trimmed = sql.strip();
        String upper = trimmed.toUpperCase(Locale.ROOT);
        if (upper.equals("ANALYZE")) {
            stats.analyzeAll();
            return new QueryResult.Update(0);
        }
        if (upper.startsWith("ANALYZE ")) {
            String table = trimmed.substring("ANALYZE ".length()).strip();
            stats.analyze(table);
            return new QueryResult.Update(0);
        }
        if (upper.startsWith("EXPLAIN ANALYZE ")) {
            String inner = trimmed.substring("EXPLAIN ANALYZE ".length());
            return new ExplainExecutor(planner, stats, storage, allocator)
                    .analyze(inner, currentSchema);
        }
        if (upper.startsWith("EXPLAIN ")) {
            String inner = trimmed.substring("EXPLAIN ".length());
            return new ExplainExecutor(planner, stats, storage, allocator)
                    .explain(inner, currentSchema);
        }
        if (upper.startsWith("USE SCHEMA ")) {
            String name = trimmed.substring("USE SCHEMA ".length()).strip();
            String resolved = name.toLowerCase(Locale.ROOT);
            if (!catalog.schemaNames().contains(resolved)) {
                throw new IllegalArgumentException("schema not found: " + name);
            }
            return new QueryResult.UseSchema(resolved);
        }
        SqlNode parsed = calcite.parse(sql);
        if (parsed instanceof SqlCreateSchema create) {
            return handleCreateSchema(create);
        }
        if (parsed instanceof SqlDropSchema drop) {
            return handleDropSchema(drop);
        }
        if (parsed instanceof SqlCreateTable create) {
            return handleCreate(create, currentSchema);
        }
        if (parsed instanceof SqlDropTable drop) {
            return handleDrop(drop, currentSchema);
        }
        if (parsed instanceof SqlTruncateTable truncate) {
            return handleTruncate(truncate, currentSchema);
        }
        RelNode plan = planner.plan(sql, currentSchema);
        ExecContext ctx = new ExecContext(storage, allocator);
        if (plan instanceof MiniDbModify modify) {
            try (BatchIterator it = modify.execute(ctx)) {
                while (it.hasNext()) {
                    it.next();
                }
                return new QueryResult.Update(modify.affected());
            }
        }
        try (BatchIterator it = ((MiniDbRel) plan).execute(ctx)) {
            return new QueryResult.Rows(materialize(it, plan));
        }
    }

    private QueryResult handleCreateSchema(SqlCreateSchema create) {
        String name = create.name.getSimple();
        if (create.ifNotExists && catalog.schemaNames().contains(name.toLowerCase(Locale.ROOT))) {
            return new QueryResult.Update(0);
        }
        catalog.createSchema(name);
        return new QueryResult.Update(0);
    }

    private QueryResult handleDropSchema(SqlDropSchema drop) {
        String name = drop.name.getSimple();
        if (drop.ifExists && !catalog.schemaNames().contains(name.toLowerCase(Locale.ROOT))) {
            return new QueryResult.Update(0);
        }
        storage.dropSchema(name);
        return new QueryResult.Update(0);
    }

    private QueryResult handleCreate(SqlCreateTable create, String currentSchema) {
        List<String> parts = create.name.names;
        String schemaName = parts.size() > 1
                ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        List<ColumnMeta> columns = new ArrayList<>();
        for (SqlNode columnNode : create.columnList) {
            SqlColumnDeclaration column = (SqlColumnDeclaration) columnNode;
            String typeName = column.dataType.getTypeName().getSimple();
            ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
            columns.add(new ColumnMeta(column.name.getSimple(), type));
        }
        TableSchema schema = new TableSchema(schemaName, tableName, columns);
        storage.createTable(schema);
        return new QueryResult.Update(0);
    }

    private QueryResult handleDrop(SqlDropTable drop, String currentSchema) {
        List<String> parts = drop.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        if (!catalog.hasTable(schemaName, tableName)) {
            if (drop.ifExists) {
                return new QueryResult.Update(0);
            }
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        storage.dropTable(schemaName, tableName);
        return new QueryResult.Update(0);
    }

    private QueryResult handleTruncate(SqlTruncateTable truncate, String currentSchema) {
        List<String> parts = truncate.name.names;
        String schemaName = parts.size() > 1 ? parts.get(0) : currentSchema;
        String tableName = parts.get(parts.size() - 1);
        storage.truncateTable(schemaName, tableName);
        return new QueryResult.Update(0);
    }

    // materialize / emptyRoot 不变(保留原实现)
}
```

**注意:** `ExplainExecutor.explain(inner)` / `analyze(inner)` 改为接 `currentSchema` —— 这需 Task 9 完成。**为让本任务编译通过**,本任务先在 `ExplainExecutor` 加 `explain(inner, currentSchema)`/`analyze(inner, currentSchema)` 重载(委托旧单参方法,临时忽略 currentSchema),Task 9 再实现真实透传。**或者**把 ExplainExecutor 改动放本任务。**决策:** 本任务在 `ExplainExecutor` 加两个重载方法,临时委托单参版本(传 `MiniDbCatalog.DEFAULT_SCHEMA`),保证编译 + 现有 EXPLAIN 测试不破。Task 9 把它替换为真实透传。

在 `ExplainExecutor.java` 临时加(Read 后 Edit):

```java
public QueryResult explain(String sql, String currentSchema) {
    return explain(sql);  // Task 9 会改为真实透传
}

public QueryResult analyze(String sql, String currentSchema) {
    return analyze(sql);  // Task 9 会改为真实透传
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SchemaDdlTest`
Expected: 全 PASS。

- [ ] **Step 6: 全量服务端测试回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 全 PASS(含 ExplainExecutorTest——临时重载委托旧路径,行为不变)。

- [ ] **Step 7: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/QueryResult.java \
        minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java \
        minidb-server/src/main/java/com/minidb/server/exec/ExplainExecutor.java \
        minidb-server/src/test/java/com/minidb/server/exec/SchemaDdlTest.java
git commit -m "feat: CREATE/DROP SCHEMA, USE SCHEMA, and qualified table name resolution"
```

---

### Task 8: SessionHandler per-channel currentSchema

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java`
- Test: `minidb-server/src/test/java/com/minidb/server/netty/SessionHandlerSchemaTest.java`(新建)

**Interfaces:**
- Consumes: `QueryExecutor.execute(sql, currentSchema)`、`QueryResult.UseSchema`(Task 7)。
- Produces: `SessionHandler` 持有 `private String currentSchema = MiniDbCatalog.DEFAULT_SCHEMA;`,每次 `handleExecute` 调 `executor.execute(sql, currentSchema)`;若结果为 `UseSchema`,更新 `currentSchema` 字段并回 `Message.UpdateCount(requestId, 0)`。

- [ ] **Step 1: 写失败测试**

创建 `SessionHandlerSchemaTest.java`(用嵌入式 `QueryExecutor`,不经网络,直接调 `executor.execute` 模拟——因为 `SessionHandler` 的网络层难单测。**改策略:** 测 `SessionHandler` 需 Netty EmbeddedChannel。简化:本任务测 `QueryExecutor.execute` 的 USE SCHEMA 返回 `UseSchema` 已在 Task 7 覆盖;本任务聚焦 `SessionHandler` 把 `UseSchema` 翻译成 `UpdateCount` 并更新字段。用 `io.netty.channel.embedded.EmbeddedChannel` + `Message.ExecuteRequest` 驱动)。

```java
package com.minidb.server.netty;

import com.minidb.protocol.Message;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.exec.QueryResult;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.file.Path;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionHandlerSchemaTest {

    @Test
    void useSchemaUpdatesCurrentSchemaForSubsequentQueries(@TempDir Path dir) {
        MiniDbCatalog catalog = new MiniDbCatalog();
        RootAllocator allocator = new RootAllocator();
        StorageManager storage = new StorageManager(catalog, allocator, dir);
        StatsManager stats = new StatsManager(storage, allocator, dir);
        storage.setStatsManager(stats);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        EmbeddedChannel ch = new EmbeddedChannel(new SessionHandler(executor));
        try {
            // create schema + table in default public
            ch.writeInbound(new Message.ExecuteRequest(1, "CREATE SCHEMA other"));
            ch.writeInbound(new Message.ExecuteRequest(2,
                    "CREATE TABLE other.t (id INTEGER)"));
            // USE SCHEMA other → should yield UpdateCount, switch this channel's schema
            ch.writeInbound(new Message.ExecuteRequest(3, "USE SCHEMA other"));
            Object out3 = ch.outboundMessages().poll();
            assertTrue(out3 instanceof Message.UpdateCount);
            // now unqualified CREATE TABLE goes to other
            ch.writeInbound(new Message.ExecuteRequest(4, "CREATE TABLE u (id INTEGER)"));
            assertTrue(catalog.hasTable("other", "u"));
        } finally {
            storage.close();
            allocator.close();
            ch.close();
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SessionHandlerSchemaTest`
Expected: 失败——`SessionHandler` 当前调 `executor.execute(sql)`(单参),`USE SCHEMA` 返回 `UseSchema` 走入 `Rows`/`Update` 之外的无分支,且 `CREATE TABLE u` 在 public 下建表而非 other。

- [ ] **Step 3: 实现 SessionHandler currentSchema**

在 `SessionHandler.java`:

```java
public class SessionHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHandler.class);

    private final QueryExecutor executor;
    private String currentSchema = MiniDbCatalog.DEFAULT_SCHEMA;

    public SessionHandler(QueryExecutor executor) {
        this.executor = executor;
    }
    // channelRead0 不变
    private void handleExecute(ChannelHandlerContext ctx, Message.ExecuteRequest req) {
        LOG.debug("executing: {}", req.sql());
        long start = System.nanoTime();
        try {
            QueryResult result = executor.execute(req.sql(), currentSchema);
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
                sendRows(ctx, req.requestId(), rows.data());
                rows.data().close();
            }
        } catch (Exception e) {
            // 不变
        }
    }
    // 其余不变
}
```

需 import `com.minidb.server.catalog.MiniDbCatalog`、`com.minidb.server.exec.QueryResult`。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=SessionHandlerSchemaTest`
Expected: PASS。

- [ ] **Step 5: 全量回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 全 PASS。

- [ ] **Step 6: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/netty/SessionHandler.java \
        minidb-server/src/test/java/com/minidb/server/netty/SessionHandlerSchemaTest.java
git commit -m "feat: per-channel currentSchema in SessionHandler driven by USE SCHEMA"
```

---

### Task 9: ExplainExecutor 透传 currentSchema

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/ExplainExecutor.java`
- Test: `minidb-server/src/test/java/com/minidb/server/exec/ExplainExecutorTest.java`(已有,加测试)

**Interfaces:**
- Consumes: `Planner.plan(sql, currentSchema)`(Task 6)。
- Produces: `explain(String sql, String currentSchema)`、`analyze(String sql, String currentSchema)` 真实透传 currentSchema 到 `planner.plan(sql, currentSchema)`。旧单参 `explain(sql)`/`analyze(sql)` 委托默认 public。

**先读 `ExplainExecutor.java`** 确认它如何调 `planner.plan`(应该是 `planner.plan(sql)`)。

- [ ] **Step 1: 读 ExplainExecutor 确认耦合点**

用 Read 工具读 `minidb-server/src/main/java/com/minidb/server/exec/ExplainExecutor.java`,定位 `planner.plan(...)` 调用处。

- [ ] **Step 2: 写失败测试**

在 `ExplainExecutorTest.java` 加(先 Read 该文件看 helper):

```java
@Test
void explainResolvesTableInCurrentSchema() {
    // 建 other schema + other.t,EXPLAIN SELECT 在 currentSchema=other 下解析
    // 具体 setup 沿用该测试文件的 helper(catalog/storage/executor)
    // 断言 EXPLAIN 不抛、返回 Rows
}
```

(具体测试代码在 Read `ExplainExecutorTest.java` 后补全——需看其 helper 方法签名。)

- [ ] **Step 3: 实现 currentSchema 透传**

在 `ExplainExecutor.java`:
- 旧 `explain(String sql)` → `return explain(sql, MiniDbCatalog.DEFAULT_SCHEMA);`
- `explain(String sql, String currentSchema)` 把内部 `planner.plan(sql)` 改 `planner.plan(sql, currentSchema)`。
- `analyze` 同理。
- 删除 Task 7 加的临时委托重载(被真实实现替代)。

- [ ] **Step 4: 跑测试**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=ExplainExecutorTest`
Expected: PASS。

- [ ] **Step 5: 全量回归**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: 全 PASS。

- [ ] **Step 6: Commit**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/ExplainExecutor.java \
        minidb-server/src/test/java/com/minidb/server/exec/ExplainExecutorTest.java
git commit -m "feat: thread currentSchema through ExplainExecutor for EXPLAIN/ANALYZE"
```

---

### Task 10: MiniDbResultSetMetaData.getSchemaName + 端到端

**Files:**
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSetMetaData.java`
- Test: `minidb-jdbc/src/test/java/com/minidb/jdbc/SchemaMetadataTest.java`(新建)

**Interfaces:**
- Consumes: Arrow `Schema.getCustomMetadata()` 跨 IPC 保留(Task 3 写入,经 `SessionHandler.sendRows` 的 `ArrowStreamWriter` → wire → `MiniDbClient.readArrow` 的 `ArrowStreamReader` → `VectorSchemaRoot.getSchema()`)。
- Produces: `getSchemaName(int column)` 返回 metadata 中 `"schema"` 键的值,缺省 `""`。

**注意:** minidb-jdbc 测试因 Calcite 不在测试 classpath 会有 `NoClassDefFoundError` 环境问题(CLAUDE.md 坑 #12)。但 `SchemaMetadataTest` 只测 `MiniDbResultSetMetaData` 纯客户端逻辑,不经网络、不引 Calcite——需手动构造带 metadata 的 `VectorSchemaRoot`。**或** 经网络端到端(需启动 server)。**决策:** 用纯客户端单测:用 `RootAllocator` 构造带 `Schema(fields, Map.of("schema","public"))` 的 `VectorSchemaRoot`,直接 new `MiniDbResultSetMetaData(root)`,断言 `getSchemaName(1)`。这避免网络 + Calcite。

- [ ] **Step 1: 写失败测试**

创建 `SchemaMetadataTest.java`:

```java
package com.minidb.jdbc;

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaMetadataTest {

    private VectorSchemaRoot rootWithSchema(String schemaName) {
        Schema schema = new Schema(
                List.of(new Field("id", new ArrowType.Int(32, true), null)),
                schemaName == null ? null : Map.of("schema", schemaName));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator());
        root.allocateNew();
        ((IntVector) root.getVector(0)).setSafe(0, 1);
        root.setRowCount(1);
        return root;
    }

    @Test
    void getSchemaNameReadsArrowMetadata() {
        try (VectorSchemaRoot root = rootWithSchema("public")) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals("public", md.getSchemaName(1));
        }
    }

    @Test
    void getSchemaNameEmptyWhenMetadataAbsent() {
        try (VectorSchemaRoot root = rootWithSchema(null)) {
            MiniDbResultSetMetaData md = new MiniDbResultSetMetaData(root);
            assertEquals("", md.getSchemaName(1));
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-jdbc -Dtest=SchemaMetadataTest`
Expected: 失败——`getSchemaName` 当前返回 `""`。

- [ ] **Step 3: 实现 getSchemaName**

在 `MiniDbResultSetMetaData.java`:

```java
@Override
public String getSchemaName(int column) {
    Map<String, String> meta = root.getSchema().getCustomMetadata();
    return meta != null ? meta.getOrDefault("schema", "") : "";
}
```

import `java.util.Map`。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-jdbc -Dtest=SchemaMetadataTest`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbResultSetMetaData.java \
        minidb-jdbc/src/test/java/com/minidb/jdbc/SchemaMetadataTest.java
git commit -m "feat: MiniDbResultSetMetaData.getSchemaName reads Arrow schema metadata"
```

---

### Task 11: 迁移现有测试 + 全量回归 + 文档

**Files:**
- Modify: 多个测试文件(把 `new TableSchema(name, cols)` 显式化为 `new TableSchema("public", name, cols)` 或保留工厂——**保留工厂即可,无需改动**,除非测试要断言 schemaName)。
- Modify: `README.md`(说明 SCHEMA 支持 + 持久化格式变更)。
- Modify: `.claude/CLAUDE.md`(更新 schema 相关架构说明 + 坑)。

**目标:** 确保全量 `./mvnw.cmd test` 绿,文档反映新能力。

- [ ] **Step 1: 全量测试**

Run: `./mvnw.cmd test`
Expected: 全 PASS(minidb-jdbc 的 `NoClassDefFoundError` 环境问题除外——CLAUDE.md 坑 #12,非回归)。

- [ ] **Step 2: 修补任何遗留测试**

若有测试因 schema 改造失败,逐个修复(优先显式 `"public"` 构造器)。

- [ ] **Step 3: 更新 README.md**

在特性列表加:schema 支持(`CREATE SCHEMA`/`DROP SCHEMA`/`USE SCHEMA`/限定名),默认 `public`。在限制/破坏性变更里说明:持久化格式从 `data/<table>.arrow` 改为 `data/<schema>/<table>.arrow`,旧数据需手动迁移(`mv data/*.arrow data/public/`)。

- [ ] **Step 4: 更新 .claude/CLAUDE.md**

- 架构段:catalog 两级 map;Calcite schema 树 root → minidb → 各 schema;currentSchema 是 per-channel(SessionHandler)非共享;StorageManager 子目录持久化。
- 关键类段:`MiniDbCatalog` 加 schema API;`TableSchema` 加 schemaName;`MiniDbRootCalciteSchema`/`MiniDbCalciteSchema` 单 schema;`QueryExecutor.execute(sql, currentSchema)`;`QueryResult.UseSchema`;`SessionHandler.currentSchema`。
- 踩坑段加:
  - `SqlIdentifier.getSimple()` 在断言关闭时对复合名静默返回首段,限定名解析必须用 `names` 分解。
  - `currentSchema` 不能放共享 `CalciteContext` 成员(并发污染),必须作参数流经调用链。
  - 持久化子目录:`flushTable` 必须 `createDirectories(file.getParent())`。
  - StatsManager key 语义变为 `schema.table`,`tableStats`/`markStale`/`dropStats` 需兼容裸名(默认 public)。

- [ ] **Step 5: 最终全量验证**

Run: `./mvnw.cmd test`
Expected: 全 PASS。

- [ ] **Step 6: Commit**

```bash
git add README.md .claude/CLAUDE.md <任何修补的测试>
git commit -m "docs: document schema support and update CLAUDE.md for schema architecture"
```

---

## Self-Review

**1. Spec 覆盖:**
- 目标 1(默认 public):Task 1/2 ✓
- 目标 2(getSchemaName):Task 10 ✓
- 目标 3(CREATE/DROP SCHEMA):Task 7 ✓
- 目标 4(schema.table 限定名):Task 5/6/7 ✓
- 目标 5(USE SCHEMA):Task 7/8 ✓(per-channel 隔离,偏离 spec 字面但更正确)
- 目标 6(零协议改动):全计划未触 minidb-protocol ✓
- 目标 7(零算子侵入):全计划未触 6 个算子文件 ✓
- 改动点 1-7:Task 1-10 全覆盖 ✓
- 测试(单元+集成):Task 2/4/7/8/10 ✓

**2. 占位符扫描:** Task 9 Step 2 的 EXPLAIN 测试代码标注"Read 后补全"——这是可接受的(依赖读现有 helper),但为彻底无占位,执行时必须先 Read `ExplainExecutorTest.java` 再写完整测试。Task 11 的"修补任何遗留测试"是条件性的,非占位。

**3. 类型一致性:**
- `TableSchema(schemaName, name, columns)` 全计划一致 ✓
- `catalog.createSchema/dropSchema/schemaNames/getTable(schema,table)/hasTable(schema,table)/tableNames(schema)/dropTable(schema,table)` 签名一致 ✓
- `storage.getTable(schema,table)/dropTable(schema,table)/markDirty(schema,table)/truncateTable(schema,table)/dropSchema(schema)` 一致 ✓
- `QueryExecutor.execute(sql, currentSchema)`、`Planner.plan(sql, currentSchema)`、`CalciteContext.planInCluster(sql, cluster, currentSchema)`、`ExplainExecutor.explain/analyze(sql, currentSchema)` 一致 ✓
- `QueryResult.UseSchema(schemaName)` 一致 ✓
- `MiniDbCalciteSchema(catalog, schemaName)`、`MiniDbRootCalciteSchema(catalog)` 一致 ✓

**残留风险:**
- StatsManager.analyze 对非 public 表不可用(Task 4 Step 7 注明,Task 11 不修——分阶段交付,EXPLAIN ANALYZE 对非 public 表的统计降级,可接受)。
- 持久化格式不兼容旧扁平数据(文档说明手动迁移)。
