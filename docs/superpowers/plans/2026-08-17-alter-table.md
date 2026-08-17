# ALTER TABLE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 MiniDB 实现 `ALTER TABLE`(列增删/改名/改类型/约束变更)。

**Architecture:** 沿用现有 DDL 模式——新增 `SqlAlterTable` parser 节点 → `QueryExecutor.handleDdl` 分发 → `AlterTableHandler` 解析语义并构造新 `TableSchema`(不可变,new 一个)→ 结构类操作重写 part 文件、元数据类操作只替换 catalog + 重建 `SimpleTable`。类型转换与约束校验从 `RexInterpreter`/`MiniDbModify` 抽成单一来源工具复用。

**Tech Stack:** Java 17、Maven(mvnw)、Apache Calcite(parser codegen)、Apache Arrow(列式)、JUnit 5。

**Spec:** `docs/superpowers/specs/2026-08-17-alter-table-design.md`

## Global Constraints

- JDK 17 必须;构建用 `./mvnw.cmd`(bash 下)。
- 单模块测试:`./mvnw.cmd test -pl minidb-server -Dtest=<Class>`;全量:`./mvnw.cmd test`。
- 改动按 conventional commit(`feat:`/`refactor:`/`test:`),小步提交,不 amend、不 `--no-verify`。
- 代码命名自解释、不加复述 WHAT 的注释;非显然逻辑(如 null 键唯一性、转换复用 CAST 语义)加 WHY 注释。
- `TableSchema`/`ColumnMeta` 是不可变 record;改结构 = new 一个新对象替换,不原地改。
- `RexInterpreter`/物理算子(稳定核心)尽量少改;工具抽取是唯一触碰点,先跑通现有测试再重构。

---

### Task 1: 抽取 VectorCasts 类型转换工具

把 `RexInterpreter.evalCast` 的逐行转换 switch 抽成单一来源,ALTER TYPE 与 CAST 共用。

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/VectorCasts.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/RexInterpreter.java`(evalCast 547–725、asLong 944–970、asDouble 972–998、asString 1000–1026、asBoolean 1028–1054)

**Interfaces:**
- Consumes: `ArrowTypes.fromSqlTypeName(String)`(storage-common)、`ArrowTypes.field(ColumnMeta)`(public,已存在)、`Kernels.scaleTo(DecimalVector, BigDecimal)`(exec.functions,已存在)。
- Produces: `VectorCasts.cast(ValueVector src, ColumnType target, int precision, int scale, BufferAllocator allocator) : FieldVector`。不关闭 src,调用方负责。

- [ ] **Step 1: 跑现有 CAST 测试,建立回归基线**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=RexInterpreterTest,DateCastTest,ImplicitCastTest,NewTypeExpressionTest`
Expected: 全绿(记录通过数)。

- [ ] **Step 2: 新建 VectorCasts.java**

把 `evalCast` 的 switch 体搬进 `VectorCasts.cast`,目标类型按 `ColumnType` 分派(而非 `SqlTypeName`),源值读取用平移来的 `asLong/asDouble/asString/asBoolean`(改为 private static)。目标向量用 `ArrowTypes.field(new ColumnMeta("cast", target, precision, scale)).createVector(allocator)` 创建。DECIMAL 目标用 `Kernels.scaleTo`;DATE/TIMESTAMP 目标保留 `new DateString(...)`/`new TimestampString(...)` 分支;CHAR/VARCHAR/NCHAR/NVARCHAR 目标统一 VarCharVector;BINARY/VARBINARY 统一 VarBinaryVector。保留 NULL 透传(源 `isNull` → 目标 `setNull`)。结尾 `out.setValueCount(rows)`。

- [ ] **Step 3: 改 RexInterpreter.evalCast 为薄壳**

`evalCast` 改为:先 `eval(call.getOperands().get(0), input)` 得 `ValueVector v`,try 内 `ColumnType target = ArrowTypes.fromSqlTypeName(call.getType().getSqlTypeName().getName())`,DECIMAL/NUMERIC 取 `precision=call.getType().getPrecision()/scale=call.getType().getScale()`(负值归一为 `ColumnMeta.PRECISION_UNSET/SCALE_UNSET`),`return VectorCasts.cast(v, target, precision, scale, allocator)`;finally `v.close()`。删掉原 switch 与四个 helper(已移入 VectorCasts)。

- [ ] **Step 4: 跑回归测试确认无漂移**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=RexInterpreterTest,DateCastTest,ImplicitCastTest,NewTypeExpressionTest`
Expected: 与 Step 1 相同的通过数,全绿。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/VectorCasts.java minidb-server/src/main/java/com/minidb/server/exec/RexInterpreter.java
git commit -m "refactor: 抽取 VectorCasts 类型转换工具供 ALTER TYPE 复用"
```

---

### Task 2: 抽取 ConstraintChecker 约束校验工具

把 `MiniDbModify` 的约束校验抽成可复用静态工具,ALTER ADD CONSTRAINT / SET NOT NULL 共用。

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/ConstraintChecker.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbModify.java`(validateInsert/validateUnique/validateForeignKeys/columnIndexes/keyOf)

**Interfaces:**
- Consumes: `ExecContext.getTable(schema, table)`、`SimpleTable.scan()`、`TableSchema`、`ForeignKey`、`ArrowTypes`。
- Produces(全部 static):
  - `void validateInsert(ExecContext ctx, SimpleTable target, VectorSchemaRoot batch)`
  - `void validateUnique(SimpleTable target, VectorSchemaRoot batch, List<String> columns, String constraintName)`
  - `void validateForeignKeys(ExecContext ctx, SimpleTable target, VectorSchemaRoot batch)`
  - `void validateTableSatisfies(ExecContext ctx, SimpleTable table, TableSchema proposed)` —— 新增:对**整表已有数据**校验 proposed 约束(主键/唯一冲突、外键引用存在、NOT NULL 无 NULL)。ALTER ADD CONSTRAINT 用。实现:把 `table.scan()` 各批逐批喂给 `validateInsert`(复用同一套校验)。

- [ ] **Step 1: 跑现有约束测试建立基线**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=ConstraintTest,ForeignKeyTest`
Expected: 全绿。

- [ ] **Step 2: 新建 ConstraintChecker.java**

把 `MiniDbModify` 的 `validateInsert`/`validateUnique`/`validateForeignKeys`/`columnIndexes`/`keyOf` 平移为 static 方法(逻辑逐行不变)。新增 `validateTableSatisfies(ExecContext ctx, SimpleTable table, TableSchema proposed)`:遍历 `table.scan()` 的每个 batch,调 `validateInsert(ctx, proposedTableWrapper, batch)` —— 用一个临时 `SimpleTable`?不,`validateInsert` 需要 `SimpleTable` 拿 schema。改为签名 `validateInsert(ExecContext ctx, TableSchema schema, SimpleTable targetForScan, VectorSchemaRoot batch)` 或拆分:核心校验按 `TableSchema` + 一个「读全表行」的回调。最简做法:`validateInsert(ExecContext ctx, TableSchema schema, SimpleTable target, VectorSchemaRoot batch)`,`target` 仅用于 `scan()` 读现有行,`schema` 用于约束定义与 `columnIndex`。`MiniDbModify` 调用时两者同源(`target.schema()`);`validateTableSatisfies` 用 `proposed` 作 schema、`table`(旧数据)作 target。

- [ ] **Step 3: 改 MiniDbModify 调用工具**

`validateInsert` 调 `ConstraintChecker.validateInsert(ctx, target.schema(), target, batch)`;删掉 `MiniDbModify` 内联的 `validateUnique`/`validateForeignKeys`/`columnIndexes`/`keyOf`(保留 `validateDeleteRestrict`/`rewriteTable` 等无关方法)。`keyOf` 被 `validateDeleteRestrict`/`rowKey` 也用,需确认:若 `validateDeleteRestrict` 用 `keyOf`,则 `keyOf` 保留为 `ConstraintChecker` 的 public static 供两者调。

- [ ] **Step 4: 跑回归测试确认无漂移**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=ConstraintTest,ForeignKeyTest`
Expected: 全绿。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/ConstraintChecker.java minidb-server/src/main/java/com/minidb/server/plan/physical/MiniDbModify.java
git commit -m "refactor: 抽取 ConstraintChecker 约束校验工具供 ALTER 复用"
```

---

### Task 3: parser 层 SqlAlterTable 节点 + codegen

**Files:**
- Create: `minidb-parser/src/main/java/com/minidb/parser/ddl/SqlAlterTable.java`
- Modify: `minidb-parser/src/main/codegen/config.fmpp`(imports/keywords/nonReservedKeywordsToAdd/alterStatementParserMethods)
- Modify: `minidb-parser/src/main/codegen/includes/parserImpls.ftl`(加 `SqlAlterTable(...)` 方法)

**Interfaces:**
- Consumes: 现有 `SqlForeignKeyConstraint`、`SqlDdlNodes`、`DataType()`、`NullableOptDefaultTrue()`、`Literal()`、`CompoundIdentifier()`、`ParenthesizedSimpleIdentifierList()`。
- Produces: `SqlAlterTable` 节点,暴露 `AlterKind kind()`、`SqlIdentifier table()`、各操作字段的 getter(列名 `SqlIdentifier`、`SqlDataTypeSpec` 类型、`SqlNode` default 值、约束字段)。

- [ ] **Step 1: 新建 SqlAlterTable.java**

`extends SqlCall`,`SqlSpecialOperator("ALTER TABLE", SqlKind.OTHER)`。`enum AlterKind { ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, RENAME_TABLE, ALTER_TYPE, SET_NOT_NULL, DROP_NOT_NULL, ADD_CONSTRAINT, DROP_CONSTRAINT }`。字段:`AlterKind kind`、`SqlIdentifier table`、`SqlIdentifier column`(ADD/DROP/RENAME/ALTER 用)、`SqlIdentifier newColumn`(RENAME_COLUMN 目标)、`SqlIdentifier newTable`(RENAME_TABLE 目标)、`SqlDataTypeSpec dataType`(ADD_COLUMN 类型)、`SqlNode defaultExpr`(ADD_COLUMN DEFAULT,可 null)、`Boolean nullable`(ADD_COLUMN NOT NULL:null/TRUE 可空、FALSE 非空)、`SqlIdentifier constraintName`(ADD/DROP CONSTRAINT,可 null)、`SqlKind constraintKind`(ADD_CONSTRAINT 的 PRIMARY_KEY/UNIQUE/OTHER[外键])、`SqlNodeList columns`(ADD_CONSTRAINT 本表列)、`SqlIdentifier refTable`/`SqlNodeList refColumns`(外键引用)。`getOperandList()` 返回全部字段(顺序与构造一致),`unparse` 按 kind 输出。仿 `SqlForeignKeyConstraint` 的写法。

- [ ] **Step 2: 改 config.fmpp**

`imports` 加 `"com.minidb.parser.ddl.SqlAlterTable"`;`keywords` 加 `"ALTER"`;`nonReservedKeywordsToAdd` 加 `"ADD" "DROP" "RENAME" "COLUMN" "CONSTRAINT" "PRIMARY" "UNIQUE" "FOREIGN" "NOT" "NULL" "DEFAULT" "TO" "SET" "DATA" "TYPE"`(已存在则跳过,避免重复);加 `alterStatementParserMethods: ["SqlAlterTable"]`。

- [ ] **Step 3: 在 parserImpls.ftl 加 SqlAlterTable 解析**

方法签名 `SqlAlter SqlAlterTable(SqlParserPos pos, String scope)`(与 `SqlCreateTable` 的 Span 风格一致,但 `alterStatementParserMethods` 的方法签名约定是 `(SqlParserPos pos, String scope)`)。解析:

```javacc
SqlAlter SqlAlterTable(SqlParserPos pos, String scope) :
{
    SqlIdentifier table;
    SqlIdentifier column = null, newName = null, refTable = null, constraintName = null;
    SqlDataTypeSpec dataType = null;
    SqlNode defaultExpr = null;
    Boolean nullable = null;
    SqlKind constraintKind = null;
    SqlNodeList columns = null, refColumns = null;
    final Span s;
}
{
    <TABLE> { s = span(); } table = CompoundIdentifier()
    (
        <ADD> [ <COLUMN> ] column = SimpleIdentifier() dataType = DataType()
            [ nullable = NullableOpt() ] [ <DEFAULT_> defaultExpr = Literal() ]
            { return new SqlAlterTable(s.end(this), SqlAlterTable.AlterKind.ADD_COLUMN, table, column, null, null, dataType, defaultExpr, nullable, null, null, null, null, null); }
    |
        <DROP> [ <COLUMN> ] column = SimpleIdentifier()
            { return new SqlAlterTable(... DROP_COLUMN ...); }
    |
        <RENAME> [ <COLUMN> ] column = SimpleIdentifier() <TO> newName = SimpleIdentifier()
            { return new SqlAlterTable(... RENAME_COLUMN, newColumn=newName ...); }
    |
        <RENAME> <TO> newName = CompoundIdentifier()
            { return new SqlAlterTable(... RENAME_TABLE, newTable=newName ...); }
    |
        <ALTER> [ <COLUMN> ] column = SimpleIdentifier()
            (
                <SET> <DATA> <TYPE> dataType = DataType() { return ... ALTER_TYPE ...; }
            |
                <SET> <NOT> <NULL> { return ... SET_NOT_NULL, nullable=FALSE ...; }
            |
                <DROP> <NOT> <NULL> { return ... DROP_NOT_NULL ...; }
            )
    |
        <ADD> [ <CONSTRAINT> constraintName = SimpleIdentifier() ]
            (
                <PRIMARY> <KEY> columns = ParenthesizedSimpleIdentifierList() { constraintKind = SqlKind.PRIMARY_KEY; }
            |
                <UNIQUE> columns = ParenthesizedSimpleIdentifierList() { constraintKind = SqlKind.UNIQUE; }
            |
                <FOREIGN> <KEY> columns = ParenthesizedSimpleIdentifierList()
                    <REFERENCES> refTable = CompoundIdentifier() [ refColumns = ParenthesizedSimpleIdentifierList() ]
                    { constraintKind = SqlKind.OTHER; }
            )
            { return ... ADD_CONSTRAINT ...; }
    |
        <DROP> <CONSTRAINT> constraintName = SimpleIdentifier() { return ... DROP_CONSTRAINT ...; }
    |
        <DROP> <PRIMARY> <KEY> { return ... DROP_CONSTRAINT, constraintKind=PRIMARY_KEY, name=null ...; }
    )
}
```

注:`NullableOpt()` 需在 parserImpls.ftl 新增(返回 Boolean,`<NOT> <NULL>` → FALSE,默认 TRUE);若已有等价方法则复用。`DEFAULT_` 是 Calcite 模板里 DEFAULT 关键字的 token 名(TableElement 里用了 `<DEFAULT_>`),沿用。

- [ ] **Step 4: 编译 + 写解析冒烟测试**

新增 `minidb-server/src/test/java/com/minidb/server/exec/AlterTableParseTest.java`(setup 同 SchemaDdlTest):对每条语法 `executor.execute("ALTER TABLE ...")` 断言不抛 `ParseException`(会抛「table not found」的 `IllegalArgumentException` 说明已过 parse 关,可接受)。先只验证 9 类语法都能被 parse(用 `assertThrows(IllegalArgumentException.class, ...)` 或直接 `executor.execute` 后忽略未实现异常)。

Run: `./mvnw.cmd test -pl minidb-server -Dtest=AlterTableParseTest`
Expected: 全绿(parse 通过,执行阶段因未实现会抛异常,测试只断言 parse 阶段)。

- [ ] **Step 5: 提交**

```bash
git add minidb-parser/src/main/java/com/minidb/parser/ddl/SqlAlterTable.java minidb-parser/src/main/codegen/config.fmpp minidb-parser/src/main/codegen/includes/parserImpls.ftl minidb-server/src/test/java/com/minidb/server/exec/AlterTableParseTest.java
git commit -m "feat: parser 支持 ALTER TABLE 语法(SqlAlterTable 节点)"
```

---

### Task 4: catalog 层 alterTable / renameTable

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java`

**Interfaces:**
- Produces:
  - `void alterTable(String schemaName, String tableName, TableSchema newSchema)` —— 校验 schema/table 存在、新 schema 同名(内部一致性),`schemas.get(sk).put(tk, newSchema)`,`notifyChange()`。
  - `void renameTable(String schemaName, String oldName, String newName)` —— 校验旧表存在、新名不与现有表/视图冲突;取旧 `TableSchema` 用 `new TableSchema(schemaName, newName, cols, pk, uk, fk, format)` 构造新副本,put 新、remove 旧,`notifyChange()`。

- [ ] **Step 1: 写失败测试**

在 `minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogTest.java` 加两个测试:`alterTableReplacesSchema`(createTable 后 alterTable 换列,断言 `getTable(...).columns()` 变了)、`renameTableMovesEntry`(createTable 后 renameTable,断言旧名 `hasTable` false、新名 true 且 schemaName 不变)。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbCatalogTest`
Expected: FAIL(方法不存在)。

- [ ] **Step 3: 实现 alterTable / renameTable**

按 Interfaces 描述实现。`renameTable` 复用 `hasView` 检查新名不撞视图(与 `createTable` 一致)。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbCatalogTest`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/catalog/MiniDbCatalog.java minidb-server/src/test/java/com/minidb/server/catalog/MiniDbCatalogTest.java
git commit -m "feat: MiniDbCatalog 支持 alterTable/renameTable"
```

---

### Task 5: storage 层 alterTable / renameTable

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java`

**Interfaces:**
- Consumes: `MiniDbCatalog.alterTable`/`renameTable`(Task 4)、`SimpleTable`、`tableStorage.tableDir`/`delete`。
- Produces:
  - `void alterTable(String schemaName, String tableName, TableSchema newSchema)` —— 元数据重建:删旧 `SimpleTable`(map remove)、`catalog.alterTable(...)`、新建 `SimpleTable(newSchema, ...)` put 回 map。**不动数据 part**。
  - `void renameTable(String schemaName, String oldName, String newName)` —— `Files.move` 表目录 → `catalog.renameTable(...)` → map remove 旧 key、按新名重建 `SimpleTable`。

- [ ] **Step 1: 写失败测试**

在 `minidb-server/src/test/java/com/minidb/server/storage/StorageManagerTest.java` 加:`alterTableRebuildsSimpleTable`(createTable 后 alterTable 换 schema,断言 `storage.getTable(...).schema().columns()` 变了且旧数据 part 仍在)、`renameTableMovesDir`(rename 后断言新目录存在、旧目录不存在、`getTable(new).schema().name()` 正确)。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StorageManagerTest`
Expected: FAIL。

- [ ] **Step 3: 实现**

`alterTable`:按 `storageKey` 取旧 `SimpleTable`,非空则 `tables.remove(sk)`,`catalog.alterTable(schemaName, tableName, newSchema)`,`tables.put(sk, new SimpleTable(newSchema, allocator, tableStorage.tableDir(...), formatFor(newSchema)))`。`renameTable`:`SimpleTable old = tables.remove(oldKey)`(判空报错),`catalog.renameTable(...)`,`Files.move(oldDir, newDir)`,`tables.put(newKey, new SimpleTable(...))`,异常时把旧表放回 map(回滚)。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StorageManagerTest`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java minidb-server/src/test/java/com/minidb/server/storage/StorageManagerTest.java
git commit -m "feat: StorageManager 支持 alterTable/renameTable"
```

---

### Task 6: AlterTableHandler 执行语义 + QueryExecutor 分发

把 ALTER 语义(构造新 schema、数据重写、约束校验、调 storage)收敛到一个外挂类。

**Files:**
- Create: `minidb-server/src/main/java/com/minidb/server/exec/AlterTableHandler.java`
- Modify: `minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java`(handleDdl 加分支)

**Interfaces:**
- Consumes: `SqlAlterTable`(Task 3)、`MiniDbCatalog.alterTable/renameTable`(Task 4)、`StorageManager.alterTable/renameTable`(Task 5)、`VectorCasts.cast`(Task 1)、`ConstraintChecker`(Task 2)、`RowCopier.copyRow/writeValue`、`ArrowTypes.fromSqlTypeName`、`SqlBasicTypeNameSpec`。
- Produces: `class AlterTableHandler { AlterTableHandler(StorageManager storage, BufferAllocator allocator); QueryResult handle(SqlAlterTable alter, String currentSchema); }`

- [ ] **Step 1: 写失败测试(核心集成)**

新建 `minidb-server/src/test/java/com/minidb/server/exec/AlterTableTest.java`(setup 同 SchemaDdlTest)。首批测试:ADD_COLUMN 可空(已有行 NULL)、ADD_COLUMN DEFAULT、DROP_COLUMN 删列、RENAME_COLUMN、RENAME_TABLE、ALTER_TYPE INT→BIGINT、SET/DROP NOT NULL、ADD/DROP PRIMARY KEY/UNIQUE/FOREIGN KEY、各报错路径。断言用 `((QueryResult.Rows) executor.execute("SELECT ...")).data()` 或 `catalog.getTable(...)`。

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=AlterTableTest`
Expected: FAIL(`QueryExecutor.handleDdl` 抛 unsupported DDL)。

- [ ] **Step 3: 实现 AlterTableHandler.handle**

限定名分解(同 `handleCreate`)→ 取旧 `TableSchema` → 按 `alter.kind()` 构造新 `TableSchema`:

- ADD_COLUMN:新列 `ColumnMeta(name, type, precision, scale, nullable)`(DECIMAL 取 precision/scale,其余 -1),append 到 columns。结构类重写:读旧表全量 → 每批建新 root → 前 N 列 `RowCopier.copyRow`,新列填 DEFAULT(用 `VectorCasts` 把常量字面量转成向量?更简单:直接按常量值逐行 `writeValue` 或填 NULL)。删旧 part → `storage.alterTable(...)` 重建 → 写新 part。
- DROP_COLUMN:校验列不参与主键/唯一/外键/被引用列(否则报错)→ 从 columns 移除 + 从主键/唯一/外键列表移除(因已校验不在其中,实际只移除 columns)→ 结构类重写(跳过被删列 copy)。
- RENAME_COLUMN:校验列不参与约束 → 构造 columns 里改名副本 → `storage.alterTable`(元数据,不重写)。
- RENAME_TABLE:→ `storage.renameTable`(元数据)。
- ALTER_TYPE:校验列不参与约束 → 构造 columns 里改类型副本 → 结构类重写(该列用 `VectorCasts.cast` 转换,其余 copy)。
- SET_NOT_NULL / DROP_NOT_NULL:构造 columns 里 nullable 改动副本 → `storage.alterTable`(元数据);SET_NOT_NULL 前用 `ConstraintChecker.validateTableSatisfies` 验无 NULL。
- ADD_CONSTRAINT:构造新主键/唯一/外键列表 → 先 `ConstraintChecker.validateTableSatisfies` 验已有数据满足 → `storage.alterTable`(元数据)。
- DROP_CONSTRAINT:按名字/主键移除对应约束 → `storage.alterTable`。

数据重写 helper 私有方法:`rewriteWithColumnTransform(SimpleTable old, TableSchema newSchema, ...)` 读旧全量、逐行建新 batch、`old.clearParts()`、`storage.alterTable`、`getTable(new)` 写 part。

- [ ] **Step 4: QueryExecutor.handleDdl 加分支**

```java
if (ddl instanceof SqlAlterTable alter) {
    return new AlterTableHandler(storage, allocator).handle(alter, currentSchema);
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=AlterTableTest`
Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/exec/AlterTableHandler.java minidb-server/src/main/java/com/minidb/server/exec/QueryExecutor.java minidb-server/src/test/java/com/minidb/server/exec/AlterTableTest.java
git commit -m "feat: 实现 ALTER TABLE 执行(列增删/改名/改类型/约束)"
```

---

### Task 7: JDBC 元数据

**Files:**
- Modify: `minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbDatabaseMetaData.java:216-223`

- [ ] **Step 1: 改返回值**

`supportsAlterTableWithAddColumn()`、`supportsAlterTableWithDropColumn()` 改 `return true;`。

- [ ] **Step 2: 编译**

Run: `./mvnw.cmd -pl minidb-jdbc -am compile -q`
Expected: BUILD SUCCESS。

- [ ] **Step 3: 提交**

```bash
git add minidb-jdbc/src/main/java/com/minidb/jdbc/MiniDbDatabaseMetaData.java
git commit -m "feat: JDBC 元数据声明支持 ALTER TABLE 增删列"
```

---

### Task 8: 集成测试 + 全量回归

**Files:**
- Create: `minidb-integration-tests/src/test/java/com/minidb/jdbc/AlterTableTest.java`(走真实 JDBC 连接,参考 `PersistenceTest` 的 server 启动模式)

- [ ] **Step 1: 写端到端测试**

覆盖:ADD COLUMN 后经 JDBC 查询可见新列、DROP COLUMN 后元数据列数变、RENAME COLUMN 后 `getColumns` 返回新名、ALTER TYPE 后结果类型变、约束 add/drop 后违规 INSERT 报错、ALTER 后重启(重建 server)结构持久。

- [ ] **Step 2: 跑集成测试**

Run: `./mvnw.cmd test -pl minidb-integration-tests`
Expected: PASS。

- [ ] **Step 3: 全量回归**

Run: `./mvnw.cmd test`
Expected: 全绿(重点看 minidb-server 的 CAST/约束/DDL 相关测试无回归)。

- [ ] **Step 4: 提交**

```bash
git add minidb-integration-tests/src/test/java/com/minidb/jdbc/AlterTableTest.java
git commit -m "test: ALTER TABLE 端到端测试"
```

---

## 完成定义

- 9 类 ALTER 语法可解析并执行;结构类操作重写 part、元数据类操作零重写。
- 类型转换与约束校验与 DML 里的 CAST/INSERT 校验行为一致(单一来源,无漂移)。
- 约束列上的结构变更严格报错;ADD CONSTRAINT / SET NOT NULL 对已有数据校验。
- JDBC 元数据声明支持;端到端测试 + 全量测试绿。
