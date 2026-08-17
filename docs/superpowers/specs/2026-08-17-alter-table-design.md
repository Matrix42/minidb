# ALTER TABLE 设计

日期: 2026-08-17
状态: 已确认,待实现
范围: 支持 `ALTER TABLE` 的列增删、改名、改类型、约束变更

## 目标

MiniDB 目前只有 CREATE/DROP/TRUNCATE 三类 DDL,`QueryExecutor.handleDdl` 无 `SqlAlter` 分支,parser 的 `config.fmpp` 也未注册 `alterStatementParserMethods`。JDBC 元数据 `supportsAlterTableWithAddColumn/DropColumn` 硬编码 `false`。

本设计引入 `ALTER TABLE`,覆盖四类操作:

- **列增删**: `ADD [COLUMN]`、`DROP [COLUMN]`
- **改名**: `RENAME [COLUMN] ... TO`、`RENAME TO`(改表名)
- **改类型**: `ALTER [COLUMN] ... SET DATA TYPE`
- **约束变更**: `ADD [CONSTRAINT] PRIMARY KEY / UNIQUE / FOREIGN KEY`、`DROP CONSTRAINT`、`ALTER [COLUMN] ... SET/DROP NOT NULL`

## 关键决策(已与用户确认)

1. **类型转换复用 CAST 语义**: `ALTER COLUMN ... SET DATA TYPE` 的逐行值转换与 DML 里的 `CAST` 严格一致(数值↔字符串互转、失败报错)。为此把 `RexInterpreter.evalCast` 的转换核心抽成单一来源工具,避免两处漂移(坑 53 的教训)。
2. **ADD COLUMN 支持 DEFAULT 常量 + NOT NULL**: 无 DEFAULT 时新列必须可空、已有行填 NULL;`NOT NULL` 无 DEFAULT 报错。DEFAULT 只支持**常量字面量**(数字/字符串/布尔/NULL),不支持函数/列引用表达式。
3. **约束联动严格报错**: `DROP COLUMN` / `RENAME COLUMN` / `ALTER TYPE` 的目标列若出现在主键、唯一键、外键(本表侧或引用侧)中 → 报错,要求先 `DROP CONSTRAINT`。规则统一、可预期、不静默丢约束。
4. **视图不追踪**: `RENAME COLUMN` / `RENAME TABLE` / `DROP COLUMN` 后依赖旧名/旧列的视图定义会失效(下次查询报「列/表不存在」),与现有 `DROP TABLE` 不检查视图依赖的行为一致。视图依赖追踪是独立功能,不在本范围。
5. **单操作**: 一条 `ALTER TABLE` 只允许一个操作(不支持 `ADD a, ADD b` 逗号连写),大幅简化 parser 与语义。

## 数据重写策略(核心)

按「是否改变列布局」分两类:

- **纯元数据类(零数据重写)**: `RENAME COLUMN`、`RENAME TABLE`。part 文件读写都按**列索引**访问(`ArrowPartFormat.read` 的 `copyFromSafe`、`ParquetPartFormat.read` 的 `getField(c)` 都是按 index,不按列名),列名/表名不参与解码;表名只体现在目录名 `data/<schema>/<table>/`。故这两类只需替换 catalog 里的 `TableSchema` + 重建 `SimpleTable`(新 `arrowSchema`);`RENAME TABLE` 额外 `Files.move` 目录。
- **结构类(重写 part 文件)**: `ADD COLUMN` / `DROP COLUMN` / `ALTER TYPE`。列数或类型变了,旧 part 按新 `arrowSchema` 解码会越界/漏列。必须**扫描旧 part → 逐行转换 → 写新 part → 删旧**,复用 `MiniDbModify.rewriteTable` 已验证的「读旧重建写新」模式(其结尾 `clearParts()` + `writePart(nb)` 无崩溃保护,与 UPDATE/DELETE 一致,首版沿用)。

## 分层设计

### 1. minidb-parser(parser 层)

- 新增节点类 `com.minidb.parser.ddl.SqlAlterTable extends SqlCall`,内部 `enum AlterKind`(ADD_COLUMN / DROP_COLUMN / RENAME_COLUMN / RENAME_TABLE / ALTER_TYPE / SET_NOT_NULL / DROP_NOT_NULL / ADD_CONSTRAINT / DROP_CONSTRAINT)+ 各操作所需字段。仿现有 `SqlForeignKeyConstraint` / `SqlTableOptions` 的 `SqlSpecialOperator` + `getOperandList` + `unparse` 模式。
- `config.fmpp` 加 `alterStatementParserMethods: ["SqlAlterTable"]`(当前该列表为空,只有 `SET OPTION` 分支)。
- `parserImpls.ftl` 加 `SqlAlterTable(SqlParserPos pos, String scope)` 解析方法,语法:

```sql
ALTER TABLE t ADD [COLUMN] c INT [NOT NULL] [DEFAULT 42]
ALTER TABLE t DROP [COLUMN] c
ALTER TABLE t RENAME [COLUMN] c TO c2
ALTER TABLE t RENAME TO t2
ALTER TABLE t ALTER [COLUMN] c SET DATA TYPE BIGINT
ALTER TABLE t ALTER [COLUMN] c SET NOT NULL
ALTER TABLE t ALTER [COLUMN] c DROP NOT NULL
ALTER TABLE t ADD [CONSTRAINT name] PRIMARY KEY (c) | UNIQUE (c) | FOREIGN KEY (c) REFERENCES r(rc)
ALTER TABLE t DROP CONSTRAINT name
ALTER TABLE t DROP PRIMARY KEY
```

- 关键点: `ALTER`、`ADD`、`DROP`、`RENAME`、`COLUMN`、`CONSTRAINT`、`PRIMARY`、`UNIQUE`、`FOREIGN`、`NOT`、`NULL`、`DEFAULT`、`TO`、`SET`、`DATA`、`TYPE` 需加入 `config.fmpp` 的 keywords/nonReservedKeywords(避免与列名/表名冲突);`DEFAULT` 值用 `Literal()` 解析(仅常量),`SET DATA TYPE` 用 `DataType()`(复用列类型解析,含 DECIMAL precision/scale)。

### 2. minidb-storage-common(类型转换工具)

- 新增 `com.minidb.storage.common.VectorCasts`,静态工具:
  - `static FieldVector cast(ValueVector src, ColumnType target, int precision, int scale, BufferAllocator allocator)` — 核心:按 `target` 建目标向量(`ArrowTypes.field(new ColumnMeta(...)).createVector(allocator)`),逐行读源值转目标类型,NULL 透传。目标类型覆盖 SMALLINT/INTEGER/BIGINT/REAL/FLOAT/DOUBLE/DECIMAL/VARCHAR/CHAR/BOOLEAN/DATE/TIME/TIMESTAMP/BINARY/VARBINARY(即现有 `evalCast` 的 switch 全集)。
  - 私有源值读取 helper `asLong/asDouble/asString/asBoolean` — 从 `RexInterpreter` 平移(处理 SmallInt/Int/BigInt/Float4/Float8/Decimal/Bit/VarChar 各源类型)。
- 这是本次唯一触碰「稳定核心」的改动: `RexInterpreter.evalCast`(现 547–725 行)改为「确定目标 ColumnType + precision/scale → 调 `VectorCasts.cast`」,删掉内联的 switch 与 asLong/asDouble/asString/asBoolean(移入 VectorCasts)。**必须先跑通现有 CAST/字符串/数学函数测试再重构**(TDD,防回归)。

### 3. minidb-server:catalog 层

`MiniDbCatalog` 加两个方法(替换不可变 `TableSchema`,触发 `notifyChange` 落 catalog.json):

- `void alterTable(String schemaName, String tableName, TableSchema newSchema)` — 校验目标表存在后 `schemas.get(sk).put(tk, newSchema)`。
- `void renameTable(String schemaName, String oldName, String newName)` — 校验新名不冲突后,取旧 `TableSchema` 构造新名副本替换、删旧条目(或直接 `alterTable`)。

### 4. minidb-server:storage 层

`StorageManager` 加 `void alterTable(String schemaName, String tableName, TableSchema newSchema, AlterPlan plan)`,其中 `AlterPlan` 是 server 侧描述「怎么从旧数据变新数据」的对象(结构类带列映射/新列默认值,元数据类为空)。职责编排:

1. 结构类:用旧 `SimpleTable.scan()` 读全量 → 按 plan 逐行转换到新 schema(复用 `RowCopier` + `VectorCasts`)→ 删旧 part → 替换 catalog + `tables` map 重建 `SimpleTable` → 写新 part。
2. 元数据类:`RENAME COLUMN` 直接替换 catalog + 重建 `SimpleTable`(数据 part 不动);`RENAME TABLE` 额外 `Files.move` 表目录。

`SimpleTable` 本身**不变**(纯存储抽象,不懂列语义);转换逻辑放在 server 侧(QueryExecutor 内的 `handleAlter` 方法,或独立 `AlterTableHandler`)。

### 5. minidb-server:exec 层(QueryExecutor)

- `handleDdl` 加 `if (ddl instanceof SqlAlterTable alter) return handleAlter(alter, currentSchema);`。
- `handleAlter`:解析 schema/table 名(与 `handleCreate` 相同的限定名分解)→ 取旧 `TableSchema` → 按 `AlterKind` 构造新 `TableSchema`(不可变,new 一个)→ 约束严格报错校验 → 调 `storage.alterTable(...)`。
- **约束校验**: 加主键/唯一 → 扫描已有行验冲突;加外键 → 验引用存在;`SET NOT NULL` → 验无 NULL。这些校验逻辑现在内联在 `MiniDbModify.validateUnique/validateForeignKeys`(private 方法),需**提取成可复用工具**(如 `exec/ConstraintChecker`),`MiniDbModify` 与 `handleAlter` 共用,避免两处漂移。

### 6. minidb-jdbc(元数据)

- `MiniDbDatabaseMetaData.supportsAlterTableWithAddColumn()` / `supportsAlterTableWithDropColumn()` 改返回 `true`。

## 数据流

```
ALTER TABLE t ADD c INT DEFAULT 42
  → calcite.parse → SqlAlterTable(ADD_COLUMN, c, INT, DEFAULT 42)
  → QueryExecutor.handleDdl → handleAlter
  → 取旧 TableSchema → 构造新 TableSchema(加列)
  → 约束校验(如涉及约束则报错)
  → storage.alterTable(schema, table, newSchema, plan)
      → 旧 SimpleTable.scan() 全量读
      → 逐行转换(旧列 copy + 新列填 DEFAULT/NULL)
      → clearParts() → 重建 SimpleTable → writePart()
  → catalog.alterTable → notifyChange → 落 catalog.json
```

## 测试(minidb-integration-tests)

- **ADD COLUMN**: 加可空列(已有行 NULL)、加 DEFAULT 常量列(已有行填默认值)、加 NOT NULL 无 DEFAULT 报错、加列后新 INSERT 正常。
- **DROP COLUMN**: 删列后数据仍正确、删不存在的列报错、删约束列(主键/唯一/外键/被引用列)报错。
- **RENAME COLUMN**: 改名列数据仍在、查询用新名、旧名失效。
- **RENAME TABLE**: 改表名数据仍在、旧名失效、目录迁移。
- **ALTER TYPE**: INT→BIGINT/DOUBLE/VARCHAR 转换正确、`'abc'`→INT 报错、改约束列类型报错。
- **约束**: `ADD PRIMARY KEY`/`UNIQUE` 对已有重复数据报错、对唯一数据成功;`ADD FOREIGN KEY` 对缺失引用报错;`SET NOT NULL` 对含 NULL 报错;`DROP CONSTRAINT` 后约束消失;`SET/DROP NOT NULL` 生效。
- **元数据**: `supportsAlterTableWithAddColumn/DropColumn` 返回 true。
- **持久化**: ALTER 后重启(重建 StorageManager/catalog)结构仍正确。

## 不在本范围

- 视图依赖追踪(RENAME/DROP 后视图失效,记为已知限制)。
- 多操作逗号连写(`ALTER TABLE t ADD a, ADD b`)。
- `ALTER COLUMN ... SET DEFAULT` / `DROP DEFAULT`(仅 ADD 时支持 DEFAULT)。
- 可更新视图(`INSERT INTO view`)、计算列/生成列。
- `RENAME CONSTRAINT`(只做 DROP + 重新 ADD)。
- 崩溃安全的数据重写(与现有 UPDATE/DELETE 一致,首版沿用 clearParts+writePart,不做交换目录)。
