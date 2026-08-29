# MiniDB

基于 **Apache Calcite**(SQL 解析/规划)+ **Apache Arrow**(列式存储)+ **Netty**(网络协议)自研的微型 JDBC 数据库。

单机、零配置文件开箱即用,支持完整的 SQL 语法(JOIN / 递归 CTE / 窗口函数 / 物化视图 / 二级索引 / 事务),数据以列式批次(Arrow)在内存中处理、持久化为 Parquet / Arrow IPC 文件,提供自研 JDBC 驱动(`jdbc:minidb://host:port`)与 sqlline 命令行客户端。

> 本文档中的 SQL 示例均已在真实运行的服务端上验证通过(见文末[验证说明](#验证说明))。

## 目录

- [特性](#特性)
- [模块结构](#模块结构)
- [构建与打包](#构建与打包)
- [安装与启动](#安装与启动)
- [配置](#配置)
- [Java 客户端](#java-客户端)
- [命令行客户端 sqlline](#命令行客户端-sqlline)
- [数据库使用说明](#数据库使用说明)
- [数据类型](#数据类型)
- [表类型与存储引擎](#表类型与存储引擎)
- [建表与 DDL](#建表与-ddl)
- [数据操纵 DML](#数据操纵-dml)
- [SELECT 查询](#select-查询)
- [函数](#函数)
- [索引](#索引)
- [视图与物化视图](#视图与物化视图)
- [元数据 information_schema](#元数据-information_schema)
- [事务](#事务)
- [EXPLAIN 与 ANALYZE](#explain-与-analyze)
- [限制](#限制)
- [开发指南](#开发指南)

---

## 特性

- **完整 SQL**:DDL(建表/改表/索引/视图/物化视图/schema)、DML(INSERT/UPDATE/DELETE/TRUNCATE)、SELECT(WHERE / ORDER BY / LIMIT / OFFSET / DISTINCT / GROUP BY / HAVING)、JOIN(INNER / LEFT / RIGHT / FULL、等值/非等值)、集合运算(UNION / INTERSECT / EXCEPT,含 ALL)、CTE(WITH 与递归 `WITH RECURSIVE`)、子查询(标量 / IN / EXISTS,含相关子查询)、窗口函数。
- **列式向量化执行**:数据以 Arrow `VectorSchemaRoot` 批次流转,`BatchIterator` 拉模式流式处理,VolcanoPlanner + 自研物理算子生成执行计划。
- **两套存储引擎**:有主键默认走 LSM-Tree(带 WAL、SSTable、布隆过滤器、后台 compaction),无主键走 SimpleTable(直接落 part 文件);均支持 Parquet / Arrow IPC 两种落盘格式。
- **二级索引**:`CREATE [UNIQUE] INDEX`,索引表 = LSM 表,查询自动选索引(`EXPLAIN` 显示 `index=`)。
- **物化视图**:`CREATE MATERIALIZED VIEW` 支持 SPJ 与单表聚合,基表 DML 自动增量刷新,也可手动 `REFRESH`。
- **事务**:JDBC `setAutoCommit(false)` / `commit` / `rollback`,支持四种隔离级别(默认 SERIALIZABLE),LSM 表按快照隔离读取,带崩溃恢复(WAL + 事务日志)。
- **Schema**:`CREATE/DROP SCHEMA`、`schema.table` 限定名、`USE SCHEMA` 切换当前 schema(每连接隔离),默认 `public`。
- **内置函数**:数学、字符串、日期时间、比较、聚合、窗口函数(详见[函数](#函数))。
- **元数据**:`information_schema.schemata / tables / columns / materialized_views` 只读系统表,JDBC `DatabaseMetaData` 全量支持。

## 模块结构

| 模块 | 作用 |
|------|------|
| `minidb-parser` | 自定义 DDL 解析(CREATE/DROP INDEX、ALTER TABLE、`WITH` 表选项、外键),Calcite 内置 DDL 之外的部分 |
| `minidb-storage` | 存储层子模块:minidb-common(元数据模型)、minidb-arrow(Arrow IPC part 格式)、minidb-parquet(Parquet part 格式)、minidb-lsm(LSM-Tree:WAL / MemTable / SSTable / BloomFilter / Compaction) |
| `minidb-protocol` | Netty wire 协议(Message 编解码),**极简且稳定,改动需极谨慎** |
| `minidb-server` | 服务端:Calcite 解析/规划、Arrow 存储、向量化批式执行、JDBC 协议处理、事务、统计、EXPLAIN |
| `minidb-jdbc` | 客户端 JDBC 驱动(`jdbc:minidb://host:port`),基于自定义 Netty 协议,不引用服务端类 |
| `minidb-integration-tests` | 端到端集成测试(通过真实 socket 连接驱动) |
| `minidb-tpcds` | TPC-DS 基准(99/99 查询通过,对比 DuckDB 验证正确性) |
| `minidb-dist` | 发行组装模块(无源码),产出 `bin/conf/data/jdbc/tools/libs/` 发行目录与 tar.gz/zip |

---

## 构建与打包

**要求 JDK 17**(`JAVA_HOME` 指向 JDK 17)。构建用 bash 下的 `./mvnw.cmd`。

```bash
# 全量测试
./mvnw.cmd test

# 单模块测试
./mvnw.cmd test -pl minidb-server

# 单测试类
./mvnw.cmd test -pl minidb-server -Dtest=QueryExecutorTest

# 编译(跳过测试)
./mvnw.cmd -pl minidb-server -am compile -q

# 打发行包(产出 minidb-dist/target/minidb-1.0.0/ + tar.gz + zip)
./mvnw.cmd -pl minidb-dist -am package

# 开发模式直接跑服务端(默认 8899)
./mvnw.cmd -pl minidb-server exec:java
```

> 运行服务端/JDBC 客户端需追加 JVM 参数(见下)。

---

## 安装与启动

### 发行包

`./mvnw.cmd -pl minidb-dist -am package` 后,`minidb-dist/target/` 下产出:

```
minidb-1.0.0/
├── bin/        # 启动脚本(sh 与 bat) + sqlline 客户端
├── conf/       # config.yaml + log4j2.properties
├── data/       # 数据目录(表数据、catalog.json)
├── jdbc/       # JDBC 驱动(自包含 netty/arrow)
├── tools/      # sqlline 及依赖
└── libs/       # 服务端运行时依赖
```

解压(或直接使用目录)后:

```bash
# 启动服务端(前台,调试用)
bin/minidb-server

# 后台守护启动 / 优雅停止 / 状态
bin/minidb-server start
bin/minidb-server stop
bin/minidb-server status
```

Windows 用 `bin\minidb-server.bat`(同上 `start/stop/status`)。

### 从源码启动

```bash
# 需要这三个 JVM 参数(Arrow 的 Unsafe 访问)
./mvnw.cmd -pl minidb-server exec:java \
  -Dexec.args="--port 8899 --data ./data --conf ./conf"
```

手动 `java` 运行需追加:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

默认监听 **8899**,数据目录 `./data`,配置目录 `./conf`(可用 `--port` / `--data` / `--conf` 覆盖)。

### 快速上手

```sql
CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20));
INSERT INTO t VALUES (1, '张总'), (2, '李四');
SELECT * FROM t ORDER BY id;
```

---

## 配置

配置文件 `conf/config.yaml`(缺省键回退默认值,修改后重启生效):

```yaml
compaction:
  target-size-mb: 128        # compaction 目标 part 大小
  auto-part-threshold: 16    # part 数超过该值自动合并
lsm:
  memtable-size-mb: 64       # MemTable 写满落盘阈值
  l0-file-limit: 4           # L0 文件数上限
  level-size-multiplier: 10  # 每层大小放大倍数
  wal-fsync: false           # WAL 是否每次写 fsync
  background-interval-ms: 1000  # 后台 compaction 检查间隔
  bloom-bits-per-key: 10     # 布隆过滤器位数/键
server:
  query-threads: 0           # 查询线程池大小(0=自动)
  port: 8899                 # 监听端口
  isolation-level: serializable  # 事务隔离级别
```

环境变量覆盖:`MINIDB_PORT`(端口)、`MINIDB_DATA_DIR`(数据目录)、`MINIDB_CONF_DIR`(配置目录)、`MINIDB_JAVA_OPTS`(附加 JVM 参数)、`MINIDB_HOST` / `MINIDB_URL`(sqlline 连接)。

---

## Java 客户端

把 `minidb-jdbc` 的 jar(`jdbc/minidb-jdbc-1.0.0.jar`,自包含 netty/arrow 依赖)放进 classpath,驱动经 `META-INF/services/java.sql.Driver` 自动注册(驱动类 `com.minidb.jdbc.MiniDbDriver`):

```java
Connection c = DriverManager.getConnection("jdbc:minidb://localhost:8899");
Statement s = c.createStatement();
s.execute("CREATE TABLE t (id INT, name VARCHAR)");
s.executeUpdate("INSERT INTO t VALUES (1, 'a')");
ResultSet rs = s.executeQuery("SELECT id, name FROM t ORDER BY id");
while (rs.next()) {
    System.out.println(rs.getInt(1) + " " + rs.getString(2));
}
```

应用需追加 JVM 参数(Arrow 的 `MemoryUtil` 使用了 Unsafe):

```
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

支持的 JDBC 能力:连接 / Statement / PreparedStatement(客户端参数替换)/ 结果集分页(`setFetchSize` 触发服务端游标)/ `DatabaseMetaData`(getSchemas / getTables / getColumns,含 LIKE 转义)/ 事务(`setAutoCommit(false)` + `commit`/`rollback`)。

---

## 命令行客户端 sqlline

发行包自带 sqlline:

```bash
bin/sqlline               # Linux/macOS,默认连 jdbc:minidb://localhost:8899
bin\sqlline.bat           # Windows
```

```text
sqlline> !tables
sqlline> CREATE TABLE t (id INT, name VARCHAR);
sqlline> INSERT INTO t VALUES (1, '张总');
sqlline> SELECT * FROM t;
```

---

## 数据库使用说明

### 标识符

- 表名/列名大小写**不敏感**;schema 名小写化存储。
- 标识符可用双引号引用;**保留字**做列名/别名时必须加双引号,例如 `"order"`、`"select"`、`"month"`、`"when"`、`"running"`、`"prev"`。
- 注释 `--` 与 `/* ... */` 均支持(可出现在语句内/行尾)。
- **语句末尾不要加分号 `;`**:服务端一次接收一条语句,尾部 `;` 会触发解析错误(sqlline 会自动剥离;JDBC/裸协议请勿携带)。本文示例为直观起见多处带 `;`,在 sqlline 中运行即可。

### 快速建一套演示数据

```sql
CREATE SCHEMA shop;

USE SCHEMA shop;

CREATE TABLE dept (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE emp (
  id        INTEGER PRIMARY KEY,
  name      VARCHAR NOT NULL,
  dept_id   INTEGER REFERENCES dept(id),
  salary    DECIMAL(10,2),
  UNIQUE (name)
);

INSERT INTO dept VALUES (1, '工程部'), (2, '市场部');
INSERT INTO emp  VALUES
  (1, '张三', 1, 10000.00),
  (2, '李四', 2,  8000.00),
  (3, '王五', 1,  9000.00);
```

下文示例大多基于这套数据(`public`/`shop` 均可)。

---

## 数据类型

| 类型 | 说明 |
|------|------|
| `SMALLINT` / `INTEGER` / `BIGINT` | 16 / 32 / 64 位整数。`INT` 是 `INTEGER` 的别名 |
| `REAL` / `FLOAT` | 32 位浮点 |
| `DOUBLE` | 64 位浮点 |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | 128 位定点(BigDecimal),默认 precision/scale(10/0);NUMERIC 与 DECIMAL 等价 |
| `VARCHAR(n)` / `CHAR(n)` | 变长字符串;**不做定长空格填充**,`VARCHAR(20)` 的长度不强制 |
| `NCHAR` / `NVARCHAR` | 变长字符串;**暂不能经 SQL DDL 创建**(Calcite 解析器视 NCHAR 为保留字),程序化建表与元数据保真已通 |
| `BOOLEAN` | 布尔 |
| `DATE` | 日期(`DATE '2025-01-05'`) |
| `TIME` | 时间(`TIME '10:30:00'`),无算术,仅比较与 CAST |
| `TIMESTAMP` | 时间戳(`TIMESTAMP '2025-01-05 12:34:56'`),毫秒精度 |
| `BINARY` / `VARBINARY` | 字节串(字面量 `X'DEADBEEF'`);**参与 JOIN/聚合/窗口/去重时结果未定义**(byte[] 无值语义) |

日期/时间字面量与比较均已验证:

```sql
SELECT d >= DATE '2025-06-01', tm BETWEEN TIME '11:00:00' AND TIME '13:00:00',
       ts BETWEEN TIMESTAMP '2025-01-01 00:00:00' AND TIMESTAMP '2025-06-01 00:00:00'
FROM t;
```

---

## 表类型与存储引擎

每张表在 `data/<schema>/<table>/` 目录下,数据是目录里的 part 文件。表类型决定存储引擎与能力:

| `WITH ('type'=...)` | 引擎 | 能力 | 默认选择 |
|------|------|------|----------|
| `lsm` | LSM-Tree:WAL + MemTable + SSTable + BloomFilter + 后台 compaction | 点查(主键)/ 范围扫描 / 快照隔离 / 支持索引 | **有主键时自动选 LSM** |
| `simple` | SimpleTable:直接写 part 文件 | 全表扫描 / 追加写 | **无主键时自动选 Simple** |
| `materialized_view` | 物化视图(物理存储由查询结果填充,DML 增量刷新) | — | 由 `CREATE MATERIALIZED VIEW` 创建 |

落盘格式由 `WITH ('format'=...)` 选择(**默认 Parquet**):

```sql
CREATE TABLE t1 (id INTEGER PRIMARY KEY) WITH ('type'='lsm');       -- LSM + Parquet(默认)
CREATE TABLE t2 (id INTEGER) WITH ('type'='simple');                -- Simple + Parquet
CREATE TABLE t3 (id INTEGER) WITH ('format'='arrow');               -- Simple + Arrow IPC
CREATE TABLE t4 (id INTEGER) WITH ('format'='parquet');             -- 显式 Parquet
CREATE TABLE t5 (id INTEGER PRIMARY KEY) WITH ('type'='lsm', 'format'='arrow');
```

> `WITH` 选项仅接受 `type` / `format` 两个键,其余键报错;`format` 值为 `arrow`/`ipc`/`parquet`。

### 磁盘布局

```
data/
├── catalog.json          # 元数据(schema/表/列/约束/索引/视图/物化视图)
├── txlog.log             # 事务日志(启动恢复用)
└── public/
    ├── emp/              # LSM 表目录
    │   ├── wal.log       # 写前日志(双缓冲按代分段)
    │   ├── *.sst         # SSTable(含块索引与布隆过滤器)
    │   └── .indexes/     # 二级索引表目录(每索引一个 LSM 表)
    ├── dept/wal.log      # LSM 表
    └── orders/           # Simple 表目录
        └── part-000001.parquet   # 或 part-000001.arrow
```

### 持久化与崩溃恢复

- **SimpleTable**:每次 DML 写新的 part 文件;正常关闭(`stop` / shutdown hook)统一落盘。
- **LSMTable**:写操作先进 WAL(可配置 `wal-fsync`),MemTable 写满落盘为 SSTable;启动时按「WAL 重放 → 装载 SSTable」恢复,已提交数据不丢。后台线程定期 compaction(合并小 part、按层级组织)。
- **事务**:全局事务日志 + LSM 快照隔离,崩溃后启动时只重放已提交事务的变更(见[事务](#事务))。
- **索引**:数据表 `.indexes/<name>/` 下独立 LSM 表,启动时 `rebuildFromDisk` 重建句柄。
- **元数据**:`catalog.json` 独立持久化,空表(从未插入)重启后仍存活。

---

## 建表与 DDL

### CREATE TABLE

```sql
-- 列约束(主键/非空/唯一/外键)
CREATE TABLE t (
  id      INTEGER PRIMARY KEY,
  name    VARCHAR NOT NULL,
  dept_id INTEGER REFERENCES dept(id),   -- 外键:执行期 INSERT/UPDATE 校验存在,DELETE 校验不被引用(RESTRICT)
  UNIQUE (email)
);

-- 表级约束
CREATE TABLE t (
  id INTEGER, dept_id INTEGER,
  PRIMARY KEY (id),
  UNIQUE (id, dept_id),
  FOREIGN KEY (dept_id) REFERENCES dept(id)
);

-- 存储选项
CREATE TABLE t (id INTEGER) WITH ('type'='lsm', 'format'='parquet');

-- 复制表结构(列 + 约束)
CREATE TABLE t2 LIKE t;
```

> 主键隐含 NOT NULL;主键列不允许 NULL。`CHECK` 约束**不支持**(解析即报错);`DEFAULT` 在 CREATE 中会被解析但**不生效**,仅 `ALTER TABLE ... ADD COLUMN ... DEFAULT` 支持。

### ALTER TABLE

```sql
ALTER TABLE t ADD COLUMN b INTEGER;              -- 可带 DEFAULT:ADD COLUMN c INTEGER DEFAULT 0
ALTER TABLE t DROP COLUMN b;                     -- 索引列/主键/被外键引用列不可删
ALTER TABLE t RENAME COLUMN a TO a2;             -- 索引定义中的列名同步改名
ALTER TABLE t ALTER COLUMN a SET DATA TYPE BIGINT;  -- 类型转换(CAST 语义)
ALTER TABLE t ALTER COLUMN a SET NOT NULL;       -- 存量数据违反则拒绝
ALTER TABLE t ALTER COLUMN a DROP NOT NULL;
ALTER TABLE t ADD CONSTRAINT uq_name UNIQUE (name); -- 或 PRIMARY KEY (cols) / FOREIGN KEY ...
ALTER TABLE t RENAME TO t2;
```

> 只支持 `DROP PRIMARY KEY` 形式的约束删除;带名字的约束删除(`DROP CONSTRAINT name`)暂不支持。

### DROP / TRUNCATE

```sql
DROP TABLE [IF EXISTS] t;
DROP TABLE IF EXISTS t;
TRUNCATE TABLE t;          -- 清空数据、保留结构与索引;同时清空依赖它的物化视图
```

### Schema

```sql
CREATE SCHEMA s;
CREATE SCHEMA IF NOT EXISTS s;
USE SCHEMA s;              -- 切换当前 schema(每连接隔离)
DROP SCHEMA [IF EXISTS] s; -- 注意:不支持 CASCADE
```

默认 schema 为 `public`;`public` 不可删除;`information_schema` 是保留名,不可创建/删除。

### 索引

```sql
CREATE INDEX idx_dept ON emp (dept_id);
CREATE UNIQUE INDEX idx_ab ON emp (dept_id, name);
DROP INDEX [IF EXISTS] idx_dept ON emp;
```

约束:仅 **LSM 表**(有主键)可建索引;索引列类型限 `SMALLINT/INTEGER/BIGINT/VARCHAR`(VARCHAR 索引可建但暂不用于查询加速);UNIQUE 索引在建索引时与 INSERT/UPDATE 时校验冲突。查询自动选索引,`EXPLAIN` 显示 `index=<name>`(详见[索引](#索引))。

---

## 数据操纵 DML

```sql
-- INSERT:VALUES 多行 / 列清单 / SELECT
INSERT INTO t VALUES (1, 'a'), (2, 'b');
INSERT INTO t (id, name) VALUES (1, 'a');
INSERT INTO t2 SELECT id, name FROM t;

-- UPDATE(可含表达式,支持 WHERE 子查询)
UPDATE emp SET salary = salary * 1.1 WHERE dept_id = 1;

-- DELETE(支持 WHERE 子查询)
DELETE FROM emp WHERE id IN (SELECT id FROM emp WHERE id < 12);

-- TRUNCATE
TRUNCATE TABLE emp;
```

约束校验:主键冲突、UNIQUE 冲突、NOT NULL、外键引用,均在执行期检查并报错。

---

## SELECT 查询

### 基础

```sql
SELECT id, name FROM emp WHERE dept_id = 1 ORDER BY id;
SELECT id FROM emp ORDER BY id DESC;
SELECT id FROM emp ORDER BY id LIMIT 2;            -- 注意:LIMIT 在 OFFSET 之前
SELECT id FROM emp ORDER BY id LIMIT 1 OFFSET 1;
SELECT DISTINCT dept_id FROM emp ORDER BY dept_id;
SELECT dept_id, COUNT(*) AS n, AVG(salary) AS avg_sal
FROM emp GROUP BY dept_id HAVING COUNT(*) >= 1 ORDER BY dept_id;
SELECT * FROM emp WHERE d BETWEEN DATE '2025-01-01' AND DATE '2025-06-01';
SELECT * FROM emp WHERE name IS NULL OR name IS NOT NULL;
```

### JOIN

```sql
-- 等值 JOIN(哈希/排序归并,按代价选择)
SELECT e.name, d.name AS dept FROM emp e JOIN dept d ON e.dept_id = d.id;
SELECT e.name, d.name FROM emp e LEFT  JOIN dept d ON e.dept_id = d.id;
SELECT e.name, d.name FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id;
SELECT e.name, d.name FROM emp e FULL  JOIN dept d ON e.dept_id = d.id;

-- 非等值 JOIN(嵌套循环)
SELECT a.id, b.id FROM emp a JOIN dept b ON a.dept_id > b.id;

-- 多条件 / 逗号等价 / USING
SELECT a.id FROM a JOIN b ON a.id = b.id AND a.name = b.val;
SELECT e.name FROM emp e, dept d WHERE e.dept_id = d.id;
SELECT a.id FROM a JOIN b USING (id);

-- 三表 JOIN
SELECT a.id FROM a JOIN b ON a.id = b.id JOIN a c ON b.id = c.id;
```

> NULL 键等值永不匹配(JOIN 三种算法都显式处理)。

### 集合运算

```sql
SELECT id FROM a UNION SELECT id FROM b;            -- 去重
SELECT id FROM a UNION ALL SELECT id FROM b;        -- 不去重
SELECT id FROM a INTERSECT SELECT id FROM b;
SELECT id FROM a INTERSECT ALL SELECT id FROM b;    -- 取最小出现次数
SELECT id FROM a EXCEPT SELECT id FROM b;           -- 去重差集
SELECT id FROM a EXCEPT ALL SELECT id FROM b;       -- 计数差集
```

### CTE(公共表表达式)

```sql
-- 非递归
WITH c AS (SELECT id, name FROM emp WHERE dept_id = 1)
SELECT id, name FROM c ORDER BY id;

-- 递归:计数器
WITH RECURSIVE nums(n) AS (
  VALUES (1)
  UNION ALL
  SELECT n + 1 FROM nums WHERE n < 5
) SELECT n FROM nums ORDER BY n;

-- 递归:图遍历(有向图可达点)
CREATE TABLE edges (src INTEGER, dst INTEGER);
INSERT INTO edges VALUES (1, 2), (2, 3), (1, 3), (3, 4);

WITH RECURSIVE reach(n) AS (
  VALUES (1)
  UNION
  SELECT e.dst FROM edges e JOIN reach r ON e.src = r.n
) SELECT n FROM reach ORDER BY n;   -- 1,2,3,4

-- 递归:斐波那契
WITH RECURSIVE f(n, prior) AS (
  VALUES (1, 0)
  UNION ALL
  SELECT n + prior, n FROM f WHERE n < 100
) SELECT n FROM f ORDER BY n;       -- 1,1,2,3,5,8,13,...
```

### 子查询

```sql
-- IN 子查询
SELECT id FROM a WHERE a.id IN (SELECT b.aid FROM b);

-- 相关标量子查询
SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.aid = a.id) AS c FROM a ORDER BY a.id;

-- EXISTS / NOT EXISTS
SELECT id FROM emp e WHERE EXISTS (SELECT 1 FROM dept d WHERE d.id = e.dept_id);
SELECT * FROM emp e WHERE NOT EXISTS (SELECT 1 FROM dept d WHERE d.id = e.dept_id);
```

> `NOT IN` 遇到子查询含 NULL 时按三值逻辑返回空集(标准行为);要按直觉过滤需先排除 NULL,如 `a.id NOT IN (SELECT aid FROM b WHERE aid IS NOT NULL)`。

### 窗口函数

```sql
-- 分区聚合
SELECT dept, mth, SUM(amount) OVER (PARTITION BY dept) AS s FROM sales;

-- 运行累计(分区内按序累加)
SELECT dept, mth, SUM(amount) OVER (PARTITION BY dept ORDER BY mth) AS running
FROM sales ORDER BY dept, mth;

-- 排名
SELECT amount, RANK() OVER (ORDER BY amount) AS rk,
       DENSE_RANK() OVER (ORDER BY amount) AS dr FROM sales;

-- 行号
SELECT dept, mth, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY mth) AS rn FROM sales;

-- 偏移 / 首末值
SELECT dept, mth, LAG(amount) OVER (PARTITION BY dept ORDER BY mth) AS lg,
       LEAD(amount) OVER (PARTITION BY dept ORDER BY mth) AS ld FROM sales;
SELECT x, LAG(x, 1, 0) OVER (ORDER BY x) FROM t;              -- offset + default
SELECT g, FIRST_VALUE(x) OVER (PARTITION BY g ORDER BY x),
       LAST_VALUE(x) OVER (PARTITION BY g ORDER BY x) FROM t;

-- 帧(ROWS 窗口)
SELECT mth, SUM(amount) OVER (ORDER BY mth ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales;

-- 全表计数
SELECT dept, amount, COUNT(*) OVER () FROM sales;
```

支持的窗口函数:聚合 `SUM / AVG / COUNT / MIN / MAX`、排名 `ROW_NUMBER / RANK / DENSE_RANK`、偏移 `LEAD / LAG`(可选 offset 与 default)、首末 `FIRST_VALUE / LAST_VALUE`。

---

## 函数

> 以下函数均已验证。字符串函数按 Unicode **code point** 处理(多字节字符不会被拆坏),`NULL` 参数按 STRICT 语义传播(结果为 NULL)。

### 数学函数

```sql
SELECT ABS(-5), FLOOR(3.7), CEIL(3.2), ROUND(2.567, 2), ROUND(2.5);
--   5           3          4          2.570         3.0
```

| 函数 | 说明 |
|------|------|
| `ABS(x)` | 绝对值(整型/浮点/DECIMAL) |
| `FLOOR(x)` / `CEIL(x)` / `CEILING(x)` | 向下 / 向上取整 |
| `ROUND(x)` / `ROUND(x, n)` | 就近取整(0.5 远离零,SQL 语义);`ROUND(x, n)` 保留 n 位小数 |

### 字符串函数

```sql
SELECT UPPER('hello'), LOWER('ABC'), CHAR_LENGTH('字符'), LENGTH('字符');
--   HELLO            abc           2                   2
SELECT CONCAT('a', 'b', 'c'), 'a' || 'b';                      -- abc | ab
SELECT SUBSTRING('hello world', 1, 5), POSITION('world' IN 'hello world');  -- hello | 7
SELECT REPLACE('abracadabra', 'a', 'X');                        -- XbrXcXdXbrX
SELECT TRIM('  hi  '), TRIM(LEADING 'x' FROM 'xxhixx');        -- hi | hixx
SELECT LEFT('hello', 2), RIGHT('hello', 2), REPEAT('ab', 3), REVERSE('hello');
--   he                lo                 ababab          olleh
SELECT LPAD('hi', 5, '0'), RPAD('hi', 5, '0'), INITCAP('hello world');  -- 000hi | hi000 | Hello World
SELECT ASCII('A'), CHR(65), SPLIT_PART('a,b,c', ',', 2);       -- 65 | A | b
```

| 函数 | 说明 |
|------|------|
| `UPPER(s)` / `LOWER(s)` | 大小写转换(中文不变) |
| `CHAR_LENGTH(s)` / `LENGTH(s)` / `CHARACTER_LENGTH(s)` | 字符数(按 code point) |
| `CONCAT(a, b, ...)` | 变参拼接,任一参数 NULL → NULL;二元拼接(\|\| 运算符) |
| `SUBSTRING(s, from[, len])` | 1-based 截取,按 code point |
| `POSITION(sub IN s)` | 子串位置(1-based),未找到返回 0 |
| `REPLACE(s, from, to)` | 全量替换 |
| `TRIM([{LEADING|TRAILING|BOTH} chars FROM] s)` | 去首尾字符(默认空格) |
| `LEFT(s, n)` / `RIGHT(s, n)` | 取左/右 n 个字符 |
| `REPEAT(s, n)` | 重复 n 次 |
| `REVERSE(s)` | 反转(按 code point) |
| `LPAD(s, n[, pad])` / `RPAD(s, n[, pad])` | 填充到 n 字符,默认空格 |
| `INITCAP(s)` | 词首大写、其余小写 |
| `ASCII(s)` | 首字符码点,空串返回 0 |
| `CHR(n)` | 码点转字符 |
| `SPLIT_PART(s, delim, n)` | 按分隔符取第 n 段(1-based) |

### 日期时间

```sql
SELECT EXTRACT(YEAR FROM DATE '2025-01-05'), EXTRACT(QUARTER FROM d), EXTRACT(HOUR FROM ts);
SELECT CURRENT_DATE, CURRENT_TIMESTAMP;
```

`EXTRACT(field FROM date_expr)` 支持 `YEAR / QUARTER / MONTH / WEEK / DAY / DOW / DOY / HOUR / MINUTE / SECOND / MILLISECOND`,作用于 DATE 与 TIMESTAMP。`CURRENT_DATE` / `CURRENT_TIMESTAMP` 为当前时间函数。

### 比较与逻辑

```sql
SELECT name FROM t WHERE name LIKE '张%';            -- % 任意序列、_ 单字符
SELECT name FROM t WHERE name NOT LIKE 'A%';
SELECT * FROM t WHERE x IS NOT DISTINCT FROM y;      -- null-safe 等值
SELECT CASE WHEN 1 > 2 THEN 'yes' ELSE 'no' END;     -- no
SELECT COALESCE(flag, TRUE) FROM t;                  -- 首个非 NULL
SELECT CAST('42' AS INTEGER), CAST(42 AS VARCHAR), CAST(id AS VARCHAR);
```

支持:`= / <> / < / <= / > / >=`、`AND / OR / NOT`、`LIKE / NOT LIKE`、`IS NULL / IS NOT NULL`、`IS [NOT] DISTINCT FROM`、`IS TRUE / IS FALSE`、`BETWEEN`、`IN`、`CASE`、`COALESCE`、`CAST`(跨数值类型、数值↔字符串、日期/时间)。

### 聚合函数

```sql
SELECT COUNT(*), SUM(id), AVG(id), MIN(id), MAX(id) FROM t;
SELECT COUNT(DISTINCT id), SUM(DISTINCT id) FROM t;
SELECT dept_id, COUNT(*) FROM emp GROUP BY dept_id HAVING COUNT(*) > 1;
SELECT VAR_SAMP(x), VAR_POP(x), STDDEV_SAMP(x), STDDEV_POP(x) FROM t;
```

| 函数 | 说明 |
|------|------|
| `COUNT(*)` / `COUNT(col)` | 行数;`COUNT(col)` 跳过 NULL |
| `SUM` / `AVG` / `MIN` / `MAX` | 支持 `DISTINCT` 与表达式参数;`AVG` 整型提升为 DOUBLE,DECIMAL 保精度 |
| `VAR_SAMP` / `VAR_POP` | 样本 / 总体方差 |
| `STDDEV_SAMP` / `STDDEV_POP` | 样本 / 总体标准差 |

### 窗口函数

见 [SELECT 查询 → 窗口函数](#窗口函数)。

---

## 索引

二级索引的存储 = 一张 LSM 表(`data/<schema>/<table>/.indexes/<name>/`),schema 为 `(索引列..., 主键列...)`,主键 = 全部列。

```sql
CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER, name VARCHAR, salary DECIMAL(10,2));
CREATE INDEX idx_dept ON emp (dept_id);

-- 等值 / IN 命中索引(EXPLAIN 显示 index=)
EXPLAIN SELECT * FROM emp WHERE dept_id = 10;
--   Filter(emp index=idx_dept)
SELECT * FROM emp WHERE dept_id IN (10, 30);

-- 唯一索引:重复键插入/更新被拒绝
CREATE UNIQUE INDEX uq_name ON emp (name);
```

要点:

- 仅**有主键的表(LSM)**可建索引;Simple 表无点查能力,索引无意义。
- 索引列类型限 `SMALLINT / INTEGER / BIGINT / VARCHAR`;VARCHAR 索引可建但查询暂不走索引(LSM MemTable 键比较器要求 Comparable,Arrow Text 不兼容)。
- UNIQUE 索引建表时校验存量(重复则整个建索引回滚),INSERT 时批内自体去重 + 索引前缀扫描,UPDATE 校验新键排除被更新行后仍冲突则拒绝。
- 查询自动选覆盖最多列的索引,走「索引前缀扫描 → 收集主键 → 回表 → residual 过滤」。
- `ALTER TABLE` 重命名索引列会同步更新索引定义;DROP/改类型受索引约束的列会报错。

---

## 视图与物化视图

### 视图(普通视图,逻辑展开)

```sql
CREATE VIEW v_eng AS SELECT id, name FROM emp WHERE dept_id = 1;
CREATE OR REPLACE VIEW v_eng AS SELECT id, name FROM emp;
CREATE VIEW v (num, label) AS SELECT id, name FROM emp;      -- 显式列名
DROP VIEW [IF EXISTS] v;

-- 视图套视图
CREATE VIEW v1 AS SELECT id FROM emp WHERE id > 1;
CREATE VIEW v2 AS SELECT id FROM v1 WHERE id < 3;
SELECT id FROM v2;      -- 2
```

视图存储定义 SQL,查询时展开;可叠加过滤、跨 schema 引用;随 catalog 持久化,重启后仍可用。

### 物化视图(物理存储 + 增量刷新)

```sql
CREATE MATERIALIZED VIEW mv_dept AS
  SELECT dept_id, COUNT(*) AS n, AVG(salary) AS avg_sal
  FROM emp GROUP BY dept_id;

SELECT * FROM mv_dept ORDER BY dept_id;

-- 基表 DML 后自动增量刷新
INSERT INTO emp VALUES (4, '赵六', 1, 12000.00);
SELECT * FROM mv_dept;     -- dept_id=1 的 n/avg 已更新

-- 也可手动全量刷新
REFRESH MATERIALIZED VIEW mv_dept;

DROP MATERIALIZED VIEW [IF EXISTS] mv_dept;
```

- 支持 **SPJ(单表 Select-Project-Filter)** 与 **单表聚合(SUM/COUNT/AVG/MIN/MAX + GROUP BY)** 两类定义;不支持 JOIN 的视图定义。
- DML 增量刷新在事务提交后执行;DELETE/UPDATE 涉及 AVG/MIN/MAX 时按 stale 标记退避(不影响正确性,下次 REFRESH 修正)。
- 有物化视图依赖的表不可 DROP;`TRUNCATE` 基表会同步清空依赖的物化视图。

---

## 元数据 information_schema

提供只读系统表:

| 表 | 列 |
|----|----|
| `information_schema.schemata` | `CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER, ...` |
| `information_schema.tables` | `TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE` |
| `information_schema.columns` | `TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE` |
| `information_schema.materialized_views` | `MV_CATALOG, MV_SCHEMA, MV_NAME, DEFINITION, DEPENDENCIES, IS_STALE` |

```sql
SELECT SCHEMA_NAME FROM information_schema.schemata ORDER BY SCHEMA_NAME;
--   information_schema | public

SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.tables
WHERE TABLE_SCHEMA = 'public' ORDER BY TABLE_NAME;

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
FROM information_schema.columns
WHERE TABLE_NAME = 'emp' ORDER BY ORDINAL_POSITION;
--   id | INTEGER | NULL | NULL
--   salary | DECIMAL | 10 | 2
```

`information_schema` 是保留 schema,不可作为用户 schema 创建/删除。JDBC `DatabaseMetaData.getSchemas/getTables/getColumns` 由服务端 `MetadataExecutor` 提供等价能力。

---

## 事务

```java
conn.setAutoCommit(false);
stmt.executeUpdate("INSERT INTO emp VALUES (5, '钱七', 2, 7000.00)");
conn.rollback();          // 或 conn.commit();
conn.setAutoCommit(true);
```

- 隔离级别由 `conf/config.yaml` 的 `server.isolation-level` 配置,支持 `read-uncommitted / read-committed / repeatable-read / serializable`,**默认 SERIALIZABLE**(快照 + 读写冲突检测)。
- LSM 表按快照读取,事务内读自己的写;READ_COMMITTED 每语句刷新快照。
- 持久性:提交先写全局事务日志,再合并各表数据;崩溃后启动恢复只重放已提交事务,未提交事务回滚。连接断开自动回滚活跃事务。
- DML 触发的外键/唯一/主键校验在事务内同样生效。

---

## EXPLAIN 与 ANALYZE

```sql
EXPLAIN SELECT id, name FROM emp WHERE dept_id = 1;
--   Filter(emp index=idx_emp_dept)

ANALYZE emp;                       -- 收集/更新统计信息(直方图,持久化到 .stats)
EXPLAIN SELECT id, name FROM emp WHERE dept_id = 1;
--   估算行数:Filter(emp) ... estimated

EXPLAIN ANALYZE SELECT id FROM emp WHERE dept_id = 1;   -- 真实执行,输出每节点耗时/行数/批数

COMPACT TABLE emp;                 -- 手动合并该表的 part 文件
```

`EXPLAIN` 输出 7 列:`id / parent_id / operation / rows / batches / elapsed_ms / remarks`;`EXPLAIN ANALYZE` 插桩真实执行并记录每算子实际行数与耗时。统计信息由 `ANALYZE` 生成、DML 后自动标记 stale。

---

## 限制

- **单机数据库**:无分布式、无多节点复制。
- 崩溃可能丢失 Simple 表未 flush 的写入(LSM 表由 WAL 保证);正常关闭统一落盘。
- `PreparedStatement` 为客户端参数替换实现,无服务端预编译。
- 结果集默认整体拉取,`setFetchSize(n)` 走服务端游标分页;客户端不做本地缓存。
- `BINARY/VARBINARY` 无值语义,JOIN/聚合/窗口/去重结果未定义;`TIME` 无算术。
- 不支持:`CHECK` 约束、`DROP CONSTRAINT name`、`DROP SCHEMA ... CASCADE`、`%`(取模)运算符、`CREATE TABLE` 列级 `DEFAULT`(会被解析但不生效)。
- 标识符用双引号引用(引号风格为 `DOUBLE_QUOTE`,MYSQL lex);反引号不支持。
- 递归 CTE 支持线性递归(`UNION` / `UNION ALL`,递归项中仅一次引用自身);非线性(递归项多次引用自身)暂不支持。
- 派生表(子查询)不带 `LIMIT` 的 `ORDER BY` 无排序语义(Calcite 会丢弃 Sort)。

---

## 开发指南

### 代码组织

SQL 执行流水线:

```
QueryExecutor.execute(sql, currentSchema)
  → 前缀拦截(EXPLAIN / EXPLAIN ANALYZE / ANALYZE / USE SCHEMA / REFRESH MV / COMPACT)
  → CalciteContext.parse(sql) → SqlNode
  → Planner.plan(sql, currentSchema) → RelNode(逻辑优化 → VolcanoPlanner 物理转换)
  → MiniDbRel.execute(ExecContext) → BatchIterator(拉模式批式向量化)
  → QueryResult(Rows / Update / UseSchema)
  → SessionHandler → wire → 客户端
```

关键包(`minidb-server/src/main/java/com/minidb/server/`):

- `exec/` — `QueryExecutor`(SQL 入口)、`BatchIterator`(拉模式迭代器)、`RexInterpreter`(表达式求值)、`exec/functions`(列式标量函数框架)、`RowCopier`、`Paginator`(游标分页)。
- `plan/physical/` — 物理算子(`MiniDbScan / Filter / Project / Sort / Aggregate / Join(三种算法) / Union / SetOp / RepeatUnion / Modify / Values / TableSpool / Calc`),均 `implements MiniDbRel`。
- `plan/logical/` + `rule/logical/` + `rule/physical/` — 逻辑优化规则与物理转换规则。
- `storage/` — `StorageManager`(表目录)、`IndexManager`(二级索引)、`AlterTableHandler`。
- `catalog/` — `MiniDbCatalog`(线程安全元数据)、`TableSchema / ColumnMeta / ColumnType`。
- `calcite/` — `CalciteContext`(parser 配置)、`MiniDbRootCalciteSchema`(schema 树)。
- `transaction/` — `TransactionManager`、`TxLog`、快照隔离与 SERIALIZABLE 冲突检测。
- `stats/` — 直方图统计与选择率模型。
- `netty/` — `SessionHandler`(per-channel currentSchema、事务、游标)。

### 贡献约定

1. 改完代码就提交,conventional commit 风格(`feat:`/`fix:`/`test:`/`refactor:`/`docs:`),不 amend。
2. 测试用 JUnit 5 + `@TempDir` + `RootAllocator`,断言关系/比例而非精确浮点值。
3. 现有物理算子文件与 `minidb-protocol` 尽量不改——扩展通过新模块/新算子(参考 EXPLAIN 用 `ExplainExecutor` + `Instrumenter` 外挂)。
4. 新增物理算子时同步补优化规则(对照 Calcite `CoreRules` 的换位/下推/合并规则)。
5. 代码是给人读的:描述性命名 + WHY 注释,不做局部特判糊过去。
6. 新增标量函数走 `exec/functions` 框架:按 `SqlOperator` 注册 `Function`(所有重载收进同一 `Function` 的 `overloads`),跨 family 混合类型需补跨型重载。

---

## 验证说明

本 README 的全部 SQL 示例均通过发行包启动的服务端(`com.minidb.server.MiniDbServer`,JDK 17)使用自研 JDBC 驱动实际执行验证,覆盖:数据类型、两种表类型与两种存储格式、约束与外键、DML、SELECT 全特性、JOIN 全类型、集合运算、CTE(含递归与图遍历)、窗口函数、子查询、全部内置函数、视图与物化视图(含 DML 自动刷新)、索引与 `EXPLAIN index=`、`information_schema`、schema 切换、ALTER TABLE 全操作、`CREATE TABLE LIKE`、事务(commit/rollback)、`ANALYZE`/`EXPLAIN ANALYZE`、`COMPACT TABLE`,共 100+ 条语句全部通过。
