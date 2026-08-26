# MiniDB

基于 Apache Calcite、Apache Arrow、Netty 的微型 JDBC 数据库。

## 特性

- 自研 JDBC 驱动（`jdbc:minidb://host:port`），基于自定义 Netty wire 协议
- Calcite 驱动的 SQL：CREATE/DROP TABLE、INSERT（VALUES 与 ...SELECT 两种形式）、UPDATE/DELETE/TRUNCATE、SELECT（WHERE / ORDER BY / LIMIT / OFFSET）
- 查询能力：JOIN（INNER/LEFT/RIGHT/FULL）、聚合（GROUP BY + COUNT/SUM/AVG/MIN/MAX + DISTINCT）、集合运算（UNION/INTERSECT/EXCEPT）、窗口函数（SUM/AVG/COUNT/MIN/MAX over + ROW_NUMBER/RANK/DENSE_RANK/LEAD/LAG/FIRST_VALUE/LAST_VALUE）、CTE（WITH，含递归 WITH RECURSIVE）
- 二级索引：CREATE INDEX / DROP INDEX、UNIQUE 索引、查询自动走索引、EXPLAIN 显示 index=
- Schema 支持：CREATE/DROP SCHEMA、`schema.table` 限定名、`USE SCHEMA` 切换当前 schema（每连接隔离），所有表默认属于 `public` schema
- 数据以 Arrow 列式批次存于内存，持久化为 Arrow IPC 文件（`data/<schema>/<table>.arrow`）
- VolcanoPlanner + 自研 ConverterRule 生成物理算子，批式向量化执行

## 构建与测试

需要 JDK 17（JAVA_HOME 指向 JDK 17）。

```
mvnw.cmd test
```

## 运行服务端

```
mvnw.cmd -pl minidb-server exec:java
```

手动运行需要增加jvm参数：

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

默认监听 8899 端口，数据目录 `./data`。可用 `--port <n>`、`--data <dir>` 覆盖（exec 参数在 `minidb-server/pom.xml` 配置）。

## Java 客户端用法

把 `minidb-jdbc` 及其依赖放进 classpath，然后：

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

应用需要增加jvm参数：

```
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

> arrow的类：org.apache.arrow.memory.util.MemoryUtil 使用了Unsafe

## SQL 语法示例

### 二级索引

```sql
-- 创建索引
CREATE INDEX idx_a ON t (a);
CREATE UNIQUE INDEX idx_ab ON t (a, b);

-- 删除索引
DROP INDEX idx_a ON t;
DROP INDEX IF EXISTS idx_a ON t;

-- 查询自动走索引（EXPLAIN 显示 index=）
EXPLAIN SELECT * FROM t WHERE a = 10;
-- 输出: Scan(t index=idx_a)
```

### 约束与 ALTER

```sql
-- 主键 + 唯一约束 + 外键
CREATE TABLE t (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  dept_id INTEGER REFERENCES dept(id)
);
CREATE TABLE t (id INTEGER, a INTEGER, UNIQUE (a));

-- ALTER TABLE
ALTER TABLE t ADD COLUMN b INTEGER;
ALTER TABLE t DROP COLUMN b;
ALTER TABLE t RENAME COLUMN a TO a2;
ALTER TABLE t ALTER COLUMN a VARCHAR;
ALTER TABLE t SET NOT NULL a;
ALTER TABLE t DROP NOT NULL a;
ALTER TABLE t ADD CONSTRAINT uq_name UNIQUE (name);
ALTER TABLE t RENAME TO t2;
```

### Schema 操作

```sql
CREATE SCHEMA s;
USE SCHEMA s;
DROP SCHEMA s CASCADE;
```

### 高级查询

```sql
-- JOIN
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id;

-- 聚合
SELECT dept, COUNT(*), AVG(salary) FROM emp GROUP BY dept HAVING COUNT(*) > 5;

-- 窗口函数
SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM emp;
SELECT name, SUM(sales) OVER (PARTITION BY dept ORDER BY month) FROM sales;

-- 递归 CTE
WITH RECURSIVE nums(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10
) SELECT * FROM nums;

-- 集合运算
SELECT id FROM t1 UNION SELECT id FROM t2;
SELECT id FROM t1 INTERSECT SELECT id FROM t2;
SELECT id FROM t1 EXCEPT SELECT id FROM t2;
```

## 支持的列类型

SMALLINT、INTEGER、BIGINT、REAL、FLOAT、DOUBLE、DECIMAL、NUMERIC、VARCHAR、CHAR、NCHAR、NVARCHAR、BOOLEAN、DATE、TIME、TIMESTAMP、BINARY、VARBINARY。

- DECIMAL/NUMERIC 为 128 位定点(BigDecimal),支持 precision/scale(默认 10/0);Calcite 将 NUMERIC 归一为 DECIMAL(两者等价)。
- CHAR/NCHAR/NVARCHAR 变长存储,不做定长空格填充(简化)。
- NCHAR 暂不能经 SQL DDL 创建(Calcite 解析器将 NCHAR 视为保留字,不在类型名语法内);程序化建表与元数据保真已通。
- 限制:BINARY/VARBINARY 参与 JOIN/聚合/窗口/去重时结果未定义(byte[] 无值语义);TIME 无算术,仅比较与 CAST。

## 元数据与 information_schema

- 表元数据(schema/表/列名 + 类型 + DECIMAL/NUMERIC 的 precision/scale)持久化在 `data/catalog.json`,独立于数据文件;空表(从未插入)重启后仍存活。
- 提供只读系统表 `information_schema.schemata` / `information_schema.tables` / `information_schema.columns`,可 `SELECT` 查询。`information_schema` 是保留名,不可作为用户 schema 创建。

## 限制

- 无事务（autoCommit 恒为 true）
- 崩溃可能丢失未 flush 的插入（正常关闭时统一落盘）
- PreparedStatement 为客户端参数替换实现
- 结果集在客户端一次性物化，不做服务端分页
- `public` schema 不可删除
- 持久化目录结构为 `data/<schema>/<table>.arrow`；旧版扁平格式 `data/<table>.arrow` 不再兼容，升级前需手动迁移：`mv data/*.arrow data/public/`（同理 `*.stats`）
