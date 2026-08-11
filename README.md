# MiniDB

基于 Apache Calcite、Apache Arrow、Netty 的微型 JDBC 数据库。

## 特性

- 自研 JDBC 驱动（`jdbc:minidb://host:port`），基于自定义 Netty wire 协议
- Calcite 驱动的 SQL：CREATE/DROP TABLE、INSERT（VALUES 与 ...SELECT 两种形式）、SELECT（支持 WHERE / ORDER BY / LIMIT）
- 数据以 Arrow 列式批次存于内存，持久化为 Arrow IPC 文件（`data/*.arrow`）
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

## 支持的列类型

INTEGER、BIGINT、DOUBLE、VARCHAR、BOOLEAN、DATE、TIMESTAMP。

## 限制

- 无事务（autoCommit 恒为 true）、无 UPDATE/DELETE、无 JOIN、无聚合
- 崩溃可能丢失未 flush 的插入（正常关闭时统一落盘）
- PreparedStatement 为客户端参数替换实现
- 结果集在客户端一次性物化，不做服务端分页
