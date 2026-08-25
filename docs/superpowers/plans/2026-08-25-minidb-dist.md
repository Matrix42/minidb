# minidb-dist 发行模块 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `minidb-dist` 模块,把服务端/JDBC 驱动/sqlline 组装成 Flink/Spark 风格的发行目录并产出 tar.gz 与 zip 归档,配置从 `data/config.yaml` 迁移到 `conf/config.yaml`。

**Architecture:** 服务端小改 4 处(MiniDbConfig 加 `server.port`、StorageManager 注入外部配置、MiniDbServer 合入 main + conf 重载、删 Main.java),新增纯组装模块 `minidb-dist`(packaging=pom,maven-assembly-plugin 一次产出 dir/tar.gz/zip),发行目录含 `bin/`(双平台脚本,含自写 sqlline 启动脚本)、`conf/`、`data/`、`jdbc/`、`tools/`(sqlline jar)、`libs/`(服务端依赖)。

**Tech Stack:** Maven(assembly-plugin 3.7.1)、bash + cmd 脚本、sqlline 1.12.0(jline 3.21.0 系)。

**Spec:** `docs/superpowers/specs/2026-08-25-minidb-dist-design.md`

## Global Constraints

- 构建命令一律 `./mvnw.cmd ...`(在 bash 下跑,不用 `mvnw.cmd`/`mvn`/`cmd //c`)。
- JDK 17,`JAVA_HOME` 指向 JDK 17。
- 服务端 JVM 参数(发行脚本与 exec 插件一致):`--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED`。
- 提交用 conventional commit(`feat:`/`refactor:`/`docs:`/`test:`),不 amend,不 `--no-verify`,在 master 直接提交。
- 每次构建/测试跑通后才提交;测试类全部 JUnit 5 + `@TempDir`。
- 代码/注释中文,标识符与路径保持原文。
- 旧构造器/旧重载保留并委托(向后兼容,现有测试零改动)。

---

### Task 1: MiniDbConfig 支持 `server.port`

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/config/MiniDbConfig.java`
- Test: `minidb-server/src/test/java/com/minidb/server/config/MiniDbConfigTest.java`

**Interfaces:**
- Consumes: 现有 `MiniDbConfig.load(Path dir)`(读 `dir/config.yaml`)。
- Produces: 新增 `int serverPort()` getter + `public static final int DEFAULT_SERVER_PORT = 8899`;YAML 键 `server.port`,>0 生效,缺省/缺文件回退 8899。

- [ ] **Step 1: 写失败测试**

在 `MiniDbConfigTest` 末尾追加两个测试:

```java
    @Test
    void serverPortDefaultsTo8899(@TempDir Path dir) {
        assertEquals(8899, MiniDbConfig.load(dir).serverPort());
    }

    @Test
    void loadsServerPort(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("config.yaml"),
                "server:\n  port: 9100\n");
        assertEquals(9100, MiniDbConfig.load(dir).serverPort());
    }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbConfigTest`
Expected: FAIL(编译错误,`serverPort()` 不存在)。

- [ ] **Step 3: 实现**

在 `MiniDbConfig` 中:

1. 常量区(与其他 DEFAULT_* 一起)加:`public static final int DEFAULT_SERVER_PORT = 8899;`(注释:`/** 监听端口,conf/config.yaml 的 server.port。 */`)
2. 字段区加:`private final int serverPort;`
3. 构造器参数表末尾加 `int serverPort`,赋值 `this.serverPort = serverPort;`
4. getter:`public int serverPort() { return serverPort; }`
5. `load()` 中在 `int queryThreads = DEFAULT_SERVER_QUERY_THREADS;` 附近加 `int serverPort = DEFAULT_SERVER_PORT;`,并在 `Integer qt = asInt(server...)` 块之后加:

```java
            Integer port = asInt(server == null ? null : server.get("port"));
            if (port != null && port > 0) {
                serverPort = port;
            }
```

6. 构造器调用处 `new MiniDbConfig(targetBytes, autoThreshold, lsmMemtable, lsmL0, lsmMultiplier, lsmFsync, lsmInterval, lsmBloom, queryThreads)` 末尾追加参数 `serverPort`。

- [ ] **Step 4: 跑测试确认通过**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=MiniDbConfigTest`
Expected: PASS(5 个既有 + 2 个新增)。

- [ ] **Step 5: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/config/MiniDbConfig.java minidb-server/src/test/java/com/minidb/server/config/MiniDbConfigTest.java
git commit -m "feat: MiniDbConfig 支持 server.port 配置"
```

---

### Task 2: StorageManager 接受外部 MiniDbConfig(配置与数据目录解耦)

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java`(构造器区域,第 52-58 行附近)

**Interfaces:**
- Consumes: `MiniDbConfig`(Task 1)。
- Produces: 新构造器 `StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir, MiniDbConfig config)`;旧 3 参构造器委托 `this(catalog, allocator, dataDir, MiniDbConfig.load(dataDir))`。`config()` getter 不变。

- [ ] **Step 1: 重构构造器**

把现有 `public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir)` 拆成两个:

```java
    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir) {
        this(catalog, allocator, dataDir, MiniDbConfig.load(dataDir));
    }

    public StorageManager(MiniDbCatalog catalog, BufferAllocator allocator, Path dataDir,
                          MiniDbConfig config) {
        this.catalog = catalog;
        this.allocator = allocator;
        this.dataDir = dataDir;
        this.config = config;
        this.catalogStore = new JsonCatalogStore(dataDir.resolve("catalog.json"));
        this.tableStorage = new IpcFileTableStorage(dataDir);
        formats.put(StorageFormat.ARROW, new ArrowPartFormat());
        formats.put(StorageFormat.PARQUET, new ParquetPartFormat());
        catalog.addListener(this::persistCatalog);
        this.lsmExecutor = new LSMBackgroundExecutor(
                config.lsmL0FileLimit(), config.compactionTargetSizeBytes(),
                config.lsmBackgroundIntervalMs());
        lsmExecutor.start();
    }
```

(原来构造器体里 `this.config = MiniDbConfig.load(dataDir);` 一行删除,其余字段赋值原样保留。)

- [ ] **Step 2: 编译 + 既有测试**

Run: `./mvnw.cmd test -pl minidb-server -Dtest=StorageManagerTest`
Expected: PASS(既有测试,行为未变)。

- [ ] **Step 3: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/storage/StorageManager.java
git commit -m "refactor: StorageManager 接受外部 MiniDbConfig,配置与数据目录解耦"
```

---

### Task 3: MiniDbServer 合入 main、新增 conf 重载、删 Main.java

**Files:**
- Modify: `minidb-server/src/main/java/com/minidb/server/MiniDbServer.java`
- Delete: `minidb-server/src/main/java/com/minidb/server/Main.java`
- Modify: `minidb-server/pom.xml`(exec 插件 mainClass)
- Modify: `CLAUDE.md`(架构描述:Main 合入、配置迁移、发行模块)

**Interfaces:**
- Consumes: `MiniDbConfig`(Task 1)、`StorageManager` 4 参构造器(Task 2)。
- Produces: `MiniDbServer.start(int port, Path dataDir, Path confDir)` 新重载;旧 `start(int port, Path dataDir)` 委托 `start(port, dataDir, dataDir)`;新静态 `main(String[] args)`(解析 `--port`/`--data`/`--conf`,port 优先级 `--port` > conf 的 `server.port` > 8899)。

- [ ] **Step 1: 新增 start 重载**

`MiniDbServer` 中把现有 `public void start(int port, Path dataDir) throws Exception` 改为两个重载:

```java
    public void start(int port, Path dataDir) throws Exception {
        // 向后兼容:配置仍从数据目录读(旧行为)。
        start(port, dataDir, dataDir);
    }

    public void start(int port, Path dataDir, Path confDir) throws Exception {
        allocator = new RootAllocator();
        storage = new StorageManager(catalog, allocator, dataDir, MiniDbConfig.load(confDir));
        // ……以下与原来 start 完全相同(boss/workers/queryPool/bootstrap/bind)……
    }
```

(把原 `start` 方法体整体搬进新重载,仅构造 StorageManager 那行换成 4 参 + `MiniDbConfig.load(confDir)`。补 import:`com.minidb.server.config.MiniDbConfig`。)

- [ ] **Step 2: 合入 main()**

类内(现有 `private static int defaultQueryThreads()` 之后)新增:

```java
    /** 启动入口(发行脚本与 mvn exec 共用):--port 覆盖 conf/config.yaml 的 server.port。 */
    public static void main(String[] args) throws Exception {
        int port = -1;
        Path dataDir = Path.of("data");
        Path confDir = Path.of("conf");
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i])) {
                port = Integer.parseInt(args[++i]);
            } else if ("--data".equals(args[i])) {
                dataDir = Path.of(args[++i]);
            } else if ("--conf".equals(args[i])) {
                confDir = Path.of(args[++i]);
            }
        }
        MiniDbConfig config = MiniDbConfig.load(confDir);
        if (port < 0) {
            port = config.serverPort();
        }
        LOG.info("MiniDB starting on port {} with data dir {}, conf dir {}", port, dataDir, confDir);
        MiniDbServer server = new MiniDbServer();
        server.start(port, dataDir, confDir);
        LOG.info("MiniDB listening on port {}", server.port());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("MiniDB shutting down");
            server.close();
        }));
        Thread.currentThread().join();
    }
```

(需要 import `java.nio.file.Path`——已有,`start` 签名用了。删除 `Main.java` 文件:`rm minidb-server/src/main/java/com/minidb/server/Main.java`。)

- [ ] **Step 3: 更新 exec 插件 mainClass**

`minidb-server/pom.xml` 第 105 行 `<argument>com.minidb.server.Main</argument>` 改为 `<argument>com.minidb.server.MiniDbServer</argument>`。

- [ ] **Step 4: 编译 + 既有测试**

Run: `./mvnw.cmd test -pl minidb-server`
Expected: PASS(全模块既有测试;`Main` 引用已无)。

- [ ] **Step 5: 手工验证 exec 启动**

Run: `./mvnw.cmd -pl minidb-server exec:java`(后台,`timeout 25` 之类)
Expected: 日志出现 `MiniDB listening on port 8899`,随后 Ctrl+C/杀进程。

- [ ] **Step 6: 更新 CLAUDE.md**

- 「执行核心」的 `QueryExecutor` 段落保持;把涉及 `com.minidb.server.Main` 的描述改为:`MiniDbServer.main(...)` 为启动入口(`--port`/`--data`/`--conf` 参数,`--port` 覆盖 conf 配置,默认 conf 目录 `conf`、数据目录 `data`)。
- 「构建与运行」节补:`./mvnw.cmd -pl minidb-server exec:java` 现在经 `MiniDbServer.main`,默认读 `conf/config.yaml`(缺省回退默认值)。
- 「文档与计划」节或架构节提及:发行模块 `minidb-dist`(见 Task 4 之后再补,本步只处理 Main 相关)。

- [ ] **Step 7: 提交**

```bash
git add minidb-server/src/main/java/com/minidb/server/MiniDbServer.java minidb-server/pom.xml CLAUDE.md
git rm minidb-server/src/main/java/com/minidb/server/Main.java
git commit -m "refactor: Main 合入 MiniDbServer,新增 --conf 与 conf/config.yaml 加载"
```

---

### Task 4: 父 pom + minidb-dist 骨架 + assembly 组装

**Files:**
- Modify: `pom.xml`(父:modules 加 minidb-dist、properties 加 sqlline.version、dependencyManagement 加 sqlline)
- Create: `minidb-dist/pom.xml`
- Create: `minidb-dist/src/main/assembly/dist.xml`
- Create: `minidb-dist/src/main/conf/config.yaml`
- Create: `minidb-dist/src/main/conf/log4j2.properties`(内容 = `minidb-server/src/main/resources/log4j2.properties` 原样复制)
- Create: `minidb-dist/src/main/data/README.txt`
- Create: `minidb-dist/src/main/README.md`

**Interfaces:**
- Consumes: 已构建的 `minidb-server` 与 shade 后 `minidb-jdbc` 产物、Maven Central 的 `sqlline:1.12.0`。
- Produces: `minidb-dist/target/minidb-1.0.0/`(目录)+ `.tar.gz` + `.zip`;布局 `bin/ conf/ data/ jdbc/ tools/ libs/` + 根 README.md。

- [ ] **Step 1: 父 pom 三处改动**

1. `<modules>` 末尾(与 `minidb-tpcds` 并列)加 `<module>minidb-dist</module>`。
2. `<properties>` 加 `<sqlline.version>1.12.0</sqlline.version>`。
3. `<dependencyManagement><dependencies>` 末尾(与 `minidb-jdbc` 项并列)加:

```xml
      <dependency>
        <groupId>sqlline</groupId>
        <artifactId>sqlline</artifactId>
        <version>${sqlline.version}</version>
      </dependency>
```

- [ ] **Step 2: minidb-dist/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.minidb</groupId>
    <artifactId>minidb-parent</artifactId>
    <version>1.0.0</version>
  </parent>
  <artifactId>minidb-dist</artifactId>
  <packaging>pom</packaging>

  <!-- 仅用于拉取发行依赖;无源码,由 assembly 组装发行目录。 -->
  <dependencies>
    <dependency>
      <groupId>com.minidb</groupId>
      <artifactId>minidb-server</artifactId>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>com.minidb</groupId>
      <artifactId>minidb-jdbc</artifactId>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>sqlline</groupId>
      <artifactId>sqlline</artifactId>
      <scope>runtime</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-assembly-plugin</artifactId>
        <version>3.7.1</version>
        <configuration>
          <finalName>minidb-1.0.0</finalName>
          <appendAssemblyId>false</appendAssemblyId>
          <descriptors>
            <descriptor>src/main/assembly/dist.xml</descriptor>
          </descriptors>
        </configuration>
        <executions>
          <execution>
            <id>dist</id>
            <phase>package</phase>
            <goals>
              <goal>single</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 3: assembly descriptor `src/main/assembly/dist.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<assembly xmlns="http://maven.apache.org/ASSEMBLY/2.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/ASSEMBLY/2.2.0 https://maven.apache.org/xsd/assembly-2.2.0.xsd">
  <id>dist</id>
  <formats>
    <format>dir</format>
    <format>tar.gz</format>
    <format>zip</format>
  </formats>
  <includeBaseDirectory>true</includeBaseDirectory>
  <baseDirectory>minidb-1.0.0</baseDirectory>

  <!-- libs/:minidb-server 全传递依赖闭包(不含 sqlline,server 不依赖它) -->
  <dependencySets>
    <dependencySet>
      <outputDirectory>libs</outputDirectory>
      <scope>runtime</scope>
      <includes>
        <include>com.minidb:minidb-server</include>
      </includes>
    </dependencySet>
    <!-- jdbc/:仅 shade 后驱动本体;useTransitiveFiltering 防止把 netty/arrow 冗余拷进 jdbc/ -->
    <dependencySet>
      <outputDirectory>jdbc</outputDirectory>
      <scope>runtime</scope>
      <useTransitiveFiltering>true</useTransitiveFiltering>
      <includes>
        <include>com.minidb:minidb-jdbc</include>
      </includes>
    </dependencySet>
    <!-- tools/:sqlline 及其 jline/jna/jansi 传递依赖,与 libs 隔离 -->
    <dependencySet>
      <outputDirectory>tools</outputDirectory>
      <scope>runtime</scope>
      <includes>
        <include>sqlline:sqlline</include>
      </includes>
    </dependencySet>
  </dependencySets>

  <fileSets>
    <!-- 脚本按原样拷贝(源码已用 LF);sh 保留执行位 -->
    <fileSet>
      <directory>src/main/bin</directory>
      <outputDirectory>bin</outputDirectory>
      <fileMode>0755</fileMode>
    </fileSet>
    <fileSet>
      <directory>src/main/conf</directory>
      <outputDirectory>conf</outputDirectory>
    </fileSet>
    <fileSet>
      <directory>src/main/data</directory>
      <outputDirectory>data</outputDirectory>
    </fileSet>
  </fileSets>

  <files>
    <file>
      <source>src/main/README.md</source>
      <outputDirectory>.</outputDirectory>
      <destName>README.md</destName>
    </file>
  </files>
</assembly>
```

- [ ] **Step 4: conf/config.yaml(带注释模板,与 MiniDbConfig 支持的键一一对应)**

```yaml
# MiniDB 服务端配置。缺省键回退默认值,修改后重启生效。
# 配置文件统一放 conf/,数据落在 data/(配置与数据解耦)。
compaction:
  target-size-mb: 128
  auto-part-threshold: 16
lsm:
  memtable-size-mb: 64
  l0-file-limit: 4
  level-size-multiplier: 10
  wal-fsync: false
  background-interval-ms: 1000
  bloom-bits-per-key: 10
server:
  query-threads: 0
  port: 8899
```

- [ ] **Step 5: conf/log4j2.properties 与 data 占位、根 README**

1. `cp minidb-server/src/main/resources/log4j2.properties minidb-dist/src/main/conf/log4j2.properties`(原样)。
2. `minidb-dist/src/main/data/README.txt`:

```
本目录存放 MiniDB 数据文件(表数据与 catalog.json)。勿手动编辑。
```

3. `minidb-dist/src/main/README.md`:

```markdown
# MiniDB 1.0.0

## 目录结构

- `bin/` 启动脚本(linux/macOS 无后缀,Windows 用 .bat)
- `conf/` 配置文件(`config.yaml` + `log4j2.properties`)
- `data/` 数据目录(表数据、catalog.json)
- `jdbc/` JDBC 驱动(自包含 netty/arrow,拷入你项目即可 `jdbc:minidb://host:port`)
- `tools/` sqlline 命令行客户端及依赖
- `libs/` 服务端运行时依赖

## 快速开始(要求 JDK 17+)

1. 启动服务端:`bin/minidb-server`(Windows:`bin\minidb-server.bat`),默认监听 8899
2. 连接:`bin/sqlline`(Windows:`bin\sqlline.bat`),默认连 `jdbc:minidb://localhost:8899`

   ```
   sqlline> CREATE TABLE t(id INT PRIMARY KEY, name VARCHAR(20));
   sqlline> INSERT INTO t VALUES (1, '张总');
   sqlline> SELECT * FROM t;
   ```

3. 停止:Ctrl+C(优雅关闭,数据落盘)

## 配置

- 端口:`conf/config.yaml` 的 `server.port`,或环境变量 `MINIDB_PORT`,或 `--port` 参数(优先级递增)
- 数据目录:环境变量 `MINIDB_DATA_DIR` 覆盖
- 日志:`conf/log4j2.properties`,输出 `logs/minidb.log`
- sqlline 连接:环境变量 `MINIDB_URL` 整体覆盖,`MINIDB_HOST`/`MINIDB_PORT` 拼默认串
- 额外 JVM 参数:环境变量 `MINIDB_JAVA_OPTS`(服务端与客户端均生效)
```

- [ ] **Step 6: 构建并验证目录结构**

Run: `./mvnw.cmd -pl minidb-dist -am package`
Expected: BUILD SUCCESS;`minidb-dist/target/` 下出现 `minidb-1.0.0/` 目录、`minidb-1.0.0.tar.gz`、`minidb-1.0.0.zip`。

验证(手动):

```bash
cd minidb-dist/target/minidb-1.0.0
ls bin/ conf/ data/ jdbc/ tools/ libs/          # 七个条目都在
ls libs | wc -l                                   # 几十个 jar
ls libs | grep -i sqlline || echo "libs 无 sqlline(正确)"
ls tools | grep -i sqlline                        # sqlline-1.12.0.jar + jline 系列
ls jdbc                                            # 仅 minidb-jdbc-1.0.0.jar
ls -l bin/                                         # sh 脚本有执行位
tar tzf ../minidb-1.0.0.tar.gz | head             # 归档含 minidb-1.0.0/ 前缀
unzip -l ../minidb-1.0.0.zip | head               # zip 同构
```

- [ ] **Step 7: CLAUDE.md 补发行模块段落**

在「构建与运行」节末尾加:

```
- **发行包**:`./mvnw.cmd -pl minidb-dist -am package` 产出 `minidb-dist/target/minidb-1.0.0/`(+tar.gz/zip),布局 `bin/`(双平台启动脚本,含自写 sqlline)/`conf/`(config.yaml + log4j2.properties)/`data/`/`jdbc/`(shade 驱动)/`tools/`(sqlline jar)/`libs/`(服务端依赖)。
```

「模块结构」相关描述里补一行 `minidb-dist`(发行组装模块,pom packaging 无源码)。

- [ ] **Step 8: 提交**

```bash
git add pom.xml minidb-dist CLAUDE.md
git commit -m "feat: 新增 minidb-dist 模块,assembly 组装发行目录与 tar.gz/zip"
```

---

### Task 5: bin 双平台启动脚本 + 端到端冒烟

**Files:**
- Create: `minidb-dist/src/main/bin/minidb-server`
- Create: `minidb-dist/src/main/bin/minidb-server.bat`
- Create: `minidb-dist/src/main/bin/sqlline`
- Create: `minidb-dist/src/main/bin/sqlline.bat`

**Interfaces:**
- Consumes: Task 4 的目录布局与 `MiniDbServer.main`(Task 3);`com.minidb.jdbc.MiniDbDriver` 驱动类;`sqlline.SqlLine` 主类。
- Produces: 四个可执行脚本;`MINIDB_HOME` 由脚本位置推导;支持 `MINIDB_JAVA_OPTS`/`MINIDB_PORT`/`MINIDB_HOST`/`MINIDB_URL`/`MINIDB_DATA_DIR`/`MINIDB_CONF_DIR` 环境变量。

- [ ] **Step 1: `bin/minidb-server`(bash)**

```bash
#!/usr/bin/env bash
# MiniDB 服务端启动脚本(linux/macOS)。Windows 用 minidb-server.bat。
set -euo pipefail

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIDB_HOME="$(dirname "$BIN_DIR")"

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA="$(command -v java || true)"
fi
if [ -z "$JAVA" ]; then
  echo "错误:未找到 java。请设置 JAVA_HOME 或加入 PATH。" >&2
  exit 1
fi

DATA_DIR="${MINIDB_DATA_DIR:-$MINIDB_HOME/data}"
CONF_DIR="${MINIDB_CONF_DIR:-$MINIDB_HOME/conf}"

JAVA_OPTS=("--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  "-Dlog4j2.configurationFile=$CONF_DIR/log4j2.properties")

ARGS=(--data "$DATA_DIR" --conf "$CONF_DIR")
if [ -n "${MINIDB_PORT:-}" ]; then
  ARGS+=(--port "$MINIDB_PORT")
fi

cd "$MINIDB_HOME"
# shellcheck disable=SC2086
exec "$JAVA" ${MINIDB_JAVA_OPTS:-} "${JAVA_OPTS[@]}" -cp "$MINIDB_HOME/libs/*" \
  com.minidb.server.MiniDbServer "${ARGS[@]}" "$@"
```

- [ ] **Step 2: `bin/minidb-server.bat`**

```bat
@echo off
rem MiniDB 服务端启动脚本(Windows)。linux/macOS 用 minidb-server。
setlocal

set "BIN_DIR=%~dp0"
for %%I in ("%BIN_DIR%..") do set "MINIDB_HOME=%%~fI"

if defined JAVA_HOME (
  set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
  set "JAVA=java"
)
where "%JAVA%" >nul 2>nul
if errorlevel 1 (
  echo 错误:未找到 java。请设置 JAVA_HOME 或加入 PATH。
  exit /b 1
)

if not defined MINIDB_DATA_DIR set "MINIDB_DATA_DIR=%MINIDB_HOME%\data"
if not defined MINIDB_CONF_DIR set "MINIDB_CONF_DIR=%MINIDB_HOME%\conf"

cd /d "%MINIDB_HOME%"

set "JAVA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dlog4j2.configurationFile=%MINIDB_CONF_DIR%\log4j2.properties"

set "ARGS=--data "%MINIDB_DATA_DIR%" --conf "%MINIDB_CONF_DIR%""
if defined MINIDB_PORT set "ARGS=%ARGS% --port %MINIDB_PORT%"

"%JAVA%" %MINIDB_JAVA_OPTS% %JAVA_OPTS% -cp "%MINIDB_HOME%\libs\*" com.minidb.server.MiniDbServer %ARGS% %*
endlocal
```

- [ ] **Step 3: `bin/sqlline`(bash)**

```bash
#!/usr/bin/env bash
# MiniDB sqlline 命令行客户端(linux/macOS)。Windows 用 sqlline.bat。
set -euo pipefail

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIDB_HOME="$(dirname "$BIN_DIR")"

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA="$(command -v java || true)"
fi
if [ -z "$JAVA" ]; then
  echo "错误:未找到 java。请设置 JAVA_HOME 或加入 PATH。" >&2
  exit 1
fi

HOST="${MINIDB_HOST:-localhost}"
PORT="${MINIDB_PORT:-8899}"
URL="${MINIDB_URL:-jdbc:minidb://${HOST}:${PORT}}"

# shellcheck disable=SC2086
exec "$JAVA" ${MINIDB_JAVA_OPTS:-} \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -cp "$MINIDB_HOME/tools/*:$MINIDB_HOME/jdbc/*" \
  sqlline.SqlLine -d com.minidb.jdbc.MiniDbDriver -u "$URL" "$@"
```

- [ ] **Step 4: `bin/sqlline.bat`**

```bat
@echo off
rem MiniDB sqlline 命令行客户端(Windows)。linux/macOS 用 sqlline。
setlocal

set "BIN_DIR=%~dp0"
for %%I in ("%BIN_DIR%..") do set "MINIDB_HOME=%%~fI"

if defined JAVA_HOME (
  set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
  set "JAVA=java"
)
where "%JAVA%" >nul 2>nul
if errorlevel 1 (
  echo 错误:未找到 java。请设置 JAVA_HOME 或加入 PATH。
  exit /b 1
)

if not defined MINIDB_HOST set "MINIDB_HOST=localhost"
if not defined MINIDB_PORT set "MINIDB_PORT=8899"
if not defined MINIDB_URL set "MINIDB_URL=jdbc:minidb://%MINIDB_HOST%:%MINIDB_PORT%"

"%JAVA%" %MINIDB_JAVA_OPTS% --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp "%MINIDB_HOME%\tools\*;%MINIDB_HOME%\jdbc\*" sqlline.SqlLine -d com.minidb.jdbc.MiniDbDriver -u "%MINIDB_URL%" %*
endlocal
```

- [ ] **Step 5: 全量回归**

Run: `./mvnw.cmd test -pl minidb-dist -am`
Expected: BUILD SUCCESS(所有模块测试 + dist 组装)。

- [ ] **Step 6: bash 脚本端到端冒烟(服务端 + sqlline + 持久化)**

从仓库根执行(在 git-bash 下):

```bash
DIST=minidb-dist/target/minidb-1.0.0
cd "$DIST"
# 1. 启动服务端
bin/minidb-server > /tmp/minidb-smoke.log 2>&1 &
SRV=$!
# 2. 等待端口就绪
for i in $(seq 1 30); do
  (exec 3<>/dev/tcp/localhost/8899) 2>/dev/null && break
  sleep 1
done
# 3. 经 sqlline 建表/插入/查询
bin/sqlline -e "CREATE TABLE smoke(id INT PRIMARY KEY, name VARCHAR(20)); INSERT INTO smoke VALUES (1, '张总'), (2, '李四'); SELECT * FROM smoke;" 2>&1 | tail -20
# 4. 停服并重启,验证数据持久化
kill $SRV; wait $SRV 2>/dev/null || true
sleep 2
bin/minidb-server > /tmp/minidb-smoke2.log 2>&1 &
SRV2=$!
for i in $(seq 1 30); do
  (exec 3<>/dev/tcp/localhost/8899) 2>/dev/null && break
  sleep 1
done
bin/sqlline -e "SELECT COUNT(*) AS n FROM smoke;" 2>&1 | tail -10
kill $SRV2; wait $SRV2 2>/dev/null || true
```

Expected: 第一次 SELECT 输出 2 行(id/name 两行,含中文「张总」);重启后 COUNT(*) 输出 `n=2`;`/tmp/minidb-smoke.log` 含 `MiniDB listening on port 8899`;`$DIST/logs/minidb.log` 生成;`$DIST/data/` 出现表文件。

(若 `bin/sqlline` 直接交互异常,可改用 `bin/sqlline -e "SELECT 1"` 单条先验证连接;sqlline 的 `-e` 执行后退出。)

- [ ] **Step 7: bat 脚本冒烟(Windows)**

```bash
DIST=$(pwd)/minidb-dist/target/minidb-1.0.0
cd "$DIST"
cmd //c "start /b minidb-server.bat"   # 或另开 cmd 窗口运行 bin\minidb-server.bat
sleep 8
cmd //c "bin\\sqlline.bat -e \"SELECT 1\""   # 或:cmd //c "sqlline.bat -e SELECT 1"
```

Expected: 服务端日志出现监听 8899;sqlline.bat 输出 `1` 行 1 列结果,无 `ClassNotFound`/`NoClassDefFoundError`。
(若 `start /b` 在 git-bash 下不可用,打开独立 cmd 窗口手动执行同样命令验证。)

- [ ] **Step 8: 收尾检查 + 提交**

1. 确认 `git status` 无遗留(target/ 已被 .gitignore 忽略)。
2. 冒烟残留清理:`rm -rf minidb-dist/target/minidb-1.0.0`(target 目录整体会被忽略,不必手动删,仅确认没进 git)。

```bash
git add minidb-dist/src/main/bin
git commit -m "feat: 发行 bin 脚本(minidb-server/sqlline 双平台)与端到端冒烟验证"
```

---

## Self-Review 记录

- **Spec 覆盖核对**:布局(bin/conf/data/jdbc/tools/libs)→ Task 4/5;`server.port` → Task 1;StorageManager 注入 → Task 2;Main 合入 + conf 重载 + exec 插件 → Task 3;发行物三形态 → Task 4 Step 6;双平台脚本与环境变量 → Task 5;测试与冒烟 → Task 1/5。无缺口。
- **占位符扫描**:无 TBD/「适当处理」类描述;每个代码步骤给了完整代码。
- **类型/签名一致性**:`MiniDbConfig.serverPort()`(Task 1)被 Task 3 main 使用;`StorageManager(catalog, allocator, dataDir, config)`(Task 2)被 Task 3 start 使用;`MiniDbServer.start(port, dataDir, confDir)`(Task 3)被 Task 5 脚本经 `Main`→`MiniDbServer.main` 间接使用;脚本入口 `com.minidb.server.MiniDbServer`(Task 3)与 Task 5 脚本一致。sqlline 主类 `sqlline.SqlLine` 与驱动类 `com.minidb.jdbc.MiniDbDriver` 已验证存在。
