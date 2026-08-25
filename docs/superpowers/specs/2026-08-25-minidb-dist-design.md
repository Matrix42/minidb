# minidb-dist 发行模块设计

日期:2026-08-25 · 状态:已评审

## 背景与目标

MiniDB 目前只能通过 `./mvnw.cmd -pl minidb-server exec:java` 启动,客户端依赖 Maven 构建产物,没有可直接分发、开箱即用的发行包。本设计新增 `minidb-dist` 模块,把服务端、JDBC 驱动、命令行客户端(sqlline)组装成 Flink/Spark 风格的发行目录,并产出 `tar.gz`(linux)与 `zip`(windows)归档。

**用户已确认的关键决策**:

1. 配置从 `data/config.yaml` 真迁移到 `conf/config.yaml`(data 目录纯数据),`server.port` 收进配置。
2. 发行物 = 目录 + tar.gz + zip 三种形态。
3. sqlline 自写启动脚本进 `bin/`(`sqlline` / `sqlline.bat`),jar 单独放 `tools/`(不放 libs)。
4. `com.minidb.server.Main` 合入 `MiniDbServer`(main() 搬进去,删 Main.java)。

## 发行目录布局

`minidb-dist/target/minidb-1.0.0/`(归档同名,后缀 `.tar.gz` / `.zip`):

```
minidb-1.0.0/
├── bin/
│   ├── minidb-server        # sh:启动服务端
│   ├── minidb-server.bat    # windows 版
│   ├── sqlline              # sh:命令行客户端
│   └── sqlline.bat          # windows 版
├── conf/
│   ├── config.yaml          # server.port + lsm/compaction 模板(带注释,与 data 解耦)
│   └── log4j2.properties    # 日志配置(源自 minidb-server resources 版本)
├── data/                    # 空目录(README 占位),数据落这里;catalog.json 也在这里
├── jdbc/
│   └── minidb-jdbc-1.0.0.jar  # shade 后主产物(自包含 netty/arrow)
├── tools/
│   ├── sqlline-1.12.0.jar
│   └── jline-*.jar          # jline-terminal/reader/console/builtins + jna/jansi 等传递依赖
├── libs/                    # minidb-server 全部运行时依赖(不含 sqlline、不含 jdbc)
└── README.md                # 布局说明 + 快速上手
```

各目录职责:`libs/` = 服务端运行时依赖;`jdbc/` = 打包好的 JDBC 驱动(客户端 classpath 入口);`tools/` = 客户端工具 sqlline 及其依赖;`conf/` = 唯一配置来源;`data/` = 纯数据;`bin/` = 双平台启动脚本。

## 服务端改动

### `config/MiniDbConfig`

- 新增 `server.port` 键(默认 8899)+ `serverPort()` getter + `DEFAULT_SERVER_PORT` 常量。
- 解析逻辑:`server.port` 存在且 > 0 时生效。

### `storage/StorageManager`

- 新构造器 `StorageManager(MiniDbCatalog, BufferAllocator, Path dataDir, MiniDbConfig config)`:`this.config = config`(不再内部 `MiniDbConfig.load`),其余不变。
- 旧 3 参构造器 `(catalog, allocator, dataDir)` 保留并委托新构造器:`this(..., MiniDbConfig.load(dataDir))`——现有测试与调用零改动,行为不变。

### `MiniDbServer`

- 新增重载 `start(int port, Path dataDir, Path confDir)`:配置从 `MiniDbConfig.load(confDir)` 读,传入 StorageManager 新构造器;其余逻辑与现有 `start` 相同。
- 旧 `start(int port, Path dataDir)` 保留并委托:`this(port, dataDir, dataDir)`(backward compat,读 `data/config.yaml`)。
- **合入 main()**:新增 `public static void main(String[] args)`(逻辑从 `Main` 迁入):
  - 参数解析:`--port`(覆盖配置)、`--data`(默认 `data`)、`--conf`(默认 `conf`)。
  - port 优先级:`--port` 参数 > conf/config.yaml 的 `server.port` > 8899。
  - 启动流程沿用现有 Main:`new MiniDbServer()` → `start(...)` → shutdown hook(close)→ `Thread.currentThread().join()`。

### 删除 `Main.java`,更新 `minidb-server/pom.xml`

- 删 `src/main/java/com/minidb/server/Main.java`。
- exec 插件 `<argument>com.minidb.server.Main</argument>` 改为 `com.minidb.server.MiniDbServer`(`./mvnw.cmd -pl minidb-server exec:java` 与发行脚本共用同一入口)。

## minidb-dist 模块

`minidb-dist/pom.xml`,packaging = `pom`(无源码,纯组装)。加入父 pom `<modules>`;父 pom dependencyManagement 增 `sqlline` 与 `<sqlline.version>1.12.0</sqlline.version>` 属性。

**依赖(runtime)**:`com.minidb:minidb-server`、`com.minidb:minidb-jdbc`、`sqlline:sqlline:1.12.0`。

**maven-assembly-plugin**(单 descriptor `src/main/assembly/dist.xml`,`formats = dir + tar.gz + zip`,finalName `minidb-1.0.0`):

| 目标目录 | 来源 | 说明 |
|---|---|---|
| `libs/` | dependencySet,`<scope>runtime</scope>`,includes `com.minidb:minidb-server`(不设 useTransitiveFiltering) | server 全传递依赖闭包 |
| `jdbc/` | dependencySet,`<scope>runtime</scope>`,includes `com.minidb:minidb-jdbc`,**useTransitiveFiltering=true** | 仅 shade 后主产物(reduced-pom 无 runtime 依赖,不会带 netty/arrow 冗余) |
| `tools/` | dependencySet,`<scope>runtime</scope>`,includes `sqlline:sqlline`(不设 useTransitiveFiltering) | sqlline + jline 系列传递依赖 |
| `conf/` | fileSet ← `src/main/conf/` | config.yaml 模板 + log4j2.properties |
| `bin/` | fileSet ← `src/main/bin/`,`<fileMode>755</fileMode>` | 4 个脚本 |
| `data/` | fileSet ← `src/main/data/`(含 README 占位) | 空数据目录 |
| `README.md` | file ← `src/main/README.md` | 使用说明 |

**注意点**:

- 三个 dependencySet 互不重叠(server 不依赖 sqlline/jline;jdbc 经 useTransitiveFiltering 隔离),`libs/` 不会混入客户端工具。
- reactor 构建时 `minidb-jdbc` 取到的是 shade 后主产物(与 `minidb-jdbc/target/minidb-jdbc-1.0.0.jar` 同构)。
- 脚本从 `src/main/bin/` 进 dist,不参与编译(无 java 源码)。

**构建命令**:`./mvnw.cmd -pl minidb-dist -am package`(需 `-am` 先构建依赖链;`minidb-jdbc` 的 shade 在 package 阶段,reactor 顺序保证 dist 拿到 shade 产物)。

## 启动脚本

双平台脚本均:
- 从自身位置推导 `MINIDB_HOME`(bash:`BIN_DIR=$(cd "$(dirname "$0")" && pwd); MINIDB_HOME=$(dirname "$BIN_DIR")`;bat:`%~dp0` + `cd`),任意目录可运行。
- `JAVA_HOME` 存在则用 `$JAVA_HOME/bin/java`,否则回退 PATH 上的 `java`,缺失时打印错误退出。
- 支持 `MINIDB_JAVA_OPTS` 透传额外 JVM 参数(默认空)。

### `bin/minidb-server` / `.bat`

```
cd "$MINIDB_HOME"
java $MINIDB_JAVA_OPTS \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -Dlog4j2.configurationFile="$MINIDB_HOME/conf/log4j2.properties" \
  -cp "$MINIDB_HOME/libs/*" \
  com.minidb.server.MiniDbServer \
  --data "$MINIDB_HOME/data" --conf "$MINIDB_HOME/conf"
```

- 日志落 `$MINIDB_HOME/logs/minidb.log`(log4j 相对路径依赖 CWD=MINIDB_HOME)。
- 支持 `MINIDB_PORT` 环境变量 → `--port`(可选,便于脚本化;未设则不传,走 conf)。
- Ctrl+C / kill 走 shutdown hook 优雅关闭。

### `bin/sqlline` / `.bat`

```
java $MINIDB_JAVA_OPTS \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -cp "$MINIDB_HOME/tools/*:$MINIDB_HOME/jdbc/*" \
  sqlline.SqlLine -d com.minidb.jdbc.MiniDbDriver \
  -u "${MINIDB_URL:-jdbc:minidb://${MINIDB_HOST:-localhost}:${MINIDB_PORT:-8899}}"
```

- add-opens 取服务端同款超集(客户端实际只需 java.nio 的 ALL-UNNAMED,超集无害)。
- `MINIDB_URL` 可整体覆盖连接串;`MINIDB_HOST`/`MINIDB_PORT` 拼默认串。

## 测试与验证

- **单测**:`MiniDbConfigTest` 补 `server.port` 解析用例(默认值 / 显式值 / 缺文件回退)。StorageManager/MiniDbServer 新构造器由既有测试编译 + 运行兜底(旧构造器委托,行为不变)。
- **全量回归**:`./mvnw.cmd test`。
- **打包冒烟**(手工):
  1. `./mvnw.cmd -pl minidb-dist -am package`;
  2. 解压 `target/minidb-1.0.0.tar.gz` 与 `zip`(各自验证目录结构与文件权限);
  3. 起 `bin/minidb-server` → 确认 8899 监听、`logs/minidb.log` 生成;
  4. `bin/sqlline` 连接,执行 `CREATE TABLE t(id INT)` / `INSERT` / `SELECT` 全链路;
  5. 重启后数据仍在(`data/` 持久化);
  6. 停服 Ctrl+C 优雅退出。
- bat 版本在 Windows 上同样跑一遍 3–6。

## 不做的事(YAGNI)

- 不做 stop 脚本(单机进程,shutdown hook 足够)。
- 不做 `bin/minidb-cli` 等额外脚本(用户只要 sqlline)。
- 不做 CI 打包 job(本地纯仓库)。
- 不在 minidb-dist 内写 Java 测试(构建期产物,手工冒烟覆盖)。
