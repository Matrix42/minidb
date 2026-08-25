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