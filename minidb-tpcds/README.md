# minidb-tpcds — TPC-DS 基准测试

对 MiniDB 跑 TPC-DS 基准:生成数据、执行 99 条标准查询、存储结果、出对比柱状图。

## 前置条件

- JDK 17、Maven(`./mvnw.cmd`)。
- DSGen 查询模板目录(本机为 `F:\DSGen-software-code-4.0.0\query_templates`),含 `query1.tpl` ~ `query99.tpl`。
- teradata tpcds 数据生成库(`com.teradata.tpcds:tpcds:1.2`)由 Maven 自动下载,无需编译 DSGen 的 C 工具。

## 构建

```bash
./mvnw.cmd -pl minidb-tpcds -am compile
```

## 三个子命令

统一入口 `com.minidb.tpcds.TpcdsBenchmark`,用 Maven exec 或打 jar 运行。以下用 `exec:java` 举例。

### 1. generate — 生成数据

```bash
./mvnw.cmd -pl minidb-tpcds -am exec:java \
  -Dexec.mainClass=com.minidb.tpcds.TpcdsBenchmark \
  -Dexec.args="generate --scale 0.1 --data-dir ./tpcds-data"
```

- `--scale`:数据规模(GB 近似),支持 0.01 / 0.1 / 1 / 10 等任意值,默认 `0.1`。
- `--data-dir`:MiniDB 数据目录,默认 `./data`。
- 结果:24 张表的 Arrow part 文件直接写到 `data/public/<table>/part-*.arrow`,catalog 注册到 `catalog.json`。**不经过 SQL INSERT**(大表逐行插入太慢)。

### 2. run — 执行查询

```bash
./mvnw.cmd -pl minidb-tpcds -am exec:java \
  -Dexec.mainClass=com.minidb.tpcds.TpcdsBenchmark \
  -Dexec.args="run --data-dir ./tpcds-data --query-dir F:/DSGen-software-code-4.0.0/query_templates --scale 0.1 --output ./results/run-1.json"
```

- 先解析 `--query-dir` 下的 99 个 `.tpl` 生成 SQL,再启动 MiniDbServer 逐条跑。
- 每条记录耗时/返回行数/成败;MiniDB 不支持的 SQL(如 `ROLLUP`/`GROUPING SETS`)记录为失败,**不中断后续查询**。
- `--output`:JSON 结果文件。

### 3. compare — 出对比柱状图

```bash
./mvnw.cmd -pl minidb-tpcds -am exec:java \
  -Dexec.mainClass=com.minidb.tpcds.TpcdsBenchmark \
  -Dexec.args="compare ./results/run-1.json ./results/run-2.json --output ./results/report.html"
```

- 读两次 run 的 JSON,生成单个自包含 HTML(Chart.js 分组柱状图):X 轴 = 查询名,每查询两根柱 = 两次耗时,附逐条耗时/行数/失败原因的表格。浏览器打开即可。

## 完整工作流

```bash
# 1. 生成数据(一次)
generate --scale 0.1 --data-dir ./tpcds-data

# 2. 跑测试(可多次,每次一个 JSON)
run --data-dir ./tpcds-data --query-dir F:/DSGen-software-code-4.0.0/query_templates --scale 0.1 --output ./results/run-1.json
run --data-dir ./tpcds-data --query-dir F:/DSGen-software-code-4.0.0/query_templates --scale 0.1 --output ./results/run-2.json

# 3. 对比两次
compare ./results/run-1.json ./results/run-2.json --output ./results/report.html
```

## 结果 JSON 结构

```json
{
  "scale": 0.1,
  "timestamp": "2026-08-17T18:00:00Z",
  "queries": [
    {"name": "query1", "elapsedMs": 123, "rowCount": 100, "success": true, "error": null},
    {"name": "query2", "elapsedMs": -1, "rowCount": -1, "success": false, "error": "ParseException ..."}
  ]
}
```

## 范围与限制

- 只测「能否执行 + 耗时」,**不校验官方 answer set**(结果正确性不在本模块范围)。
- 查询生成用简化求值(固定 seed,可复现),不追求与官方 qgen 的精确分布一致。
- 数据生成单线程;不自动 compaction(part 数可能较多)。
