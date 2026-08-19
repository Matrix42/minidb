package com.minidb.tpcds;

import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableType;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * TPC-DS 基准 CLI 入口,三个子命令:
 * <pre>
 *   generate --scale 0.1 --data-dir ./data
 *   run      --data-dir ./data --scale 0.1 --output ./results/run.json  [--direct] [--query-dir <dir>]
 *   compare  run-1.json run-2.json --output ./results/report.html
 * </pre>
 */
public class TpcdsBenchmark {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        String command = args[0];
        switch (command) {
            case "generate" -> {
                Map<String, String> opts = parseOptions(args, 1);
                double scale = Double.parseDouble(opts.getOrDefault("scale", "0.1"));
                Path dataDir = Path.of(opts.getOrDefault("data-dir", "./data"));
                new TpcdsDataGenerator().generate(scale, dataDir, StorageFormat.PARQUET, TableType.LSM);
            }
            case "run" -> {
                Map<String, String> opts = parseOptions(args, 1);
                double scale = Double.parseDouble(opts.getOrDefault("scale", "0.1"));
                Path dataDir = Path.of(opts.getOrDefault("data-dir", "./data"));
                Path output = Path.of(opts.getOrDefault("output", "./results/run.json"));
                TpcdsTemplateParser parser = new TpcdsTemplateParser();
                // --query-dir 可选:缺省用模块内置的 99 个模板(无需外部 DSGen 工具)。
                Map<String, String> queries = opts.containsKey("query-dir")
                        ? parser.parseAll(Path.of(opts.get("query-dir")), scale)
                        : parser.parseBundled(scale);
                TpcdsBenchmarkRunner runner = new TpcdsBenchmarkRunner();
                if (opts.containsKey("direct")) {
                    // --direct:直接用 QueryExecutor 执行,不走 MiniDbServer/JDBC 网络层。
                    runner.runDirect(queries, dataDir, output, scale);
                } else {
                    runner.run(queries, dataDir, output, scale);
                }
            }
            case "compare" -> {
                if (args.length < 3) {
                    usage();
                    return;
                }
                Path run1 = Path.of(args[1]);
                Path run2 = Path.of(args[2]);
                Map<String, String> opts = parseOptions(args, 3);
                Path output = Path.of(opts.getOrDefault("output", "./results/report.html"));
                new TpcdsCompare().compare(run1, run2, output);
            }
            default -> usage();
        }
    }

    private static Map<String, String> parseOptions(String[] args, int start) {
        Map<String, String> opts = new HashMap<>();
        for (int i = start; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                // 布尔标志(如 --direct):无后续值。
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    opts.put(key, args[i + 1]);
                    i++;
                } else {
                    opts.put(key, "true");
                }
            }
        }
        return opts;
    }

    private static void usage() {
        System.out.println("""
                用法:
                  generate --scale 0.1 --data-dir ./data
                  run      --data-dir ./data [--direct] [--query-dir <dir>] --scale 0.1 --output ./results/run.json
                  compare  run-1.json run-2.json --output ./results/report.html
                """);
    }
}
