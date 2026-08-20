package com.minidb.tpcds;

import com.minidb.storage.common.StorageFormat;
import com.minidb.storage.common.TableType;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TPC-DS 基准 CLI 入口,三个子命令:
 * <pre>
 *   generate --scale 0.1 --data-dir ./data
 *   run      --data-dir ./data --scale 0.1 --output ./results/run.json  [--direct] [--query-dir <dir>] [--name <name>]
 *   compare  file1.json [file2.json ...] --output ./results/report.html
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
                String name = opts.getOrDefault("name", null);
                TpcdsTemplateParser parser = new TpcdsTemplateParser();
                Map<String, String> queries = opts.containsKey("query-dir")
                        ? parser.parseAll(Path.of(opts.get("query-dir")), scale)
                        : parser.parseBundled(scale);
                TpcdsBenchmarkRunner runner = new TpcdsBenchmarkRunner();
                if (opts.containsKey("direct")) {
                    runner.runDirect(queries, dataDir, output, scale, name);
                } else {
                    runner.run(queries, dataDir, output, scale, name);
                }
            }
            case "compare" -> {
                if (args.length < 3) {
                    usage();
                    return;
                }
                // 收集所有位置参数(JSON 文件)和 --output
                List<Path> files = new ArrayList<>();
                Path output = Path.of("./results/report.html");
                for (int i = 1; i < args.length; i++) {
                    if (args[i].startsWith("--")) {
                        String key = args[i].substring(2);
                        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                            if ("output".equals(key)) {
                                output = Path.of(args[i + 1]);
                            }
                            i++;
                        }
                    } else {
                        files.add(Path.of(args[i]));
                    }
                }
                if (files.isEmpty()) {
                    System.err.println("至少需要一个 JSON 文件");
                    usage();
                    return;
                }
                List<TpcdsCompare.NamedRun> runs = new ArrayList<>();
                for (Path f : files) {
                    runs.add(new TpcdsCompare.NamedRun(null, f));
                }
                new TpcdsCompare().compare(runs, output);
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
                  run      --data-dir ./data [--direct] [--query-dir <dir>] [--name <name>] --scale 0.1 --output ./results/run.json
                  compare  file1.json [file2.json ...] --output ./results/report.html
                """);
    }
}
