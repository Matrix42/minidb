package com.minidb.tpcds;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * TPC-DS 基准 CLI 入口,三个子命令:
 * <pre>
 *   generate --scale 0.1 --data-dir ./data
 *   run      --data-dir ./data --query-dir F:/DSGen-software-code-4.0.0/query_templates --scale 0.1 --output ./results/run.json
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
                new TpcdsDataGenerator().generate(scale, dataDir);
            }
            case "run" -> {
                Map<String, String> opts = parseOptions(args, 1);
                double scale = Double.parseDouble(opts.getOrDefault("scale", "0.1"));
                Path dataDir = Path.of(opts.getOrDefault("data-dir", "./data"));
                Path queryDir = Path.of(opts.getOrDefault("query-dir", "./query_templates"));
                Path output = Path.of(opts.getOrDefault("output", "./results/run.json"));
                Map<String, String> queries = new TpcdsTemplateParser().parseAll(queryDir, scale);
                new TpcdsBenchmarkRunner().run(queries, dataDir, output, scale);
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
            if (args[i].startsWith("--") && i + 1 < args.length) {
                opts.put(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
        return opts;
    }

    private static void usage() {
        System.out.println("""
                用法:
                  generate --scale 0.1 --data-dir ./data
                  run      --data-dir ./data --query-dir <query_templates> --scale 0.1 --output ./results/run.json
                  compare  run-1.json run-2.json --output ./results/report.html
                """);
    }
}
