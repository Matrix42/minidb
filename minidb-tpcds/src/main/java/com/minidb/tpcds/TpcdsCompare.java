package com.minidb.tpcds;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 对比多次 TPC-DS 运行结果,生成单个自包含 HTML(Chart.js 分组柱状图):
 * X 轴 = 查询名,每查询 N 根柱(N 次运行),附逐条耗时/行数/失败原因的表格。
 */
public class TpcdsCompare {

    private static final String[] COLORS = {
            "#4c6ef5", "#f59f00", "#40c057", "#fa5252", "#ae3ec9",
            "#15aabf", "#f783ac", "#fab005", "#7950f2", "#82c91e"
    };

    public record NamedRun(String name, Path path) {
    }

    public void compare(List<NamedRun> runs, Path outputHtml) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        // 解析每个 run 的最终名称
        List<String> names = new ArrayList<>();
        List<Map<String, Result>> allResults = new ArrayList<>();
        for (NamedRun run : runs) {
            JsonNode root = mapper.readTree(run.path.toFile());
            String runName = run.name;
            if (runName == null || runName.isEmpty()) {
                runName = root.path("name").asText(null);
                if (runName == null || runName.isEmpty()) {
                    runName = run.path.getFileName().toString();
                }
            }
            names.add(runName);
            allResults.add(readResults(root));
        }

        int n = names.size();

        // 按查询号排序对齐
        TreeMap<String, long[]> aligned = new TreeMap<>((a, b) -> Integer.compare(
                queryNumber(a), queryNumber(b)));
        for (int i = 0; i < n; i++) {
            int idx = i;
            for (String qName : allResults.get(i).keySet()) {
                long[] times = aligned.computeIfAbsent(qName, k -> {
                    long[] arr = new long[n];
                    for (int j = 0; j < arr.length; j++) {
                        arr[j] = -1;
                    }
                    return arr;
                });
                times[idx] = allResults.get(idx).get(qName).elapsedMs();
            }
        }

        // 构建 JS 数据
        StringBuilder labels = new StringBuilder();
        StringBuilder[] datasets = new StringBuilder[n];
        for (int i = 0; i < n; i++) {
            datasets[i] = new StringBuilder();
        }

        StringBuilder tableRows = new StringBuilder();
        for (Map.Entry<String, long[]> e : aligned.entrySet()) {
            String qName = e.getKey();
            long[] times = e.getValue();
            if (labels.length() > 0) {
                labels.append(", ");
                for (int i = 0; i < n; i++) {
                    datasets[i].append(", ");
                }
            }
            labels.append('"').append(qName).append('"');
            for (int i = 0; i < n; i++) {
                datasets[i].append(times[i] >= 0 ? times[i] : 0);
            }

            tableRows.append("<tr><td>").append(qName).append("</td>");
            for (int i = 0; i < n; i++) {
                tableRows.append("<td>").append(fmt(times[i])).append("</td>");
            }
            tableRows.append("<td>").append(escape(errorFor(allResults, qName)))
                    .append("</td></tr>\n");
        }

        // 构建 Chart.js datasets
        StringBuilder dsJson = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                dsJson.append(", ");
            }
            dsJson.append("{label: '").append(escapeJs(names.get(i)))
                    .append("', data: [").append(datasets[i])
                    .append("], backgroundColor: '").append(COLORS[i % COLORS.length])
                    .append("'}");
        }

        // 表格头
        StringBuilder th = new StringBuilder("<tr><th>查询</th>");
        for (String name : names) {
            th.append("<th>").append(escape(name)).append("(ms)</th>");
        }
        th.append("<th>失败原因</th></tr>");

        String html = """
                <!DOCTYPE html>
                <html>
                <head>
                <meta charset="utf-8">
                <title>TPC-DS 对比</title>
                <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
                </head>
                <body>
                <h2>TPC-DS 查询耗时对比(毫秒)</h2>
                <canvas id="chart" height="120"></canvas>
                <table border="1" cellspacing="0" cellpadding="4">
                %s
                %s
                </table>
                <script>
                new Chart(document.getElementById('chart'), {
                  type: 'bar',
                  data: {
                    labels: [%s],
                    datasets: [%s]
                  },
                  options: {scales: {y: {beginAtZero: true, title: {display: true, text: 'ms'}}}}
                });
                </script>
                </body>
                </html>
                """.formatted(th, tableRows, labels, dsJson);

        Files.writeString(outputHtml, html);
    }

    private record Result(long elapsedMs, String error) {
    }

    private Map<String, Result> readResults(JsonNode root) {
        Map<String, Result> map = new LinkedHashMap<>();
        for (JsonNode q : root.path("queries")) {
            String name = q.path("name").asText();
            long elapsed = q.path("elapsedMs").asLong(-1);
            String error = q.path("error").isNull() ? "" : q.path("error").asText("");
            map.put(name, new Result(elapsed, error));
        }
        return map;
    }

    private static String errorFor(List<Map<String, Result>> allResults, String qName) {
        for (Map<String, Result> r : allResults) {
            Result res = r.get(qName);
            if (res != null && res.error() != null && !res.error().isEmpty()) {
                return res.error();
            }
        }
        return "";
    }

    private static int queryNumber(String name) {
        try {
            return Integer.parseInt(name.substring("query".length()));
        } catch (RuntimeException e) {
            return 0;
        }
    }

    private static String fmt(long ms) {
        return ms < 0 ? "失败" : String.valueOf(ms);
    }

    private static String escape(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static String escapeJs(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("'", "\\'");
    }
}
