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
 * 对比两次 TPC-DS 运行结果,生成单个自包含 HTML(Chart.js 分组柱状图):
 * X 轴 = 查询名,每查询两根柱(两次耗时),附逐条耗时/行数/失败原因的表格。
 */
public class TpcdsCompare {

    public void compare(Path run1, Path run2, Path outputHtml) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Result> r1 = readResults(mapper.readTree(run1.toFile()));
        Map<String, Result> r2 = readResults(mapper.readTree(run2.toFile()));

        TreeMap<String, long[]> aligned = new TreeMap<>((a, b) -> Integer.compare(
                queryNumber(a), queryNumber(b)));
        for (String name : r1.keySet()) {
            aligned.computeIfAbsent(name, k -> new long[2])[0] = r1.get(name).elapsedMs();
        }
        for (String name : r2.keySet()) {
            aligned.computeIfAbsent(name, k -> new long[2])[1] = r2.get(name).elapsedMs();
        }

        StringBuilder labels = new StringBuilder();
        StringBuilder data1 = new StringBuilder();
        StringBuilder data2 = new StringBuilder();
        StringBuilder tableRows = new StringBuilder();
        for (Map.Entry<String, long[]> e : aligned.entrySet()) {
            String name = e.getKey();
            long[] times = e.getValue();
            if (labels.length() > 0) {
                labels.append(", ");
                data1.append(", ");
                data2.append(", ");
            }
            labels.append('"').append(name).append('"');
            data1.append(times[0] > 0 ? times[0] : 0);
            data2.append(times[1] > 0 ? times[1] : 0);
            tableRows.append("<tr><td>").append(name).append("</td>")
                    .append("<td>").append(fmt(times[0])).append("</td>")
                    .append("<td>").append(fmt(times[1])).append("</td>")
                    .append("<td>").append(escape(r1.get(name) != null ? r1.get(name).error() : ""))
                    .append("</td></tr>\n");
        }

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
                <tr><th>查询</th><th>第 1 次(ms)</th><th>第 2 次(ms)</th><th>失败原因</th></tr>
                %s
                </table>
                <script>
                new Chart(document.getElementById('chart'), {
                  type: 'bar',
                  data: {
                    labels: [%s],
                    datasets: [
                      {label: '第 1 次', data: [%s], backgroundColor: '#4c6ef5'},
                      {label: '第 2 次', data: [%s], backgroundColor: '#f59f00'}
                    ]
                  },
                  options: {scales: {y: {beginAtZero: true, title: {display: true, text: 'ms'}}}}
                });
                </script>
                </body>
                </html>
                """.formatted(tableRows, labels, data1, data2);

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
}
