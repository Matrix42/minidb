package com.minidb.tpcds;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** 从 JFR 录制文件生成自包含火焰图 HTML。 使用 d3-flame-graph 渲染(CDN 加载,与 TpcdsCompare 的 Chart.js CDN 模式一致)。 */
public class FlamegraphWriter {

    public void write(Path jfrFile, Path htmlFile) throws IOException {
        // 1. 从 JFR 提取执行采样,转为 folded stack 格式
        Map<String, Long> foldedStacks = extractFoldedStacks(jfrFile);

        // 2. 生成 HTML
        String html = buildHtml(foldedStacks);
        if (htmlFile.getParent() != null) {
            Files.createDirectories(htmlFile.getParent());
        }
        Files.writeString(htmlFile, html);
    }

    /**
     * 从 JFR 录制文件提取 {@code jdk.ExecutionSample} 事件, 转为 folded stack 格式: {@code func1;func2;func3
     * count}。
     */
    Map<String, Long> extractFoldedStacks(Path jfrFile) throws IOException {
        Map<String, Long> stacks = new LinkedHashMap<>();

        try (RecordingFile recording = new RecordingFile(jfrFile)) {
            while (recording.hasMoreEvents()) {
                RecordedEvent event = recording.readEvent();
                if (event == null) {
                    continue;
                }
                if (!"jdk.ExecutionSample".equals(event.getEventType().getName())) {
                    continue;
                }
                List<RecordedFrame> frames = event.getStackTrace().getFrames();
                if (frames.isEmpty()) {
                    continue;
                }
                // 栈帧从底(入口)到顶(当前方法),火焰图格式从底到顶用分号分隔
                // 采样时栈顶最具体,我们反转使 root 在左
                // 标准 folded stack: bottom;...;top count
                StringBuilder sb = new StringBuilder();
                for (int i = frames.size() - 1; i >= 0; i--) {
                    if (!sb.isEmpty()) {
                        sb.append(';');
                    }
                    sb.append(formatFrame(frames.get(i)));
                }
                String key = sb.toString();
                stacks.merge(key, 1L, Long::sum);
            }
        }
        return stacks;
    }

    private static String formatFrame(RecordedFrame frame) {
        RecordedMethod method = frame.getMethod();
        if (method != null) {
            String className = method.getType().getName();
            // 简化包名: 去 java.base/ 等模块前缀,截短 com.minidb 等长包名
            String shortName = simplifyClassName(className);
            return shortName + "." + method.getName();
        }
        return "?";
    }

    private static String simplifyClassName(String className) {
        // 去模块前缀如 "jdk.jfr/" / "java.base/"
        String s = className.replaceFirst("^[^/]+/", "");
        // 把包名缩写: com.minidb.server.exec → c.m.s.exec
        s = s.replace("com.minidb.", "c.m.");
        return s;
    }

    private String buildHtml(Map<String, Long> foldedStacks) {
        // foldedStacks 按 count 降序排列
        String data =
                foldedStacks.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .map(e -> e.getKey() + " " + e.getValue())
                        .collect(Collectors.joining("\\n"));

        return """
                <!DOCTYPE html>
                <html>
                <head>
                <meta charset="utf-8">
                <title>TPC-DS Flame Graph</title>
                <style>
                body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
                #header { padding: 12px 16px; background: #1a1a2e; color: #e0e0e0; }
                #header h2 { margin: 0; font-size: 18px; }
                #header span { font-size: 12px; color: #888; }
                #chart { width: 100%%; }
                #search { margin: 8px 16px; padding: 6px 12px; width: 300px; border: 1px solid #444;
                          background: #1a1a2e; color: #e0e0e0; border-radius: 4px; font-size: 14px; }
                #search::placeholder { color: #666; }
                .tooltip { position: absolute; background: rgba(0,0,0,0.85); color: #fff; padding: 6px 10px;
                           border-radius: 4px; font-size: 12px; pointer-events: none; }
                </style>
                <script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
                <script src="https://cdn.jsdelivr.net/npm/d3-flame-graph@4.1.3/dist/d3-flamegraph.min.js"></script>
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/d3-flame-graph@4.1.3/dist/d3-flamegraph.css">
                </head>
                <body>
                <div id="header">
                  <h2>TPC-DS Benchmark Flame Graph</h2>
                  <span>Samples by method — click to zoom, search to highlight</span>
                </div>
                <input id="search" type="text" placeholder="Search methods..." oninput="search(this.value)">
                <div id="chart"></div>
                <script>
                // 将 folded stack 格式转换为 d3-flame-graph 需要的层级树
                const raw = `%s`;
                const root = { name: "root", value: 0, children: [] };
                const lines = raw.trim().split("\\n").filter(l => l);
                for (const line of lines) {
                  const lastSpace = line.lastIndexOf(" ");
                  const stack = line.substring(0, lastSpace);
                  const count = +line.substring(lastSpace + 1);
                  const frames = stack.split(";");
                  let node = root;
                  node.value += count;
                  for (const frame of frames) {
                    let child = node.children.find(c => c.name === frame);
                    if (!child) {
                      child = { name: frame, value: 0, children: [] };
                      node.children.push(child);
                    }
                    child.value += count;
                    node = child;
                  }
                }

                const fg = flamegraph()
                  .width(window.innerWidth - 16)
                  .cellHeight(18)
                  .transitionDuration(500)
                  .transitionEase(d3.easeCubicOut)
                  .sort(true)
                  .selfValue(false)
                  .onClick(d => { search(d.data.name); return true; });

                d3.select("#chart").datum(root).call(fg);

                window.addEventListener("resize", () => {
                  fg.width(window.innerWidth - 16);
                  d3.select("#chart").call(fg);
                });

                function search(term) {
                  const lower = term.toLowerCase();
                  fg.search(lower);
                  d3.select("#chart").call(fg);
                }
                </script>
                </body>
                </html>
                """
                .formatted(data);
    }
}
