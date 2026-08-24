package com.minidb.tpcds;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlamegraphWriterTest {

    @Test
    void generatesHtmlFromJfrRecording(@TempDir Path tmpDir) throws Exception {
        Path jfrFile = tmpDir.resolve("test.jfr");
        Path htmlFile = tmpDir.resolve("flame.html");

        recordJfr(jfrFile);
        assertTrue(Files.size(jfrFile) > 0, "JFR 文件不应为空");

        new FlamegraphWriter().write(jfrFile, htmlFile);

        String html = Files.readString(htmlFile);
        assertFalse(html.isBlank(), "HTML 不应为空");
        assertTrue(html.contains("<!DOCTYPE html>"), "应为 HTML 文档");
        assertTrue(html.contains("d3-flame-graph"), "应包含火焰图库引用");
        assertTrue(html.contains("FlamegraphWriterTest") || html.contains("doWork"),
                "应包含测试类名或 doWork 栈帧");
    }

    @Test
    void htmlIsSelfContained(@TempDir Path tmpDir) throws Exception {
        Path jfrFile = tmpDir.resolve("empty.jfr");
        Path htmlFile = tmpDir.resolve("flame.html");

        recordJfr(jfrFile);

        new FlamegraphWriter().write(jfrFile, htmlFile);

        String html = Files.readString(htmlFile);
        assertTrue(html.contains("cdn.jsdelivr.net") || html.contains("d3-flame-graph"),
                "应通过 CDN 引用火焰图库");
        assertFalse(html.contains("file://"), "不应有本地文件引用");
    }

    @Test
    void containsFoldedStackFormat(@TempDir Path tmpDir) throws Exception {
        Path jfrFile = tmpDir.resolve("test.jfr");
        Path htmlFile = tmpDir.resolve("flame.html");

        recordJfr(jfrFile);

        new FlamegraphWriter().write(jfrFile, htmlFile);

        String html = Files.readString(htmlFile);
        assertTrue(html.contains("FlamegraphWriterTest") || html.contains("doWork"),
                "应包含测试方法名");
    }

    private static void recordJfr(Path jfrFile) throws Exception {
        jdk.jfr.Recording recording = new jdk.jfr.Recording();
        recording.setDestination(jfrFile);
        recording.setSettings(Map.of(
                "jdk.ExecutionSample#enabled", "true",
                "jdk.ExecutionSample#period", "1 ms"
        ));
        recording.start();
        doWork();
        recording.stop();
        recording.close();
    }

    private static void doWork() {
        // 产生足够多的 CPU 活动,让 JFR 采样到有意义的栈帧(不会被 JIT 完全消除)
        long sum = 0;
        for (int i = 0; i < 10_000_000; i++) {
            sum += i * i;
        }
        // 防止被优化掉
        if (sum == -1) {
            System.out.println("unreachable");
        }
    }
}