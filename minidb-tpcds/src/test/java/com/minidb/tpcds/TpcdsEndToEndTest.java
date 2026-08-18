package com.minidb.tpcds;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpcdsEndToEndTest {

    @Test
    void endToEnd(@TempDir Path dataDir) throws Exception {
        // 1. 生成数据(小 scale)
        new TpcdsDataGenerator().generate(0.01, dataDir);

        // 2. 解析内置 99 个查询,只跑前 10 个保持测试快
        Map<String, String> all = new TpcdsTemplateParser().parseBundled(0.01);
        Map<String, String> subset = new LinkedHashMap<>();
        int i = 0;
        for (Map.Entry<String, String> e : all.entrySet()) {
            if (i++ >= 10) {
                break;
            }
            subset.put(e.getKey(), e.getValue());
        }
        assertEquals(10, subset.size());

        // 3. 跑查询
        Path runJson = dataDir.resolve("run.json");
        new TpcdsBenchmarkRunner().run(subset, dataDir, runJson, 0.01);
        JsonNode root = new ObjectMapper().readTree(runJson.toFile());
        assertEquals(10, root.path("queries").size());

        // 4. 对比(同文件两次,验证 HTML 生成)
        Path report = dataDir.resolve("report.html");
        new TpcdsCompare().compare(runJson, runJson, report);
        String html = Files.readString(report);
        assertTrue(html.contains("Chart"), "应含 Chart.js");
        assertTrue(html.contains("canvas"), "应含 canvas");
    }
}
