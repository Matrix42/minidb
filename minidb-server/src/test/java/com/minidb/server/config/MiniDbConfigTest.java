package com.minidb.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MiniDbConfigTest {

    @Test
    void defaultsWhenNoFile(@TempDir Path dir) {
        MiniDbConfig config = MiniDbConfig.load(dir);
        assertEquals(128L * 1024 * 1024, config.compactionTargetSizeBytes());
        assertEquals(16, config.compactionAutoPartThreshold());
    }

    @Test
    void loadsYaml(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("config.yaml"),
                "compaction:\n  target-size-mb: 64\n  auto-part-threshold: 5\n");
        MiniDbConfig config = MiniDbConfig.load(dir);
        assertEquals(64L * 1024 * 1024, config.compactionTargetSizeBytes());
        assertEquals(5, config.compactionAutoPartThreshold());
    }

    @Test
    void partialYamlFallsBackToDefault(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("config.yaml"),
                "compaction:\n  target-size-mb: 32\n");
        MiniDbConfig config = MiniDbConfig.load(dir);
        assertEquals(32L * 1024 * 1024, config.compactionTargetSizeBytes());
        assertEquals(16, config.compactionAutoPartThreshold());
    }
}
