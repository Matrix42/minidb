package com.minidb.server.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * 数据库配置。从 {@code data/config.yaml} 加载(YAML 格式),缺文件或缺键时回退默认值。
 *
 * <pre>
 * compaction:
 *   target-size-mb: 128
 *   auto-part-threshold: 16
 * </pre>
 */
public final class MiniDbConfig {

    public static final long DEFAULT_COMPACTION_TARGET_SIZE_BYTES = 128L * 1024 * 1024;
    public static final int DEFAULT_COMPACTION_AUTO_PART_THRESHOLD = 16;

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final long compactionTargetSizeBytes;
    private final int compactionAutoPartThreshold;

    private MiniDbConfig(long compactionTargetSizeBytes, int compactionAutoPartThreshold) {
        this.compactionTargetSizeBytes = compactionTargetSizeBytes;
        this.compactionAutoPartThreshold = compactionAutoPartThreshold;
    }

    public long compactionTargetSizeBytes() {
        return compactionTargetSizeBytes;
    }

    public int compactionAutoPartThreshold() {
        return compactionAutoPartThreshold;
    }

    public static MiniDbConfig load(Path dataDir) {
        long targetBytes = DEFAULT_COMPACTION_TARGET_SIZE_BYTES;
        int autoThreshold = DEFAULT_COMPACTION_AUTO_PART_THRESHOLD;
        Path file = dataDir.resolve("config.yaml");
        if (Files.exists(file)) {
            Map<String, Object> compaction = asMap(readYaml(file).get("compaction"));
            Long targetMb = asLong(compaction == null ? null : compaction.get("target-size-mb"));
            if (targetMb != null && targetMb > 0) {
                targetBytes = targetMb * 1024 * 1024;
            }
            Integer threshold = asInt(compaction == null ? null : compaction.get("auto-part-threshold"));
            if (threshold != null && threshold > 0) {
                autoThreshold = threshold;
            }
        }
        return new MiniDbConfig(targetBytes, autoThreshold);
    }

    private static Map<String, Object> readYaml(Path file) {
        try {
            return YAML.readValue(file.toFile(), new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load config: " + file, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return value instanceof Map ? (Map<String, Object>) value : null;
    }

    private static Long asLong(Object value) {
        return value instanceof Number n ? n.longValue() : null;
    }

    private static Integer asInt(Object value) {
        return value instanceof Number n ? n.intValue() : null;
    }
}
