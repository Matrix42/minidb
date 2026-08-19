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
 * lsm:
 *   memtable-size-mb: 64
 *   l0-file-limit: 4
 *   level-size-multiplier: 10
 *   wal-fsync: false
 *   background-interval-ms: 1000
 *   bloom-bits-per-key: 10
 * </pre>
 */
public final class MiniDbConfig {

    public static final long DEFAULT_COMPACTION_TARGET_SIZE_BYTES = 128L * 1024 * 1024;
    public static final int DEFAULT_COMPACTION_AUTO_PART_THRESHOLD = 16;

    public static final long DEFAULT_LSM_MEMTABLE_SIZE_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_LSM_L0_FILE_LIMIT = 4;
    public static final int DEFAULT_LSM_LEVEL_SIZE_MULTIPLIER = 10;
    public static final boolean DEFAULT_LSM_WAL_FSYNC = false;
    public static final long DEFAULT_LSM_BACKGROUND_INTERVAL_MS = 1000;
    public static final int DEFAULT_LSM_BLOOM_BITS_PER_KEY = 10;

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final long compactionTargetSizeBytes;
    private final int compactionAutoPartThreshold;

    private final long lsmMemtableSizeBytes;
    private final int lsmL0FileLimit;
    private final int lsmLevelSizeMultiplier;
    private final boolean lsmWalFsync;
    private final long lsmBackgroundIntervalMs;
    private final int lsmBloomBitsPerKey;

    private MiniDbConfig(long compactionTargetSizeBytes, int compactionAutoPartThreshold,
                         long lsmMemtableSizeBytes, int lsmL0FileLimit, int lsmLevelSizeMultiplier,
                         boolean lsmWalFsync, long lsmBackgroundIntervalMs, int lsmBloomBitsPerKey) {
        this.compactionTargetSizeBytes = compactionTargetSizeBytes;
        this.compactionAutoPartThreshold = compactionAutoPartThreshold;
        this.lsmMemtableSizeBytes = lsmMemtableSizeBytes;
        this.lsmL0FileLimit = lsmL0FileLimit;
        this.lsmLevelSizeMultiplier = lsmLevelSizeMultiplier;
        this.lsmWalFsync = lsmWalFsync;
        this.lsmBackgroundIntervalMs = lsmBackgroundIntervalMs;
        this.lsmBloomBitsPerKey = lsmBloomBitsPerKey;
    }

    public long compactionTargetSizeBytes() {
        return compactionTargetSizeBytes;
    }

    public int compactionAutoPartThreshold() {
        return compactionAutoPartThreshold;
    }

    public long lsmMemtableSizeBytes() {
        return lsmMemtableSizeBytes;
    }

    public int lsmL0FileLimit() {
        return lsmL0FileLimit;
    }

    public int lsmLevelSizeMultiplier() {
        return lsmLevelSizeMultiplier;
    }

    public boolean lsmWalFsync() {
        return lsmWalFsync;
    }

    public long lsmBackgroundIntervalMs() {
        return lsmBackgroundIntervalMs;
    }

    public int lsmBloomBitsPerKey() {
        return lsmBloomBitsPerKey;
    }

    public static MiniDbConfig load(Path dataDir) {
        long targetBytes = DEFAULT_COMPACTION_TARGET_SIZE_BYTES;
        int autoThreshold = DEFAULT_COMPACTION_AUTO_PART_THRESHOLD;
        long lsmMemtable = DEFAULT_LSM_MEMTABLE_SIZE_BYTES;
        int lsmL0 = DEFAULT_LSM_L0_FILE_LIMIT;
        int lsmMultiplier = DEFAULT_LSM_LEVEL_SIZE_MULTIPLIER;
        boolean lsmFsync = DEFAULT_LSM_WAL_FSYNC;
        long lsmInterval = DEFAULT_LSM_BACKGROUND_INTERVAL_MS;
        int lsmBloom = DEFAULT_LSM_BLOOM_BITS_PER_KEY;
        Path file = dataDir.resolve("config.yaml");
        if (Files.exists(file)) {
            Map<String, Object> root = readYaml(file);
            Map<String, Object> compaction = asMap(root.get("compaction"));
            Long targetMb = asLong(compaction == null ? null : compaction.get("target-size-mb"));
            if (targetMb != null && targetMb > 0) {
                targetBytes = targetMb * 1024 * 1024;
            }
            Integer threshold = asInt(compaction == null ? null : compaction.get("auto-part-threshold"));
            if (threshold != null && threshold > 0) {
                autoThreshold = threshold;
            }
            Map<String, Object> lsm = asMap(root.get("lsm"));
            Long mtMb = asLong(lsm == null ? null : lsm.get("memtable-size-mb"));
            if (mtMb != null && mtMb > 0) {
                lsmMemtable = mtMb * 1024 * 1024;
            }
            Integer l0 = asInt(lsm == null ? null : lsm.get("l0-file-limit"));
            if (l0 != null && l0 > 0) {
                lsmL0 = l0;
            }
            Integer mult = asInt(lsm == null ? null : lsm.get("level-size-multiplier"));
            if (mult != null && mult > 0) {
                lsmMultiplier = mult;
            }
            Boolean fsync = asBoolean(lsm == null ? null : lsm.get("wal-fsync"));
            if (fsync != null) {
                lsmFsync = fsync;
            }
            Long interval = asLong(lsm == null ? null : lsm.get("background-interval-ms"));
            if (interval != null && interval > 0) {
                lsmInterval = interval;
            }
            Integer bloom = asInt(lsm == null ? null : lsm.get("bloom-bits-per-key"));
            if (bloom != null && bloom > 0) {
                lsmBloom = bloom;
            }
        }
        return new MiniDbConfig(targetBytes, autoThreshold,
                lsmMemtable, lsmL0, lsmMultiplier, lsmFsync, lsmInterval, lsmBloom);
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

    private static Boolean asBoolean(Object value) {
        return value instanceof Boolean b ? b : null;
    }
}
