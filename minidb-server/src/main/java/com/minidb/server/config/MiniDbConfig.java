package com.minidb.server.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.minidb.server.transaction.TransactionIsolation;
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
 * server:
 *   query-threads: 0
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

    /** 查询线程池大小:0 = 自动(可用处理器数)。 */
    public static final int DEFAULT_SERVER_QUERY_THREADS = 0;

    /** 监听端口,conf/config.yaml 的 server.port。 */
    public static final int DEFAULT_SERVER_PORT = 8899;

    /** 默认事务隔离级别,conf/config.yaml 的 server.isolation-level。 */
    public static final TransactionIsolation DEFAULT_ISOLATION_LEVEL = TransactionIsolation.SERIALIZABLE;

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final long compactionTargetSizeBytes;
    private final int compactionAutoPartThreshold;

    private final long lsmMemtableSizeBytes;
    private final int lsmL0FileLimit;
    private final int lsmLevelSizeMultiplier;
    private final boolean lsmWalFsync;
    private final long lsmBackgroundIntervalMs;
    private final int lsmBloomBitsPerKey;
    private final int serverQueryThreads;
    private final int serverPort;
    private final TransactionIsolation isolationLevel;

    private MiniDbConfig(long compactionTargetSizeBytes, int compactionAutoPartThreshold,
                         long lsmMemtableSizeBytes, int lsmL0FileLimit, int lsmLevelSizeMultiplier,
                         boolean lsmWalFsync, long lsmBackgroundIntervalMs, int lsmBloomBitsPerKey,
                         int serverQueryThreads, int serverPort, TransactionIsolation isolationLevel) {
        this.compactionTargetSizeBytes = compactionTargetSizeBytes;
        this.compactionAutoPartThreshold = compactionAutoPartThreshold;
        this.lsmMemtableSizeBytes = lsmMemtableSizeBytes;
        this.lsmL0FileLimit = lsmL0FileLimit;
        this.lsmLevelSizeMultiplier = lsmLevelSizeMultiplier;
        this.lsmWalFsync = lsmWalFsync;
        this.lsmBackgroundIntervalMs = lsmBackgroundIntervalMs;
        this.lsmBloomBitsPerKey = lsmBloomBitsPerKey;
        this.serverQueryThreads = serverQueryThreads;
        this.serverPort = serverPort;
        this.isolationLevel = isolationLevel;
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

    /** 查询线程池大小:0 = 自动(可用处理器数)。 */
    public int serverQueryThreads() {
        return serverQueryThreads;
    }

    /** 监听端口,conf/config.yaml 的 server.port。 */
    public int serverPort() {
        return serverPort;
    }

    /** 事务隔离级别,conf/config.yaml 的 server.isolation-level。 */
    public TransactionIsolation isolationLevel() {
        return isolationLevel;
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
        int queryThreads = DEFAULT_SERVER_QUERY_THREADS;
        int serverPort = DEFAULT_SERVER_PORT;
        TransactionIsolation isolationLevel = DEFAULT_ISOLATION_LEVEL;
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
            Map<String, Object> server = asMap(root.get("server"));
            Integer qt = asInt(server == null ? null : server.get("query-threads"));
            if (qt != null && qt >= 0) {
                queryThreads = qt;
            }
            Integer port = asInt(server == null ? null : server.get("port"));
            if (port != null && port > 0) {
                serverPort = port;
            }
            String isoStr = asString(server == null ? null : server.get("isolation-level"));
            if (isoStr != null) {
                isolationLevel = TransactionIsolation.fromString(isoStr);
            }
        }
        return new MiniDbConfig(targetBytes, autoThreshold,
                lsmMemtable, lsmL0, lsmMultiplier, lsmFsync, lsmInterval, lsmBloom, queryThreads, serverPort, isolationLevel);
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

    private static String asString(Object value) {
        return value instanceof CharSequence s ? s.toString() : null;
    }
}
