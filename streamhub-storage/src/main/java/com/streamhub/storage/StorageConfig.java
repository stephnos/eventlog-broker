package com.streamhub.storage;

import java.nio.file.Path;
import java.time.Duration;

public record StorageConfig(
        Path dataDir,
        long segmentMaxBytes,
        Duration retentionTime,
        long retentionBytes,
        int indexIntervalBytes
) {
    public static StorageConfig defaults(Path dataDir) {
        return new StorageConfig(
                dataDir,
                64 * 1024 * 1024, // 64 MB segments for demo (prod: 1 GB)
                Duration.ofHours(24),
                512 * 1024 * 1024L,
                4096
        );
    }
}
