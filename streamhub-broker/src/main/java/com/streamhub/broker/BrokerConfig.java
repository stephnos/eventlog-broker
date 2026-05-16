package com.streamhub.broker;

import com.streamhub.storage.StorageConfig;

import java.nio.file.Path;

public record BrokerConfig(
        int port,
        int healthPort,
        Path dataDir,
        long maxBufferedBytes,
        double diskWatermarkRatio,
        long connectionBytesPerSec,
        long topicBytesPerSec,
        int requestQueueCapacity,
        int replicaCount
) {
    public static BrokerConfig defaults() {
        Path data = Path.of(env("STREAMHUB_DATA", "./data"));
        return new BrokerConfig(
                Integer.parseInt(env("STREAMHUB_PORT", "9092")),
                Integer.parseInt(env("STREAMHUB_HEALTH_PORT", "8080")),
                data,
                64 * 1024 * 1024,
                0.9,
                10 * 1024 * 1024,
                5 * 1024 * 1024,
                1024,
                3 // fake local replicas for ACK_ALL
        );
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v != null ? v : defaultValue;
    }

    public StorageConfig storageConfig() {
        return StorageConfig.defaults(dataDir);
    }
}
