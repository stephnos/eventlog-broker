package com.streamhub.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class LogManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    private final StorageConfig config;
    private final Map<String, TopicLogs> topics = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "segment-cleaner");
                t.setDaemon(true);
                return t;
            });

    public LogManager(StorageConfig config) {
        this.config = config;
        cleaner.scheduleAtFixedRate(this::runRetention, 30, 30, TimeUnit.SECONDS);
        recoverTopics();
    }

    public void createTopic(String topic, int partitions) throws IOException {
        if (topics.containsKey(topic)) {
            throw new IllegalStateException("Topic already exists: " + topic);
        }
        var topicLogs = new TopicLogs(topic, partitions, config);
        topics.put(topic, topicLogs);
        log.info("Created topic {} with {} partitions", topic, partitions);
    }

    public void deleteTopic(String topic) throws IOException {
        TopicLogs removed = topics.remove(topic);
        if (removed == null) throw new IllegalStateException("Unknown topic: " + topic);
        Path dir = config.dataDir().resolve(topic);
        if (Files.exists(dir)) {
            try (var walk = Files.walk(dir)) {
                walk.sorted((a, b) -> -a.compareTo(b)).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        log.warn("Failed to delete {}", p, e);
                    }
                });
            }
        }
    }

    public Optional<TopicLogs> getTopic(String topic) {
        return Optional.ofNullable(topics.get(topic));
    }

    public Map<String, Integer> listTopics() {
        var map = new ConcurrentHashMap<String, Integer>();
        topics.forEach((t, logs) -> map.put(t, logs.partitions()));
        return map;
    }

    private void recoverTopics() {
        Path root = config.dataDir();
        if (!Files.exists(root)) {
            try {
                Files.createDirectories(root);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }
        try (var dirs = Files.list(root)) {
            dirs.filter(Files::isDirectory).forEach(topicDir -> {
                String topic = topicDir.getFileName().toString();
                try (var parts = Files.list(topicDir)) {
                    int count = (int) parts.filter(p -> p.getFileName().toString().startsWith("partition-"))
                            .count();
                    if (count > 0) {
                        topics.put(topic, new TopicLogs(topic, count, config));
                        log.info("Recovered topic {} partitions={}", topic, count);
                    }
                } catch (IOException e) {
                    log.warn("Failed to recover topic {}", topic, e);
                }
            });
        } catch (IOException e) {
            log.warn("Failed to list data dir", e);
        }
    }

    private void runRetention() {
        topics.values().forEach(t -> {
            t.partitionLogs().forEach(p -> {
                try {
                    p.enforceRetention();
                } catch (IOException e) {
                    log.warn("Retention failed for {}-{}", t.name(), p.partition(), e);
                }
            });
        });
    }

    @Override
    public void close() {
        cleaner.shutdownNow();
        topics.values().forEach(TopicLogs::close);
    }
}
