package com.streamhub.coordinator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class OffsetStore {
    private final Path offsetsFile;
    private final Map<String, Long> offsets = new ConcurrentHashMap<>();

    public OffsetStore(Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        this.offsetsFile = dataDir.resolve("consumer-offsets.properties");
        load();
    }

    public long committedOffset(String groupId, String topic, int partition) {
        return offsets.getOrDefault(key(groupId, topic, partition), 0L);
    }

    public void commit(String groupId, String topic, int partition, long offset) throws IOException {
        offsets.put(key(groupId, topic, partition), offset);
        persist();
    }

    public Map<String, Long> snapshot() {
        return Map.copyOf(offsets);
    }

    private void load() throws IOException {
        if (!Files.exists(offsetsFile)) return;
        for (String line : Files.readAllLines(offsetsFile)) {
            if (line.isBlank() || line.startsWith("#")) continue;
            int eq = line.indexOf('=');
            if (eq > 0) {
                offsets.put(line.substring(0, eq), Long.parseLong(line.substring(eq + 1)));
            }
        }
    }

    private void persist() throws IOException {
        var sb = new StringBuilder();
        offsets.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        Files.writeString(offsetsFile, sb.toString(), StandardCharsets.UTF_8);
    }

    private static String key(String groupId, String topic, int partition) {
        return groupId + "." + topic + "." + partition;
    }
}
