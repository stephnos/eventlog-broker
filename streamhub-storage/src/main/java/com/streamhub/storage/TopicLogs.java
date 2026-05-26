package com.streamhub.storage;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

public final class TopicLogs implements AutoCloseable {
    private final String name;
    private final List<PartitionLog> partitions;

    public TopicLogs(String name, int partitionCount, StorageConfig config) throws IOException {
        this.name = name;
        this.partitions = IntStream.range(0, partitionCount)
                .mapToObj(i -> {
                    try {
                        return new PartitionLog(name, i, config);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
    }

    public String name() {
        return name;
    }

    public int partitions() {
        return partitions.size();
    }

    public PartitionLog partition(int id) {
        if (id < 0 || id >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition " + id);
        }
        return partitions.get(id);
    }

    public List<PartitionLog> partitionLogs() {
        return partitions;
    }

    @Override
    public void close() {
        partitions.forEach(p -> {});
    }
}
