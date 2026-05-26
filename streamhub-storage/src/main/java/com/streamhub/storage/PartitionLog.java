package com.streamhub.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public final class PartitionLog {
    private static final Logger log = LoggerFactory.getLogger(PartitionLog.class);

    private final String topic;
    private final int partition;
    private final StorageConfig config;
    private final Path partitionDir;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<LogSegment> segments = new ArrayList<>();
    private long nextOffset;
    private LogSegment active;

    public PartitionLog(String topic, int partition, StorageConfig config) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.config = config;
        this.partitionDir = config.dataDir().resolve(topic).resolve("partition-" + partition);
        Files.createDirectories(partitionDir);
        loadSegments();
        if (active == null) {
            rollSegment(0);
        }
        nextOffset = segments.isEmpty() ? 0 : segments.getLast().nextOffset();
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long highWatermark() {
        lock.readLock().lock();
        try {
            return nextOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<LogSegment.StoredRecord> append(List<AppendEntry> entries) throws IOException {
        lock.writeLock().lock();
        try {
            var results = new ArrayList<LogSegment.StoredRecord>();
            for (AppendEntry e : entries) {
                if (active.size() >= config.segmentMaxBytes()) {
                    rollSegment(active.nextOffset());
                }
                long offset = active.append(e.timestamp(), e.key(), e.value());
                results.add(new LogSegment.StoredRecord(offset, e.timestamp(), e.key(), e.value()));
                nextOffset = offset + 1;
            }
            active.flush();
            return results;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<LogSegment.StoredRecord> read(long fromOffset, int maxBytes) throws IOException {
        lock.readLock().lock();
        try {
            var results = new ArrayList<LogSegment.StoredRecord>();
            for (LogSegment seg : segments) {
                if (seg.nextOffset() <= fromOffset) continue;
                results.addAll(seg.read(fromOffset, maxBytes - estimateBytes(results)));
                if (estimateBytes(results) >= maxBytes) break;
                fromOffset = seg.nextOffset();
            }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            active.flush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int enforceRetention() throws IOException {
        lock.writeLock().lock();
        try {
            int removed = 0;
            long totalSize = segments.stream().mapToLong(s -> {
                try {
                    return s.size();
                } catch (IOException e) {
                    return 0;
                }
            }).sum();
            long cutoffTime = System.currentTimeMillis() - config.retentionTime().toMillis();
            while (segments.size() > 1) {
                LogSegment oldest = segments.getFirst();
                boolean sizeExceeded = totalSize > config.retentionBytes();
                boolean timeExceeded = oldest.nextOffset() > 0 && isSegmentExpired(oldest, cutoffTime);
                if (!sizeExceeded && !timeExceeded) break;
                totalSize -= oldest.size();
                oldest.delete();
                segments.removeFirst();
                removed++;
                log.info("Deleted segment {} baseOffset={}", oldest.baseOffset(), oldest.baseOffset());
            }
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean isSegmentExpired(LogSegment seg, long cutoffTime) {
        return seg.baseOffset() < nextOffset - 1;
    }

    private void rollSegment(long baseOffset) throws IOException {
        Path file = partitionDir.resolve(String.format("%020d.log", baseOffset));
        active = new LogSegment(file, baseOffset, config.indexIntervalBytes());
        segments.add(active);
        log.info("Rolled segment topic={} partition={} baseOffset={}", topic, partition, baseOffset);
    }

    private void loadSegments() throws IOException {
        if (!Files.exists(partitionDir)) return;
        try (Stream<Path> files = Files.list(partitionDir)) {
            files.filter(p -> p.toString().endsWith(".log"))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .forEach(p -> {
                        try {
                            long base = Long.parseLong(p.getFileName().toString().replace(".log", ""));
                            segments.add(new LogSegment(p, base, config.indexIntervalBytes()));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        if (!segments.isEmpty()) {
            active = segments.getLast();
            nextOffset = active.nextOffset();
        }
    }

    private static int estimateBytes(List<LogSegment.StoredRecord> records) {
        return records.stream().mapToInt(r -> 32 + (r.key() == null ? 0 : r.key().length) + r.value().length)
                .sum();
    }

    public record AppendEntry(long timestamp, byte[] key, byte[] value) {}
}
