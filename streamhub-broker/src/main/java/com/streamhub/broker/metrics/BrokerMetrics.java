package com.streamhub.broker.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class BrokerMetrics {
    private final LongAdder appendCount = new LongAdder();
    private final LongAdder fetchCount = new LongAdder();
    private final LongAdder segmentRolls = new LongAdder();
    private final AtomicLong bufferedBytes = new AtomicLong();
    private final ConcurrentHashMap<String, LongAdder> topicAppendBytes = new ConcurrentHashMap<>();
    private final LatencyTracker appendLatency = new LatencyTracker();
    private final LatencyTracker fetchLatency = new LatencyTracker();

    public void recordAppend(String topic, int bytes, long nanos) {
        appendCount.increment();
        appendLatency.record(nanos);
        topicAppendBytes.computeIfAbsent(topic, k -> new LongAdder()).add(bytes);
    }

    public void recordFetch(long nanos) {
        fetchCount.increment();
        fetchLatency.record(nanos);
    }

    public void recordSegmentRoll() {
        segmentRolls.increment();
    }

    public void setBufferedBytes(long bytes) {
        bufferedBytes.set(bytes);
    }

    public long appendRate() {
        return appendCount.sum();
    }

    public long fetchRate() {
        return fetchCount.sum();
    }

    public long segmentRollCount() {
        return segmentRolls.sum();
    }

    public long bufferedBytes() {
        return bufferedBytes.get();
    }

    public LatencyTracker appendLatency() {
        return appendLatency;
    }

    public LatencyTracker fetchLatency() {
        return fetchLatency;
    }

    public static final class LatencyTracker {
        private final LongAdder count = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final ConcurrentHashMap<Long, LongAdder> buckets = new ConcurrentHashMap<>();

        public void record(long nanos) {
            count.increment();
            totalNanos.add(nanos);
            long ms = nanos / 1_000_000;
            buckets.computeIfAbsent(bucket(ms), k -> new LongAdder()).increment();
        }

        public double percentile(double p) {
            long total = count.sum();
            if (total == 0) return 0;
            long target = (long) (total * p);
            long cumulative = 0;
            var sorted = buckets.keySet().stream().sorted().toList();
            for (Long b : sorted) {
                cumulative += buckets.get(b).sum();
                if (cumulative >= target) return b;
            }
            return sorted.isEmpty() ? 0 : sorted.getLast();
        }

        private static long bucket(long ms) {
            if (ms < 1) return 1;
            if (ms < 5) return 5;
            if (ms < 10) return 10;
            if (ms < 50) return 50;
            if (ms < 100) return 100;
            return 500;
        }
    }
}
