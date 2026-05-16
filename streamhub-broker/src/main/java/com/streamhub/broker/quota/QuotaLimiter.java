package com.streamhub.broker.quota;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class QuotaLimiter {
    private final long bytesPerSecond;
    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public QuotaLimiter(long bytesPerSecond) {
        this.bytesPerSecond = bytesPerSecond;
    }

    public boolean tryAcquire(String key, int bytes) {
        if (bytesPerSecond <= 0) return true;
        return buckets.computeIfAbsent(key, k -> new TokenBucket(bytesPerSecond)).tryConsume(bytes);
    }

    private static final class TokenBucket {
        private final long capacity;
        private final double refillPerNanos;
        private double tokens;
        private long lastRefillNanos;

        TokenBucket(long bytesPerSecond) {
            this.capacity = bytesPerSecond;
            this.refillPerNanos = bytesPerSecond / 1_000_000_000.0;
            this.tokens = capacity;
            this.lastRefillNanos = System.nanoTime();
        }

        synchronized boolean tryConsume(int bytes) {
            refill();
            if (tokens >= bytes) {
                tokens -= bytes;
                return true;
            }
            return false;
        }

        private void refill() {
            long now = System.nanoTime();
            double elapsed = now - lastRefillNanos;
            tokens = Math.min(capacity, tokens + elapsed * refillPerNanos);
            lastRefillNanos = now;
        }
    }
}
