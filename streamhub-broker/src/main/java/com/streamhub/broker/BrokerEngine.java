package com.streamhub.broker;

import com.streamhub.broker.metrics.BrokerMetrics;
import com.streamhub.broker.quota.QuotaLimiter;
import com.streamhub.broker.replica.FakeReplicaSet;
import com.streamhub.coordinator.GroupCoordinator;
import com.streamhub.protocol.AckMode;
import com.streamhub.protocol.Record;
import com.streamhub.protocol.Request;
import com.streamhub.protocol.Response;
import com.streamhub.storage.LogManager;
import com.streamhub.storage.PartitionLog;
import com.streamhub.storage.TopicLogs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class BrokerEngine {
    private static final Logger log = LoggerFactory.getLogger(BrokerEngine.class);

    private final BrokerConfig config;
    private final LogManager logManager;
    private final GroupCoordinator coordinator;
    private final FakeReplicaSet replicas;
    private final BrokerMetrics metrics = new BrokerMetrics();
    private final QuotaLimiter connectionQuota;
    private final QuotaLimiter topicQuota;
    private final AtomicLong pendingBufferBytes = new AtomicLong();
    private final ExecutorService requestExecutor;

    public BrokerEngine(BrokerConfig config) throws IOException {
        this.config = config;
        Files.createDirectories(config.dataDir());
        this.logManager = new LogManager(config.storageConfig());
        this.coordinator = new GroupCoordinator(logManager, config.dataDir());
        this.replicas = new FakeReplicaSet(config.dataDir(), config.replicaCount());
        this.connectionQuota = new QuotaLimiter(config.connectionBytesPerSec());
        this.topicQuota = new QuotaLimiter(config.topicBytesPerSec());
        this.requestExecutor = new ThreadPoolExecutor(
                4, 16, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(config.requestQueueCapacity()),
                r -> {
                    Thread t = new Thread(r, "broker-request");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "coordinator-evict");
            t.setDaemon(true);
            return t;
        }).scheduleAtFixedRate(coordinator::evictStaleMembers, 15, 15, TimeUnit.SECONDS);
    }

    public BrokerMetrics metrics() {
        return metrics;
    }

    public GroupCoordinator coordinator() {
        return coordinator;
    }

    public LogManager logManager() {
        return logManager;
    }

    public Response handle(Request request, String connectionId) {
        try {
            return requestExecutor.submit(() -> process(request, connectionId)).get(30, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            return new Response.Error(request.correlationId(), "Broker overloaded");
        } catch (Exception e) {
            log.error("Request failed", e);
            return new Response.Error(request.correlationId(), e.getMessage());
        }
    }

    private Response process(Request request, String connectionId) {
        int payloadBytes = estimateSize(request);
        if (!connectionQuota.tryAcquire(connectionId, payloadBytes)) {
            return new Response.Error(request.correlationId(), "Connection quota exceeded");
        }
        if (pendingBufferBytes.get() > config.maxBufferedBytes()) {
            return new Response.Error(request.correlationId(), "Broker backpressure: buffer full");
        }
        if (diskWatermarkExceeded()) {
            return new Response.Error(request.correlationId(), "Disk watermark exceeded");
        }

        long start = System.nanoTime();
        Response response = switch (request) {
            case Request.CreateTopic r -> handleCreateTopic(r);
            case Request.DeleteTopic r -> handleDeleteTopic(r);
            case Request.Produce r -> handleProduce(r, connectionId, payloadBytes);
            case Request.Fetch r -> handleFetch(r);
            case Request.JoinGroup r -> handleJoinGroup(r);
            case Request.Heartbeat r -> handleHeartbeat(r);
            case Request.SyncGroup r -> handleSyncGroup(r);
            case Request.CommitOffset r -> handleCommitOffset(r);
            case Request.LeaveGroup r -> handleLeaveGroup(r);
            case Request.Metadata r -> handleMetadata(r);
        };
        long elapsed = System.nanoTime() - start;
        if (request instanceof Request.Produce) {
            if (elapsed > 50_000_000) log.warn("Slow append correlationId={} ms={}", request.correlationId(), elapsed / 1_000_000);
        } else if (request instanceof Request.Fetch) {
            if (elapsed > 50_000_000) log.warn("Slow fetch correlationId={} ms={}", request.correlationId(), elapsed / 1_000_000);
        }
        return response;
    }

    private Response handleCreateTopic(Request.CreateTopic r) {
        try {
            logManager.createTopic(r.topic(), r.partitions());
            return new Response.CreateTopic(r.correlationId(), true, "ok");
        } catch (Exception e) {
            return new Response.CreateTopic(r.correlationId(), false, e.getMessage());
        }
    }

    private Response handleDeleteTopic(Request.DeleteTopic r) {
        try {
            logManager.deleteTopic(r.topic());
            return new Response.DeleteTopic(r.correlationId(), true, "ok");
        } catch (Exception e) {
            return new Response.DeleteTopic(r.correlationId(), false, e.getMessage());
        }
    }

    private Response handleProduce(Request.Produce r, String connectionId, int bytes) {
        if (!topicQuota.tryAcquire(r.topic(), bytes)) {
            return new Response.Error(r.correlationId(), "Topic quota exceeded");
        }
        pendingBufferBytes.addAndGet(bytes);
        try {
            TopicLogs topic = logManager.getTopic(r.topic())
                    .orElseThrow(() -> new IllegalArgumentException("Unknown topic"));
            PartitionLog partition = topic.partition(r.partition());
            var entries = r.records().stream()
                    .map(rec -> new PartitionLog.AppendEntry(
                            rec.timestamp() > 0 ? rec.timestamp() : System.currentTimeMillis(),
                            rec.key(), rec.value()))
                    .toList();

            if (r.ackMode() == AckMode.ACK_NONE) {
                requestExecutor.execute(() -> {
                    try {
                        partition.append(entries);
                    } catch (IOException e) {
                        log.error("Async append failed", e);
                    } finally {
                        pendingBufferBytes.addAndGet(-bytes);
                    }
                });
                return new Response.Produce(r.correlationId(), partition.highWatermark(), List.of());
            }

            List<PartitionLog.AppendEntry> toWrite = entries;
            var stored = partition.append(toWrite);

            if (r.ackMode() == AckMode.ACK_ALL) {
                replicas.replicate(partition, toWrite);
                if (!replicas.hasQuorum()) {
                    return new Response.Error(r.correlationId(), "Insufficient replicas");
                }
            }

            List<Long> resultOffsets = new ArrayList<>();
            long base = stored.isEmpty() ? partition.highWatermark() : stored.getFirst().offset();
            for (var s : stored) resultOffsets.add(s.offset());

            metrics.recordAppend(r.topic(), bytes, System.nanoTime());
            return new Response.Produce(r.correlationId(), base, resultOffsets);
        } catch (Exception e) {
            return new Response.Error(r.correlationId(), e.getMessage());
        } finally {
            pendingBufferBytes.addAndGet(-bytes);
            metrics.setBufferedBytes(pendingBufferBytes.get());
        }
    }

    private Response handleFetch(Request.Fetch r) {
        long start = System.nanoTime();
        try {
            TopicLogs topic = logManager.getTopic(r.topic())
                    .orElseThrow(() -> new IllegalArgumentException("Unknown topic"));
            PartitionLog partition = topic.partition(r.partition());
            var stored = partition.read(r.offset(), r.maxBytes());
            var records = stored.stream()
                    .map(s -> new Record(s.offset(), s.timestamp(), s.key(), s.value()))
                    .toList();
            metrics.recordFetch(System.nanoTime() - start);
            return new Response.Fetch(r.correlationId(), records, partition.highWatermark());
        } catch (Exception e) {
            return new Response.Error(r.correlationId(), e.getMessage());
        }
    }

    private Response handleJoinGroup(Request.JoinGroup r) {
        var result = coordinator.joinGroup(r.groupId(), r.memberId(), r.topics());
        var offsets = coordinator.committedOffsets(r.groupId(), result.assignment());
        return new Response.JoinGroup(r.correlationId(), result.memberId(), result.generationId(),
                result.protocol(), result.assignment(), offsets);
    }

    private Response handleHeartbeat(Request.Heartbeat r) {
        return new Response.Heartbeat(r.correlationId(),
                coordinator.heartbeat(r.groupId(), r.memberId()));
    }

    private Response handleSyncGroup(Request.SyncGroup r) {
        var assignment = coordinator.syncGroup(r.groupId(), r.memberId());
        var offsets = coordinator.committedOffsets(r.groupId(), assignment);
        return new Response.SyncGroup(r.correlationId(), assignment, offsets);
    }

    private Response handleCommitOffset(Request.CommitOffset r) {
        boolean ok = coordinator.commitOffset(r.groupId(), r.topic(), r.partition(), r.offset());
        return new Response.CommitOffset(r.correlationId(), ok);
    }

    private Response handleLeaveGroup(Request.LeaveGroup r) {
        coordinator.leaveGroup(r.groupId(), r.memberId());
        return new Response.LeaveGroup(r.correlationId(), true);
    }

    private Response handleMetadata(Request.Metadata r) {
        var topics = logManager.listTopics();
        if (r.topic() != null) {
            topics = topics.entrySet().stream()
                    .filter(e -> e.getKey().equals(r.topic()))
                    .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return new Response.Metadata(r.correlationId(), topics);
    }

    private boolean diskWatermarkExceeded() {
        try {
            long usable = Files.getFileStore(config.dataDir()).getUsableSpace();
            long total = Files.getFileStore(config.dataDir()).getTotalSpace();
            double usedRatio = 1.0 - (double) usable / total;
            return usedRatio > config.diskWatermarkRatio();
        } catch (IOException e) {
            return false;
        }
    }

    private static int estimateSize(Request r) {
        if (r instanceof Request.Produce p) {
            return p.records().stream().mapToInt(rec ->
                    (rec.key() == null ? 0 : rec.key().length) + rec.value().length + 16).sum();
        }
        return 256;
    }

    public void close() {
        requestExecutor.shutdownNow();
        logManager.close();
    }
}
