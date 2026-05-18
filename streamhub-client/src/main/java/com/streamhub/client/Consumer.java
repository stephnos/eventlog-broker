package com.streamhub.client;

import com.streamhub.protocol.PartitionAssignment;
import com.streamhub.protocol.Record;
import com.streamhub.protocol.Request;
import com.streamhub.protocol.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * At-least-once delivery (default): offsets committed after poll; on crash before commit,
 * records may be redelivered. Use manual commit + idempotent processing for stronger guarantees.
 */
public final class Consumer implements AutoCloseable {
    private final ClientConfig config;
    private final Transport transport;
    private final String groupId;
    private String memberId = "";
    private List<PartitionAssignment> assignment = List.of();
    private final Map<String, Long> positions = new ConcurrentHashMap<>();
    private long lastAutoCommit = System.currentTimeMillis();

    public Consumer(ClientConfig config, String groupId) throws IOException {
        this.config = config;
        this.groupId = groupId;
        this.transport = new Transport(config.host(), config.port());
        this.transport.connect();
    }

    public void subscribe(List<String> topics) throws IOException {
        int corr = transport.nextCorrelationId();
        var req = new Request.JoinGroup(corr, groupId, memberId, topics);
        Response resp = transport.send(req);
        if (resp instanceof Response.JoinGroup j) {
            memberId = j.memberId();
            applyAssignment(j.assignment(), j.committedOffsets());
        } else if (resp instanceof Response.Error e) {
            throw new IOException(e.message());
        }
        corr = transport.nextCorrelationId();
        resp = transport.send(new Request.SyncGroup(corr, groupId, memberId));
        if (resp instanceof Response.SyncGroup s) {
            applyAssignment(s.assignment(), s.committedOffsets());
        }
    }

    private void applyAssignment(List<PartitionAssignment> newAssignment, Map<String, Long> committedOffsets) {
        assignment = newAssignment;
        for (PartitionAssignment a : assignment) {
            String posKey = key(a);
            long offset = committedOffsets.getOrDefault(posKey, 0L);
            positions.put(posKey, offset);
        }
    }

    public ConsumerRecords poll(long timeoutMs) throws IOException {
        heartbeat();
        var result = new HashMap<ConsumerRecords.TopicPartition, List<Record>>();
        for (PartitionAssignment a : assignment) {
            String posKey = key(a);
            long offset = positions.getOrDefault(posKey, 0L);
            int corr = transport.nextCorrelationId();
            var req = new Request.Fetch(corr, a.topic(), a.partition(), offset, 64 * 1024);
            Response resp = transport.send(req);
            if (resp instanceof Response.Fetch f && !f.records().isEmpty()) {
                result.put(new ConsumerRecords.TopicPartition(a.topic(), a.partition()), f.records());
                long next = f.records().getLast().offset() + 1;
                positions.put(posKey, next);
            }
        }
        if (config.autoCommit() && System.currentTimeMillis() - lastAutoCommit > config.autoCommitIntervalMs()) {
            commitSync();
            lastAutoCommit = System.currentTimeMillis();
        }
        return new ConsumerRecords(result);
    }

    public void commitSync() throws IOException {
        for (PartitionAssignment a : assignment) {
            String posKey = key(a);
            Long offset = positions.get(posKey);
            if (offset == null) continue;
            int corr = transport.nextCorrelationId();
            transport.send(new Request.CommitOffset(corr, groupId, a.topic(), a.partition(), offset));
        }
    }

    public void commitAsync() {
        Thread.ofVirtual().start(() -> {
            try {
                commitSync();
            } catch (IOException ignored) {
            }
        });
    }

    private void heartbeat() throws IOException {
        if (memberId.isEmpty()) return;
        int corr = transport.nextCorrelationId();
        transport.send(new Request.Heartbeat(corr, groupId, memberId));
    }

    @Override
    public void close() throws IOException {
        if (!memberId.isEmpty()) {
            int corr = transport.nextCorrelationId();
            transport.send(new Request.LeaveGroup(corr, groupId, memberId));
        }
        transport.close();
    }

    private static String key(PartitionAssignment a) {
        return a.topic() + "-" + a.partition();
    }
}
