package com.streamhub.protocol;

import java.util.List;

public sealed interface Request permits
        Request.CreateTopic,
        Request.DeleteTopic,
        Request.Produce,
        Request.Fetch,
        Request.JoinGroup,
        Request.Heartbeat,
        Request.SyncGroup,
        Request.CommitOffset,
        Request.LeaveGroup,
        Request.Metadata {

    int correlationId();

    record CreateTopic(int correlationId, String topic, int partitions) implements Request {}

    record DeleteTopic(int correlationId, String topic) implements Request {}

    record ProduceRecord(byte[] key, byte[] value, long timestamp) {}

    record Produce(
            int correlationId,
            String topic,
            int partition,
            AckMode ackMode,
            List<ProduceRecord> records
    ) implements Request {}

    record Fetch(
            int correlationId,
            String topic,
            int partition,
            long offset,
            int maxBytes
    ) implements Request {}

    record JoinGroup(
            int correlationId,
            String groupId,
            String memberId,
            List<String> topics
    ) implements Request {}

    record Heartbeat(int correlationId, String groupId, String memberId) implements Request {}

    record SyncGroup(int correlationId, String groupId, String memberId) implements Request {}

    record CommitOffset(
            int correlationId,
            String groupId,
            String topic,
            int partition,
            long offset
    ) implements Request {}

    record LeaveGroup(int correlationId, String groupId, String memberId) implements Request {}

    record Metadata(int correlationId, String topic) implements Request {}
}
