package com.streamhub.protocol;

import java.util.List;
import java.util.Map;

public sealed interface Response permits
        Response.CreateTopic,
        Response.DeleteTopic,
        Response.Produce,
        Response.Fetch,
        Response.JoinGroup,
        Response.Heartbeat,
        Response.SyncGroup,
        Response.CommitOffset,
        Response.LeaveGroup,
        Response.Metadata,
        Response.Error {

    int correlationId();

    record CreateTopic(int correlationId, boolean success, String message) implements Response {}

    record DeleteTopic(int correlationId, boolean success, String message) implements Response {}

    record Produce(int correlationId, long baseOffset, List<Long> offsets) implements Response {}

    record Fetch(int correlationId, List<Record> records, long highWatermark) implements Response {}

    record JoinGroup(
            int correlationId,
            String memberId,
            int generationId,
            String protocolType,
            List<PartitionAssignment> assignment,
            Map<String, Long> committedOffsets
    ) implements Response {}

    record Heartbeat(int correlationId, boolean ok) implements Response {}

    record SyncGroup(
            int correlationId,
            List<PartitionAssignment> assignment,
            Map<String, Long> committedOffsets
    ) implements Response {}

    record CommitOffset(int correlationId, boolean success) implements Response {}

    record LeaveGroup(int correlationId, boolean success) implements Response {}

    record Metadata(
            int correlationId,
            Map<String, Integer> topics
    ) implements Response {}

    record Error(int correlationId, String message) implements Response {}
}
