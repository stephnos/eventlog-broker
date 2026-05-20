package com.streamhub.coordinator;

import com.streamhub.protocol.PartitionAssignment;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ConsumerGroup {
    private final String groupId;
    private final Map<String, Member> members = new ConcurrentHashMap<>();
    private volatile int generationId = 1;
    private volatile GroupState state = GroupState.EMPTY;
    private volatile Map<String, List<PartitionAssignment>> assignment = Map.of();
    private volatile Instant rebalanceDeadline = Instant.EPOCH;

    public ConsumerGroup(String groupId) {
        this.groupId = groupId;
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public GroupState state() {
        return state;
    }

    public Map<String, Member> members() {
        return members;
    }

    public Map<String, List<PartitionAssignment>> assignment() {
        return assignment;
    }

    public void setAssignment(Map<String, List<PartitionAssignment>> assignment) {
        this.assignment = assignment;
        this.state = GroupState.STABLE;
        this.rebalanceDeadline = Instant.EPOCH;
    }

    public void beginRebalance() {
        this.generationId++;
        this.state = GroupState.REBALANCING;
        this.assignment = Map.of();
        this.rebalanceDeadline = Instant.now().plusSeconds(30);
    }

    public Instant rebalanceDeadline() {
        return rebalanceDeadline;
    }

    public enum GroupState {
        EMPTY, REBALANCING, STABLE
    }

    public record Member(String memberId, List<String> subscribedTopics, Instant lastHeartbeat) {}
}
