package com.streamhub.coordinator;

import com.streamhub.protocol.PartitionAssignment;
import com.streamhub.storage.LogManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class GroupCoordinator {
    private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

    private final LogManager logManager;
    private final OffsetStore offsetStore;
    private final RangeAssignor assignor = new RangeAssignor();
    private final Map<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
    private final AtomicLong rebalanceCount = new AtomicLong();

    public GroupCoordinator(LogManager logManager, Path dataDir) throws IOException {
        this.logManager = logManager;
        this.offsetStore = new OffsetStore(dataDir);
    }

    public OffsetStore offsetStore() {
        return offsetStore;
    }

    public long rebalanceCount() {
        return rebalanceCount.get();
    }

    public JoinResult joinGroup(String groupId, String memberId, List<String> topics) {
        ConsumerGroup group = groups.computeIfAbsent(groupId, ConsumerGroup::new);
        synchronized (group) {
            String assignedId = memberId == null || memberId.isEmpty()
                    ? "member-" + UUID.randomUUID()
                    : memberId;
            boolean isNew = !group.members().containsKey(assignedId);
            group.members().put(assignedId, new ConsumerGroup.Member(assignedId, topics, Instant.now()));

            if (isNew || group.state() != ConsumerGroup.GroupState.STABLE) {
                if (group.state() != ConsumerGroup.GroupState.REBALANCING) {
                    group.beginRebalance();
                    rebalanceCount.incrementAndGet();
                    log.info("Rebalance started group={} generation={}", groupId, group.generationId());
                }
                scheduleAssignment(group);
                return new JoinResult(assignedId, group.generationId(), "range",
                        group.assignment().getOrDefault(assignedId, List.of()),
                        group.state() == ConsumerGroup.GroupState.STABLE);
            }
            return new JoinResult(assignedId, group.generationId(), "range",
                    group.assignment().getOrDefault(assignedId, List.of()), true);
        }
    }

    public List<PartitionAssignment> syncGroup(String groupId, String memberId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) return List.of();
        synchronized (group) {
            return group.assignment().getOrDefault(memberId, List.of());
        }
    }

    public boolean heartbeat(String groupId, String memberId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) return false;
        synchronized (group) {
            ConsumerGroup.Member m = group.members().get(memberId);
            if (m == null) return false;
            group.members().put(memberId, new ConsumerGroup.Member(memberId, m.subscribedTopics(), Instant.now()));
            return group.state() == ConsumerGroup.GroupState.STABLE
                    || group.state() == ConsumerGroup.GroupState.REBALANCING;
        }
    }

    public void leaveGroup(String groupId, String memberId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) return;
        synchronized (group) {
            group.members().remove(memberId);
            group.beginRebalance();
            rebalanceCount.incrementAndGet();
            scheduleAssignment(group);
        }
    }

    public boolean commitOffset(String groupId, String topic, int partition, long offset) {
        try {
            offsetStore.commit(groupId, topic, partition, offset);
            return true;
        } catch (IOException e) {
            log.error("Commit failed", e);
            return false;
        }
    }

    public long lag(String groupId, String topic, int partition) {
        long hw = logManager.getTopic(topic)
                .map(t -> t.partition(partition).highWatermark())
                .orElse(0L);
        long committed = offsetStore.committedOffset(groupId, topic, partition);
        return Math.max(0, hw - committed);
    }

    public void evictStaleMembers() {
        Instant now = Instant.now();
        for (ConsumerGroup group : groups.values()) {
            synchronized (group) {
                var stale = group.members().entrySet().stream()
                        .filter(e -> e.getValue().lastHeartbeat().isBefore(now.minusSeconds(45)))
                        .map(Map.Entry::getKey)
                        .toList();
                if (!stale.isEmpty()) {
                    stale.forEach(group.members()::remove);
                    group.beginRebalance();
                    rebalanceCount.incrementAndGet();
                    scheduleAssignment(group);
                }
            }
        }
    }

    private void scheduleAssignment(ConsumerGroup group) {
        Map<String, Integer> topicPartitions = new ConcurrentHashMap<>();
        for (ConsumerGroup.Member m : group.members().values()) {
            for (String topic : m.subscribedTopics()) {
                logManager.getTopic(topic).ifPresent(t ->
                        topicPartitions.put(topic, t.partitions()));
            }
        }
        List<String> members = RangeAssignor.sortMembers(new ArrayList<>(group.members().keySet()));
        Map<String, List<PartitionAssignment>> assignment = assignor.assign(members, topicPartitions);
        group.setAssignment(assignment);
        log.info("Assignment complete group={} generation={} members={}", group.groupId(),
                group.generationId(), members.size());
    }

    public Map<String, Long> committedOffsets(String groupId, List<PartitionAssignment> assignment) {
        var map = new java.util.LinkedHashMap<String, Long>();
        for (PartitionAssignment a : assignment) {
            String key = a.topic() + "-" + a.partition();
            map.put(key, offsetStore.committedOffset(groupId, a.topic(), a.partition()));
        }
        return map;
    }

    public record JoinResult(
            String memberId,
            int generationId,
            String protocol,
            List<PartitionAssignment> assignment,
            boolean stable
    ) {}
}
