package com.streamhub.coordinator;

import com.streamhub.protocol.PartitionAssignment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Range assignor: for each topic, divide partitions contiguously among members.
 * Round-robin assignor is documented as future work in README.
 */
public final class RangeAssignor {

    public Map<String, List<PartitionAssignment>> assign(
            List<String> sortedMemberIds,
            Map<String, Integer> topicPartitions) {
        var result = new LinkedHashMap<String, List<PartitionAssignment>>();
        for (String member : sortedMemberIds) {
            result.put(member, new ArrayList<>());
        }
        if (sortedMemberIds.isEmpty()) return result;

        List<String> topics = new ArrayList<>(topicPartitions.keySet());
        Collections.sort(topics);
        for (String topic : topics) {
            int partitionCount = topicPartitions.get(topic);
            int memberCount = sortedMemberIds.size();
            int perMember = partitionCount / memberCount;
            int remainder = partitionCount % memberCount;
            int partition = 0;
            for (int m = 0; m < memberCount; m++) {
                int assignCount = perMember + (m < remainder ? 1 : 0);
                String memberId = sortedMemberIds.get(m);
                for (int i = 0; i < assignCount; i++) {
                    result.get(memberId).add(new PartitionAssignment(topic, partition++));
                }
            }
        }
        return result;
    }

    public static List<String> sortMembers(List<String> memberIds) {
        var copy = new ArrayList<>(memberIds);
        copy.sort(Comparator.naturalOrder());
        return copy;
    }
}
