package com.streamhub.client;

import com.streamhub.protocol.Record;

import java.util.List;
import java.util.Map;

public record ConsumerRecords(Map<TopicPartition, List<Record>> recordsByPartition) {
    public record TopicPartition(String topic, int partition) {}

    public boolean isEmpty() {
        return recordsByPartition.isEmpty();
    }

    public void forEach(RecordHandler handler) {
        recordsByPartition.forEach((tp, records) -> {
            for (Record r : records) {
                handler.onRecord(tp.topic(), tp.partition(), r);
            }
        });
    }

    @FunctionalInterface
    public interface RecordHandler {
        void onRecord(String topic, int partition, Record record);
    }
}
