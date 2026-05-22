package com.streamhub.protocol;

public record TopicPartition(String topic, int partition) {
    public String key() {
        return topic + "-" + partition;
    }
}
