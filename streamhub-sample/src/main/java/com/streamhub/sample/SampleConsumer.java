package com.streamhub.sample;

import com.streamhub.client.ClientConfig;
import com.streamhub.client.Consumer;
import com.streamhub.client.ConsumerRecords;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Demonstrates at-least-once handling: tracks processed IDs in memory so duplicates
 * after rebalance/restart are skipped (production would use RocksDB/H2).
 */
public final class SampleConsumer {
    private final Set<String> processed = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("STREAMHUB_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("STREAMHUB_PORT", "9092"));
        String topic = System.getenv().getOrDefault("STREAMHUB_TOPIC", "events");
        String group = System.getenv().getOrDefault("STREAMHUB_GROUP", "demo-group");

        ClientConfig config = ClientConfig.defaults(host, port);
        SampleConsumer demo = new SampleConsumer();
        try (Consumer consumer = new Consumer(config, group)) {
            consumer.subscribe(java.util.List.of(topic));
            int emptyPolls = 0;
            while (emptyPolls < 20) {
                ConsumerRecords records = consumer.poll(1000);
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                records.forEach((t, p, r) -> demo.handle(t, p, r.offset(),
                        new String(r.value(), StandardCharsets.UTF_8)));
                consumer.commitSync();
            }
            System.out.println("Processed " + demo.processed.size() + " unique events");
        }
    }

    private void handle(String topic, int partition, long offset, String value) {
        String id = topic + "-" + partition + "-" + offset;
        if (processed.add(id)) {
            System.out.println("Consumed: " + value);
        } else {
            System.out.println("Duplicate skipped: " + value);
        }
    }
}
