package com.streamhub.sample;

import com.streamhub.client.AdminClient;
import com.streamhub.client.ClientConfig;
import com.streamhub.client.Producer;

import java.io.IOException;

public final class SampleProducer {
    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("STREAMHUB_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("STREAMHUB_PORT", "9092"));
        String topic = System.getenv().getOrDefault("STREAMHUB_TOPIC", "events");

        try (AdminClient admin = new AdminClient(host, port)) {
            try {
                admin.createTopic(topic, 4);
            } catch (IOException ignored) {
            }
        }

        ClientConfig config = ClientConfig.defaults(host, port);
        try (Producer producer = new Producer(config)) {
            for (int i = 0; i < 1000; i++) {
                String msg = "event-" + i;
                producer.send(topic, ("key-" + (i % 10)).getBytes(),
                        msg.getBytes());
            }
            producer.flush();
            System.out.println("Produced 1000 events to " + topic);
        }
    }
}
