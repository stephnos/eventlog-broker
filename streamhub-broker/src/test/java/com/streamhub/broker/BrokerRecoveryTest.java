package com.streamhub.broker;

import com.streamhub.broker.health.HealthServer;
import com.streamhub.broker.netty.BrokerServer;
import com.streamhub.client.AdminClient;
import com.streamhub.client.ClientConfig;
import com.streamhub.client.Consumer;
import com.streamhub.client.Producer;
import com.streamhub.protocol.AckMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerRecoveryTest {

    @TempDir
    Path dataDir;

    @Test
    void survivesRestartAndContinuesFromCommittedOffset() throws Exception {
        int port = findFreePort();
        int healthPort = findFreePort();
        Path dir = dataDir.resolve("broker-data");
        ClientConfig config = new ClientConfig("localhost", port, AckMode.ACK_LEADER, 0, 65536, false, 0);

        try (var broker = startBroker(port, healthPort, dir)) {
            try (AdminClient admin = new AdminClient("localhost", port)) {
                admin.createTopic("recovery-topic", 1);
            }
            try (Producer producer = new Producer(config)) {
                producer.send("recovery-topic", null, "before-crash".getBytes());
                producer.flush();
            }
            try (Consumer consumer = new Consumer(config, "recovery-group")) {
                consumer.subscribe(List.of("recovery-topic"));
                assertFalse(consumer.poll(3000).isEmpty());
                consumer.commitSync();
            }
        }

        try (var broker = startBroker(port, healthPort, dir)) {
            try (Consumer consumer = new Consumer(config, "recovery-group")) {
                consumer.subscribe(List.of("recovery-topic"));
                assertTrue(consumer.poll(500).isEmpty());
            }
            try (Producer producer = new Producer(config)) {
                producer.send("recovery-topic", null, "after-restart".getBytes());
                producer.flush();
            }
            try (Consumer consumer = new Consumer(config, "recovery-group")) {
                consumer.subscribe(List.of("recovery-topic"));
                assertFalse(consumer.poll(3000).isEmpty());
            }
        }
    }

    private static RunningBroker startBroker(int port, int healthPort, Path dir) throws Exception {
        var config = new BrokerConfig(port, healthPort, dir, 64 * 1024 * 1024, 0.9,
                10 * 1024 * 1024, 5 * 1024 * 1024, 1024, 3);
        BrokerEngine engine = new BrokerEngine(config);
        HealthServer health = new HealthServer(healthPort, engine);
        BrokerServer server = new BrokerServer(config, engine);
        server.start();
        Thread.sleep(400);
        return new RunningBroker(server, health, engine);
    }

    private record RunningBroker(BrokerServer server, HealthServer health, BrokerEngine engine)
            implements AutoCloseable {
        @Override
        public void close() {
            server.close();
            health.close();
            engine.close();
        }
    }

    private static int findFreePort() throws Exception {
        try (var socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
