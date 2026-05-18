package com.streamhub.client;

import com.streamhub.protocol.AckMode;
import com.streamhub.protocol.Request;
import com.streamhub.protocol.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Producer implements AutoCloseable {
    private final ClientConfig config;
    private final Transport transport;
    private final BlockingQueue<PendingRecord> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread senderThread;
    private int partition = 0;

    public Producer(ClientConfig config) throws IOException {
        this.config = config;
        this.transport = new Transport(config.host(), config.port());
        this.transport.connect();
        this.queue = new ArrayBlockingQueue<>(10_000);
        this.senderThread = Thread.ofVirtual().name("producer-sender").start(this::runSender);
    }

    public void send(String topic, byte[] key, byte[] value) throws IOException {
        send(topic, key, value, System.currentTimeMillis());
    }

    public void send(String topic, byte[] key, byte[] value, long timestamp) throws IOException {
        if (!queue.offer(new PendingRecord(topic, key, value, timestamp))) {
            throw new IOException("Producer queue full");
        }
    }

    public void flush() throws IOException {
        while (!queue.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Flush interrupted", e);
            }
        }
    }

    private void runSender() {
        while (running.get()) {
            try {
                List<PendingRecord> batch = drainBatch();
                if (batch.isEmpty()) {
                    Thread.sleep(config.lingerMs());
                    continue;
                }
                sendBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // log in real app
            }
        }
    }

    private List<PendingRecord> drainBatch() throws InterruptedException {
        var batch = new ArrayList<PendingRecord>();
        PendingRecord first = queue.poll(config.lingerMs(), TimeUnit.MILLISECONDS);
        if (first == null) return batch;
        batch.add(first);
        int bytes = first.estimatedSize();
        while (bytes < config.maxBatchBytes()) {
            PendingRecord next = queue.poll();
            if (next == null) break;
            if (!next.topic.equals(first.topic)) {
                queue.offer(next);
                break;
            }
            batch.add(next);
            bytes += next.estimatedSize();
        }
        return batch;
    }

    private void sendBatch(List<PendingRecord> batch) throws IOException {
        String topic = batch.getFirst().topic;
        var records = batch.stream()
                .map(p -> new Request.ProduceRecord(p.key, p.value, p.timestamp))
                .toList();
        int corr = transport.nextCorrelationId();
        var req = new Request.Produce(corr, topic, partition, config.ackMode(), records);
        Response resp = transport.send(req);
        if (resp instanceof Response.Error e) {
            throw new IOException(e.message());
        }
        // Round-robin across partitions would use topic metadata; v1 uses partition 0.
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        senderThread.interrupt();
        transport.close();
    }

    private record PendingRecord(String topic, byte[] key, byte[] value, long timestamp) {
        int estimatedSize() {
            return (key == null ? 0 : key.length) + value.length + 16;
        }
    }
}
