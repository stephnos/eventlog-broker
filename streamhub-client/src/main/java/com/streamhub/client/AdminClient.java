package com.streamhub.client;

import com.streamhub.protocol.Request;
import com.streamhub.protocol.Response;

import java.io.IOException;
import java.util.Map;

public final class AdminClient implements AutoCloseable {
    private final Transport transport;

    public AdminClient(String host, int port) throws IOException {
        this.transport = new Transport(host, port);
        this.transport.connect();
    }

    public void createTopic(String topic, int partitions) throws IOException {
        int corr = transport.nextCorrelationId();
        Response resp = transport.send(new Request.CreateTopic(corr, topic, partitions));
        if (resp instanceof Response.CreateTopic c && !c.success()) {
            throw new IOException(c.message());
        }
        if (resp instanceof Response.Error e) throw new IOException(e.message());
    }

    public void deleteTopic(String topic) throws IOException {
        int corr = transport.nextCorrelationId();
        Response resp = transport.send(new Request.DeleteTopic(corr, topic));
        if (resp instanceof Response.DeleteTopic d && !d.success()) {
            throw new IOException(d.message());
        }
    }

    public Map<String, Integer> listTopics() throws IOException {
        int corr = transport.nextCorrelationId();
        Response resp = transport.send(new Request.Metadata(corr, null));
        if (resp instanceof Response.Metadata m) return m.topics();
        if (resp instanceof Response.Error e) throw new IOException(e.message());
        return Map.of();
    }

    @Override
    public void close() throws IOException {
        transport.close();
    }
}
