package com.streamhub.client;

import com.streamhub.protocol.ProtocolCodec;
import com.streamhub.protocol.Request;
import com.streamhub.protocol.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public final class Transport implements AutoCloseable {
    private final String host;
    private final int port;
    private final AtomicInteger correlationId = new AtomicInteger(1);
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private final Object lock = new Object();

    public Transport(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        in = socket.getInputStream();
        out = socket.getOutputStream();
    }

    public Response send(Request request) throws IOException {
        synchronized (lock) {
            byte[] encoded = ProtocolCodec.encode(request);
            out.write(encoded);
            out.flush();
            return readResponse();
        }
    }

    public int nextCorrelationId() {
        return correlationId.getAndIncrement();
    }

    private Response readResponse() throws IOException {
        byte[] lenBuf = in.readNBytes(4);
        if (lenBuf.length < 4) throw new IOException("Connection closed");
        int len = ByteBuffer.wrap(lenBuf).getInt();
        byte[] payload = in.readNBytes(len);
        byte[] frame = new byte[4 + len];
        System.arraycopy(lenBuf, 0, frame, 0, 4);
        System.arraycopy(payload, 0, frame, 4, len);
        return ProtocolCodec.decodeResponse(frame);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) socket.close();
    }
}
