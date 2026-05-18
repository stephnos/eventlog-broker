package com.streamhub.client;

import com.streamhub.protocol.AckMode;

public record ClientConfig(
        String host,
        int port,
        AckMode ackMode,
        long lingerMs,
        int maxBatchBytes,
        boolean autoCommit,
        long autoCommitIntervalMs
) {
    public static ClientConfig defaults(String host, int port) {
        return new ClientConfig(host, port, AckMode.ACK_LEADER, 5, 64 * 1024, true, 5000);
    }
}
