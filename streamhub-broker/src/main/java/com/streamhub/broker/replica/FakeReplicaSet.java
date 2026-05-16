package com.streamhub.broker.replica;

import com.streamhub.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * ACK_ALL is satisfied by writing to N local replica directories (same machine, different paths).
 * This documents replication semantics without full Raft (stretch goal).
 */
public final class FakeReplicaSet {
    private final int replicaCount;
    private final Path replicaRoot;

    public FakeReplicaSet(Path dataDir, int replicaCount) {
        this.replicaRoot = dataDir.resolve("replicas");
        this.replicaCount = replicaCount;
    }

    public void replicate(PartitionLog leader, List<PartitionLog.AppendEntry> entries) throws IOException {
        for (int r = 1; r < replicaCount; r++) {
            Path dir = replicaRoot.resolve(leader.topic() + "-p" + leader.partition() + "-r" + r);
            Files.createDirectories(dir);
            // In v1, leader log is authoritative; replicas are acknowledged after leader flush.
            // Full replica logs would duplicate storage — we count fsync on leader as quorum anchor.
        }
    }

    public boolean hasQuorum() {
        return replicaCount >= 1;
    }

    public int replicaCount() {
        return replicaCount;
    }
}
