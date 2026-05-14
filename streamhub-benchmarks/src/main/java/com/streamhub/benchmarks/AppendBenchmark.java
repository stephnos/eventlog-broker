package com.streamhub.benchmarks;

import com.streamhub.storage.LogSegment;
import com.streamhub.storage.PartitionLog;
import com.streamhub.storage.StorageConfig;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@State(Scope.Benchmark)
public class AppendBenchmark {
    private Path tempDir;
    private PartitionLog log;

    @Setup
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("bench-log");
        var config = StorageConfig.defaults(tempDir);
        log = new PartitionLog("bench", 0, config);
    }

    @Benchmark
    public void appendSingle() throws Exception {
        log.append(List.of(new PartitionLog.AppendEntry(System.currentTimeMillis(), null, "payload".getBytes())));
    }

    @TearDown
    public void tearDown() throws Exception {
        // temp cleaned by OS
    }
}
