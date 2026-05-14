package com.streamhub.benchmarks;

import com.streamhub.protocol.AckMode;
import com.streamhub.protocol.ProtocolCodec;
import com.streamhub.protocol.Request;
import com.streamhub.protocol.Response;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ProtocolCodecBenchmark {

    private byte[] encoded;

    @Setup
    public void setup() throws Exception {
        var req = new Request.Produce(1, "bench", 0, AckMode.ACK_LEADER,
                List.of(new Request.ProduceRecord("k".getBytes(), "value".getBytes(), System.currentTimeMillis())));
        encoded = ProtocolCodec.encode(req);
    }

    @Benchmark
    public byte[] encodeProduce() throws Exception {
        var req = new Request.Produce(1, "bench", 0, AckMode.ACK_LEADER,
                List.of(new Request.ProduceRecord("k".getBytes(), "value".getBytes(), System.currentTimeMillis())));
        return ProtocolCodec.encode(req);
    }

    @Benchmark
    public Request decodeProduce() throws Exception {
        return ProtocolCodec.decodeRequest(encoded);
    }

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
                .include(ProtocolCodecBenchmark.class.getSimpleName())
                .mode(Mode.Throughput)
                .forks(1)
                .build()).run();
    }
}
