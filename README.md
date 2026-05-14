# StreamHub

Fault-tolerant append-only event log with consumer groups — a production-style “tiny Kafka slice” in Java 21.

## Architecture

```
[Producer] --batch--> [Broker: Netty API]
                           |
                     [Partition leader]
                           |
              [Log segments on disk] + [sparse offset index]
                           |
[Consumer group] <--fetch-- [Coordinator: range assignment + commits]
```

| Module | Responsibility |
|--------|----------------|
| `streamhub-protocol` | Length-prefixed TCP wire format |
| `streamhub-storage` | Segmented append-only logs, retention |
| `streamhub-coordinator` | Consumer groups, range assignor, offset store |
| `streamhub-broker` | Netty server, quotas, metrics, health |
| `streamhub-client` | Producer, Consumer, Admin SDK |
| `streamhub-sample` | Demo producer/consumer |
| `streamhub-benchmarks` | JMH microbenchmarks |

## Why length-prefixed TCP (not gRPC)?

- **Zero code-gen friction** — no `.proto` compiler or protobuf runtime in the hot path.
- **Inspectable** — `nc` + hex dump debug frames; JMH can benchmark encode/decode directly.
- **Sufficient for v1** — single broker, modest API surface; gRPC shines when you need streaming RPC, multi-language stubs, and HTTP/2 multiplexing at cluster scale.

## Delivery semantics

| Mode | Behavior |
|------|----------|
| **At-least-once (default)** | Auto-commit after `poll()`; crash before commit → redelivery. Sample consumer dedupes by `(topic, partition, offset)`. |
| **Manual commit** | `enable.auto.commit=false` equivalent: call `commitSync()` after processing. |
| **Exactly-once (stretch)** | Idempotent producer sequence numbers + transactional offset commit + external id store (RocksDB/H2). Documented path, not fully implemented. |

## Producer

```java
ClientConfig config = ClientConfig.defaults("localhost", 9092);
try (Producer p = new Producer(config)) {
    p.send("events", "key".getBytes(), "payload".getBytes());
    p.flush();
}
```

- **Batching**: `lingerMs` + `maxBatchBytes`
- **Ack modes**: `ACK_NONE`, `ACK_LEADER` (local log + fsync), `ACK_ALL` (quorum via N fake local replica dirs — see `FakeReplicaSet`)

## Consumer groups

- **Assignor**: **Range** (contiguous partition ranges per member). Round-robin documented as future work.
- **Rebalance**: on join/leave/heartbeat timeout (45s).
- **Offsets**: persisted to `consumer-offsets.properties` under the data directory.

## Storage engine

- Append-only segment files (`{baseOffset}.log`), rolled at 64 MB (configurable; use 1 GB in production).
- **Index**: sparse in-memory `offset → file position`, one entry per `indexIntervalBytes` (4096).
- **RandomAccessFile** (not mmap) for v1: portable, explicit `fsync` boundaries; upgrade to mmap for read-heavy workloads.
- **Retention**: time (24h default) and total size (512 MB default); background cleaner every 30s.

## Concurrency

- **Netty** boss/worker event loops for I/O.
- **Bounded `ThreadPoolExecutor`** (queue 1024) for request handling — no unbounded queues on hot paths.
- **Java 21 virtual threads** for client producer sender and async commits — cheap blocking I/O. Broker uses a classic pool because request work is CPU + disk bound and benefits from predictable parallelism; virtual threads excel when you have many idle/blocking client connections.

## Observability

- Health: `GET http://localhost:8080/health`
- Metrics: `GET http://localhost:8080/metrics` (append/fetch counts, lag via coordinator, p50/p95/p99 latency buckets, rebalance count)
- Structured WARN logs for append/fetch &gt; 50 ms

## Quick start

```bash
mvn -q clean package -DskipTests
java -jar streamhub-broker/target/streamhub-broker-1.0.0-SNAPSHOT.jar
```

Another terminal:

```bash
mvn -q -pl streamhub-sample exec:java -Dexec.mainClass=com.streamhub.sample.SampleProducer
mvn -q -pl streamhub-sample exec:java -Dexec.mainClass=com.streamhub.sample.SampleConsumer
```

### Docker Compose

```bash
docker compose up --build
```

## Tests

```bash
mvn test
```

Covers: log segment round-trip, range assignor, broker restart + offset recovery.

## Benchmarks

```bash
mvn -q -pl streamhub-benchmarks package
java -jar streamhub-benchmarks/target/benchmarks.jar ProtocolCodecBenchmark
```

## Failure injection

| Scenario | Command |
|----------|---------|
| Hard kill | `docker compose kill -s SIGKILL broker` then `docker compose up broker` — offsets/commits replay from disk |
| Disk full | `dd` into volume or lower `diskWatermarkRatio` — producer receives backpressure error |
| Rebalance during poll | Scale consumers: run two `SampleConsumer` processes with same `STREAMHUB_GROUP` |

## Stretch goals (documented / partial)

| Goal | Status |
|------|--------|
| Log compaction | Future — keyed retention policy on segments |
| Dead-letter topic | Future — client retry + DLT route |
| Schema registry lite | Future — schema id in record header |
| Raft cluster | Future — `FakeReplicaSet` simulates ACK_ALL on one node |
| Exactly-once demo | Sample dedupe shows pattern; full EOS not implemented |

## License

MIT
