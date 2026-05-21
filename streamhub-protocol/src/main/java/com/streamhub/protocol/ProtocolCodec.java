package com.streamhub.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Length-prefixed framing: [4-byte length][payload].
 * Payload: [apiKey:1][correlationId:4][body...]
 */
public final class ProtocolCodec {

    private ProtocolCodec() {}

    public static byte[] encode(Request request) throws IOException {
        var body = new ByteArrayOutputStream();
        var out = new DataOutputStream(body);
        out.writeByte(apiKey(request).code());
        out.writeInt(request.correlationId());
        encodeBody(request, out);
        return frame(body.toByteArray());
    }

    public static byte[] encode(Response response) throws IOException {
        var body = new ByteArrayOutputStream();
        var out = new DataOutputStream(body);
        out.writeByte(apiKey(response).code());
        out.writeInt(response.correlationId());
        encodeBody(response, out);
        return frame(body.toByteArray());
    }

    public static Request decodeRequest(byte[] framed) throws IOException {
        byte[] payload = unframe(framed);
        var in = new DataInputStream(new ByteArrayInputStream(payload));
        ApiKey key = ApiKey.fromCode(in.readByte());
        int correlationId = in.readInt();
        return decodeRequestBody(key, correlationId, in);
    }

    public static Response decodeResponse(byte[] framed) throws IOException {
        byte[] payload = unframe(framed);
        var in = new DataInputStream(new ByteArrayInputStream(payload));
        ApiKey key = ApiKey.fromCode(in.readByte());
        int correlationId = in.readInt();
        return decodeResponseBody(key, correlationId, in);
    }

    public static int frameLength(byte[] framed) {
        return ByteBuffer.wrap(framed, 0, 4).getInt() + 4;
    }

    private static byte[] frame(byte[] payload) throws IOException {
        var out = new ByteArrayOutputStream(4 + payload.length);
        var dos = new DataOutputStream(out);
        dos.writeInt(payload.length);
        dos.write(payload);
        return out.toByteArray();
    }

    private static byte[] unframe(byte[] data) throws IOException {
        if (data.length < 4) throw new EOFException("Incomplete frame");
        int len = ByteBuffer.wrap(data).getInt();
        if (data.length < 4 + len) throw new EOFException("Incomplete payload");
        return java.util.Arrays.copyOfRange(data, 4, 4 + len);
    }

    private static ApiKey apiKey(Request r) {
        return switch (r) {
            case Request.CreateTopic ignored -> ApiKey.CREATE_TOPIC;
            case Request.DeleteTopic ignored -> ApiKey.DELETE_TOPIC;
            case Request.Produce ignored -> ApiKey.PRODUCE;
            case Request.Fetch ignored -> ApiKey.FETCH;
            case Request.JoinGroup ignored -> ApiKey.JOIN_GROUP;
            case Request.Heartbeat ignored -> ApiKey.HEARTBEAT;
            case Request.SyncGroup ignored -> ApiKey.SYNC_GROUP;
            case Request.CommitOffset ignored -> ApiKey.COMMIT_OFFSET;
            case Request.LeaveGroup ignored -> ApiKey.LEAVE_GROUP;
            case Request.Metadata ignored -> ApiKey.METADATA;
        };
    }

    private static ApiKey apiKey(Response r) {
        return switch (r) {
            case Response.CreateTopic ignored -> ApiKey.CREATE_TOPIC;
            case Response.DeleteTopic ignored -> ApiKey.DELETE_TOPIC;
            case Response.Produce ignored -> ApiKey.PRODUCE;
            case Response.Fetch ignored -> ApiKey.FETCH;
            case Response.JoinGroup ignored -> ApiKey.JOIN_GROUP;
            case Response.Heartbeat ignored -> ApiKey.HEARTBEAT;
            case Response.SyncGroup ignored -> ApiKey.SYNC_GROUP;
            case Response.CommitOffset ignored -> ApiKey.COMMIT_OFFSET;
            case Response.LeaveGroup ignored -> ApiKey.LEAVE_GROUP;
            case Response.Metadata ignored -> ApiKey.METADATA;
            case Response.Error ignored -> ApiKey.ERROR;
        };
    }

    private static void encodeBody(Request r, DataOutputStream out) throws IOException {
        switch (r) {
            case Request.CreateTopic ct -> {
                writeString(out, ct.topic());
                out.writeInt(ct.partitions());
            }
            case Request.DeleteTopic dt -> writeString(out, dt.topic());
            case Request.Produce p -> {
                writeString(out, p.topic());
                out.writeInt(p.partition());
                out.writeByte(p.ackMode().code());
                out.writeInt(p.records().size());
                for (Request.ProduceRecord rec : p.records()) {
                    writeBytes(out, rec.key());
                    writeBytes(out, rec.value());
                    out.writeLong(rec.timestamp());
                }
            }
            case Request.Fetch f -> {
                writeString(out, f.topic());
                out.writeInt(f.partition());
                out.writeLong(f.offset());
                out.writeInt(f.maxBytes());
            }
            case Request.JoinGroup j -> {
                writeString(out, j.groupId());
                writeString(out, j.memberId());
                out.writeInt(j.topics().size());
                for (String t : j.topics()) writeString(out, t);
            }
            case Request.Heartbeat h -> {
                writeString(out, h.groupId());
                writeString(out, h.memberId());
            }
            case Request.SyncGroup s -> {
                writeString(out, s.groupId());
                writeString(out, s.memberId());
            }
            case Request.CommitOffset c -> {
                writeString(out, c.groupId());
                writeString(out, c.topic());
                out.writeInt(c.partition());
                out.writeLong(c.offset());
            }
            case Request.LeaveGroup l -> {
                writeString(out, l.groupId());
                writeString(out, l.memberId());
            }
            case Request.Metadata m -> writeString(out, m.topic() == null ? "" : m.topic());
        }
    }

    private static void encodeBody(Response r, DataOutputStream out) throws IOException {
        switch (r) {
            case Response.CreateTopic ct -> {
                out.writeBoolean(ct.success());
                writeString(out, ct.message());
            }
            case Response.DeleteTopic dt -> {
                out.writeBoolean(dt.success());
                writeString(out, dt.message());
            }
            case Response.Produce p -> {
                out.writeLong(p.baseOffset());
                out.writeInt(p.offsets().size());
                for (Long o : p.offsets()) out.writeLong(o);
            }
            case Response.Fetch f -> {
                out.writeLong(f.highWatermark());
                out.writeInt(f.records().size());
                for (Record rec : f.records()) encodeRecord(out, rec);
            }
            case Response.JoinGroup j -> {
                writeString(out, j.memberId());
                out.writeInt(j.generationId());
                writeString(out, j.protocolType());
                encodeAssignments(out, j.assignment());
                encodeOffsetMap(out, j.committedOffsets());
            }
            case Response.Heartbeat h -> out.writeBoolean(h.ok());
            case Response.SyncGroup s -> {
                encodeAssignments(out, s.assignment());
                encodeOffsetMap(out, s.committedOffsets());
            }
            case Response.CommitOffset c -> out.writeBoolean(c.success());
            case Response.LeaveGroup l -> out.writeBoolean(l.success());
            case Response.Metadata m -> {
                out.writeInt(m.topics().size());
                for (var e : m.topics().entrySet()) {
                    writeString(out, e.getKey());
                    out.writeInt(e.getValue());
                }
            }
            case Response.Error e -> writeString(out, e.message());
        }
    }

    private static Request decodeRequestBody(ApiKey key, int correlationId, DataInputStream in)
            throws IOException {
        return switch (key) {
            case CREATE_TOPIC -> new Request.CreateTopic(correlationId, readString(in), in.readInt());
            case DELETE_TOPIC -> new Request.DeleteTopic(correlationId, readString(in));
            case PRODUCE -> {
                String topic = readString(in);
                int partition = in.readInt();
                AckMode ack = AckMode.fromCode(in.readByte());
                int count = in.readInt();
                var records = new ArrayList<Request.ProduceRecord>(count);
                for (int i = 0; i < count; i++) {
                    records.add(new Request.ProduceRecord(
                            readBytes(in), readBytes(in), in.readLong()));
                }
                yield new Request.Produce(correlationId, topic, partition, ack, records);
            }
            case FETCH -> new Request.Fetch(
                    correlationId, readString(in), in.readInt(), in.readLong(), in.readInt());
            case JOIN_GROUP -> {
                String groupId = readString(in);
                String memberId = readString(in);
                int n = in.readInt();
                var topics = new ArrayList<String>(n);
                for (int i = 0; i < n; i++) topics.add(readString(in));
                yield new Request.JoinGroup(correlationId, groupId, memberId, topics);
            }
            case HEARTBEAT -> new Request.Heartbeat(correlationId, readString(in), readString(in));
            case SYNC_GROUP -> new Request.SyncGroup(correlationId, readString(in), readString(in));
            case COMMIT_OFFSET -> new Request.CommitOffset(
                    correlationId, readString(in), readString(in), in.readInt(), in.readLong());
            case LEAVE_GROUP -> new Request.LeaveGroup(correlationId, readString(in), readString(in));
            case METADATA -> {
                String topic = readString(in);
                yield new Request.Metadata(correlationId, topic.isEmpty() ? null : topic);
            }
            default -> throw new IOException("Unexpected request key: " + key);
        };
    }

    private static Response decodeResponseBody(ApiKey key, int correlationId, DataInputStream in)
            throws IOException {
        return switch (key) {
            case CREATE_TOPIC -> new Response.CreateTopic(correlationId, in.readBoolean(), readString(in));
            case DELETE_TOPIC -> new Response.DeleteTopic(correlationId, in.readBoolean(), readString(in));
            case PRODUCE -> {
                long base = in.readLong();
                int n = in.readInt();
                var offsets = new ArrayList<Long>(n);
                for (int i = 0; i < n; i++) offsets.add(in.readLong());
                yield new Response.Produce(correlationId, base, offsets);
            }
            case FETCH -> {
                long hw = in.readLong();
                int n = in.readInt();
                var records = new ArrayList<Record>(n);
                for (int i = 0; i < n; i++) records.add(decodeRecord(in));
                yield new Response.Fetch(correlationId, records, hw);
            }
            case JOIN_GROUP -> new Response.JoinGroup(
                    correlationId, readString(in), in.readInt(), readString(in),
                    decodeAssignments(in), decodeOffsetMap(in));
            case HEARTBEAT -> new Response.Heartbeat(correlationId, in.readBoolean());
            case SYNC_GROUP -> new Response.SyncGroup(
                    correlationId, decodeAssignments(in), decodeOffsetMap(in));
            case COMMIT_OFFSET -> new Response.CommitOffset(correlationId, in.readBoolean());
            case LEAVE_GROUP -> new Response.LeaveGroup(correlationId, in.readBoolean());
            case METADATA -> {
                int n = in.readInt();
                Map<String, Integer> topics = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) topics.put(readString(in), in.readInt());
                yield new Response.Metadata(correlationId, topics);
            }
            case ERROR -> new Response.Error(correlationId, readString(in));
            default -> throw new IOException("Unexpected response key: " + key);
        };
    }

    private static void encodeRecord(DataOutputStream out, Record r) throws IOException {
        out.writeLong(r.offset());
        out.writeLong(r.timestamp());
        writeBytes(out, r.key());
        writeBytes(out, r.value());
    }

    private static Record decodeRecord(DataInputStream in) throws IOException {
        return new Record(in.readLong(), in.readLong(), readBytes(in), readBytes(in));
    }

    private static void encodeAssignments(DataOutputStream out, List<PartitionAssignment> a)
            throws IOException {
        out.writeInt(a.size());
        for (PartitionAssignment p : a) {
            writeString(out, p.topic());
            out.writeInt(p.partition());
        }
    }

    private static List<PartitionAssignment> decodeAssignments(DataInputStream in) throws IOException {
        int n = in.readInt();
        var list = new ArrayList<PartitionAssignment>(n);
        for (int i = 0; i < n; i++) list.add(new PartitionAssignment(readString(in), in.readInt()));
        return list;
    }

    private static void encodeOffsetMap(DataOutputStream out, Map<String, Long> offsets) throws IOException {
        out.writeInt(offsets.size());
        for (var e : offsets.entrySet()) {
            writeString(out, e.getKey());
            out.writeLong(e.getValue());
        }
    }

    private static Map<String, Long> decodeOffsetMap(DataInputStream in) throws IOException {
        int n = in.readInt();
        var map = new LinkedHashMap<String, Long>(n);
        for (int i = 0; i < n; i++) map.put(readString(in), in.readLong());
        return map;
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = in.readNBytes(len);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void writeBytes(DataOutputStream out, byte[] b) throws IOException {
        if (b == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(b.length);
            out.write(b);
        }
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) return null;
        return in.readNBytes(len);
    }
}
