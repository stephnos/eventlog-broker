package com.streamhub.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtocolCodecTest {

    @Test
    void produceRoundTrip() throws Exception {
        var req = new Request.Produce(42, "t", 1, AckMode.ACK_LEADER,
                List.of(new Request.ProduceRecord("k".getBytes(), "v".getBytes(), 1L)));
        byte[] encoded = ProtocolCodec.encode(req);
        Request decoded = ProtocolCodec.decodeRequest(encoded);
        assertEquals(req.correlationId(), decoded.correlationId());
        var orig = (Request.Produce) req;
        var got = (Request.Produce) decoded;
        assertEquals(orig.topic(), got.topic());
        assertEquals(orig.partition(), got.partition());
        assertEquals(orig.ackMode(), got.ackMode());
        assertEquals(1, got.records().size());
        assertEquals("k", new String(got.records().getFirst().key()));
        assertEquals("v", new String(got.records().getFirst().value()));
    }

    @Test
    void joinGroupResponseRoundTrip() throws Exception {
        var resp = new Response.JoinGroup(1, "m1", 2, "range",
                List.of(new PartitionAssignment("t", 0)),
                Map.of("t-0", 5L));
        byte[] encoded = ProtocolCodec.encode(resp);
        Response decoded = ProtocolCodec.decodeResponse(encoded);
        assertEquals(resp, decoded);
    }
}
