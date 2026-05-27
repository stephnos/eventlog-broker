package com.streamhub.coordinator;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RangeAssignorTest {

    @Test
    void assignsPartitionsContiguously() {
        var assignor = new RangeAssignor();
        var members = List.of("m1", "m2");
        var topics = Map.of("t", 4);
        var result = assignor.assign(members, topics);
        assertEquals(2, result.get("m1").size());
        assertEquals(2, result.get("m2").size());
    }
}
