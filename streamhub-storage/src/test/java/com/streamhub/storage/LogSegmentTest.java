package com.streamhub.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogSegmentTest {

    @TempDir
    Path temp;

    @Test
    void appendAndReadRoundTrip() throws Exception {
        Path file = temp.resolve("00000000000000000000.log");
        try (LogSegment seg = new LogSegment(file, 0, 4096)) {
            long o0 = seg.append(System.currentTimeMillis(), "k1".getBytes(), "v1".getBytes());
            long o1 = seg.append(System.currentTimeMillis(), null, "v2".getBytes());
            assertEquals(0L, o0);
            assertEquals(1L, o1);
            seg.flush();
            var records = seg.read(0, 1024 * 1024);
            assertEquals(2, records.size());
            assertEquals(0L, records.get(0).offset());
            assertEquals(1L, records.get(1).offset());
        }
    }
}
