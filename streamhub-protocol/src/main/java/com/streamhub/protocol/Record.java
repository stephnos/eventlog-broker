package com.streamhub.protocol;

import java.util.Arrays;
import java.util.Objects;

public record Record(
        long offset,
        long timestamp,
        byte[] key,
        byte[] value
) {
    public Record {
        key = key == null ? null : key.clone();
        value = value == null ? new byte[0] : value.clone();
    }

    @Override
    public byte[] key() {
        return key == null ? null : key.clone();
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record r)) return false;
        return offset == r.offset && timestamp == r.timestamp
                && Arrays.equals(key, r.key) && Arrays.equals(value, r.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, timestamp, Arrays.hashCode(key), Arrays.hashCode(value));
    }
}
