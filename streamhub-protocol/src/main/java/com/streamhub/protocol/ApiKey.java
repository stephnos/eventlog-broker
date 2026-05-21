package com.streamhub.protocol;

public enum ApiKey {
    CREATE_TOPIC(0),
    DELETE_TOPIC(1),
    PRODUCE(2),
    FETCH(3),
    JOIN_GROUP(4),
    HEARTBEAT(5),
    SYNC_GROUP(6),
    COMMIT_OFFSET(7),
    LEAVE_GROUP(8),
    METADATA(9),
    ERROR(127);

    private final byte code;

    ApiKey(int code) {
        this.code = (byte) code;
    }

    public byte code() {
        return code;
    }

    public static ApiKey fromCode(byte code) {
        for (ApiKey k : values()) {
            if (k.code == code) return k;
        }
        throw new IllegalArgumentException("Unknown api key: " + code);
    }
}
