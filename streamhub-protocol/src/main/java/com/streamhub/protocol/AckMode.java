package com.streamhub.protocol;

public enum AckMode {
    ACK_NONE(0),
    ACK_LEADER(1),
    ACK_ALL(2);

    private final byte code;

    AckMode(int code) {
        this.code = (byte) code;
    }

    public byte code() {
        return code;
    }

    public static AckMode fromCode(byte code) {
        for (AckMode m : values()) {
            if (m.code == code) return m;
        }
        throw new IllegalArgumentException("Unknown ack mode: " + code);
    }
}
