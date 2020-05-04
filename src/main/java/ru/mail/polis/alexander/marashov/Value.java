package ru.mail.polis.alexander.marashov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final public class Value implements Comparable<Value> {

    private final long timestamp;
    private final ByteBuffer value;

    public Value(final long timestamp, @NotNull final ByteBuffer value) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.value = value;
    }

    public Value(final long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        value = null;
    }

    public boolean isTombstone() {
        return value == null;
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return -Long.compare(this.timestamp, o.timestamp);
    }

    public ByteBuffer getData() {
        assert !isTombstone();
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
