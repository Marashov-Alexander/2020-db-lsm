package ru.mail.polis.alexander.marashov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class Value implements Comparable<Value> {

    private final long timestamp;
    private final ByteBuffer data;

    /**
     * Creates the Value instance.
     */
    public Value(final long timestamp, @NotNull final ByteBuffer data) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = data;
    }

    /**
     * Creates the Value instance for tombstone.
     */
    public Value(final long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        data = null;
    }

    public boolean isTombstone() {
        return data == null;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(this.timestamp, o.timestamp);
    }

    public ByteBuffer getData() {
        assert !isTombstone();
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
