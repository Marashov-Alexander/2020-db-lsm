package ru.mail.polis.alexander.marashov;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class SSTable implements Table {

    private static final Logger log = LoggerFactory.getLogger(SSTable.class);

    private FileChannel fileChannel;
    private int rowsCount;
    private Integer[] offsets;

    public SSTable(@NotNull final File file) {
        try {
            deserialize(file);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public static void serialize(final Iterator<Cell> iterator, final int rowsCount, final File file) throws IOException {
        FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        ByteBuffer indexesBuffer = ByteBuffer.allocate((rowsCount + 1) * Integer.BYTES);

        final AtomicInteger position = new AtomicInteger(0);
        final AtomicInteger currentRowIndex = new AtomicInteger(0);

        iterator.forEachRemaining(it -> {
            indexesBuffer.putInt(position.get());

            // key length, key, timestamp
            int size = Integer.BYTES + it.getKey().capacity() + Long.BYTES;

            long timestamp = it.getValue().getTimestamp();
            boolean isTombstone = it.getValue().isTombstone();

            if (isTombstone) {
                timestamp *= -1;
            } else {
                // + value length, value
                size += Integer.BYTES + it.getValue().getData().capacity();
            }

            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.putInt(it.getKey().capacity());
            buffer.put(it.getKey());
            buffer.putLong(timestamp);

            if (!isTombstone) {
                buffer.putInt(it.getValue().getData().capacity());
                buffer.put(it.getValue().getData());
            }

            buffer.flip();

            try {
                position.addAndGet(fileChannel.write(buffer));
            } catch (IOException e) {
                log.error(e.getMessage());
            }

            currentRowIndex.incrementAndGet();
        });

        indexesBuffer.putInt(rowsCount);
        indexesBuffer.flip();
        try {
            fileChannel.write(indexesBuffer);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private void deserialize(final File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);

        ByteBuffer rowsCountBuffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(rowsCountBuffer, fileChannel.size() - Integer.BYTES);
        rowsCountBuffer.flip();

        rowsCount = rowsCountBuffer.getInt();
        offsets = new Integer[rowsCount];

        ByteBuffer indexesBuffer = ByteBuffer.allocate(rowsCount * Integer.BYTES);
        fileChannel.read(indexesBuffer, fileChannel.size() - (rowsCount + 1) * Integer.BYTES);
        indexesBuffer.flip();

        for (int i = 1; i <= rowsCount; ++i) {
            offsets[i - 1] = indexesBuffer.getInt();
        }
    }

    private ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private int getIntFrom(final int offset) throws IOException {
        intBuffer.position(0);
        fileChannel.read(intBuffer, offset);
        intBuffer.flip();
        return intBuffer.getInt();
    }

    private ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
    private long getLongFrom(final int offset) throws IOException {
        longBuffer.position(0);
        fileChannel.read(longBuffer, offset);
        longBuffer.flip();
        return longBuffer.getLong();
    }

    private ByteBuffer getFrom(final int offset, final int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        fileChannel.read(buffer, offset);
        buffer.flip();
        return buffer;
    }

    @NotNull
    private ByteBuffer key(final int row) throws IOException {
        final int offset = offsets[row];
        final int keyLength = getIntFrom(offset);
        return getFrom(offset + Integer.BYTES, keyLength);
    }

    @NotNull
    private Value value(final int row) throws IOException {
        final int offset = offsets[row];
        final int keyLength = getIntFrom(offset);

        long timestamp = getLongFrom(offset + Integer.BYTES + keyLength);
        if (timestamp < 0) {
            timestamp *= -1;
            return new Value(timestamp);
        }

        final int valueLength = getIntFrom(offset + Integer.BYTES + keyLength + Long.BYTES);
        ByteBuffer valueBuffer = getFrom(
                offset + Integer.BYTES + keyLength + Long.BYTES + Integer.BYTES,
                valueLength
        );
        return new Value(timestamp, valueBuffer);
    }

    private int binarySearch(@NotNull ByteBuffer from) throws IOException {
        assert rowsCount > 0;

        int left = 0;
        int right = rowsCount - 1;

        ByteBuffer foundKey;
        while (left < right - 1) {
            int center = (left + right + 1) / 2;
            foundKey = key(center);
            if (from.compareTo(foundKey) <= 0) {
                right = center;
            } else {
                left = center;
            }
        }
        final int leftToFrom = key(left).compareTo(from);
        if (leftToFrom >= 0) {
            return left;
        } else {
            final int rightToFrom = key(right).compareTo(from);
            if (rightToFrom < 0) {
                return rowsCount;
            } else {
                return right;
            }
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new Iterator<>() {

            private int rowIndex = binarySearch(from);
            @Override
            public boolean hasNext() {
                return rowIndex < rowsCount;
            }

            @Override
            public Cell next() {
                try {
                    Cell cell = new Cell(key(rowIndex), value(rowIndex));
                    ++rowIndex;
                    return cell;
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
                return null;
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public long sizeInBytes() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return -1;
    }

    @Override
    public int size() {
        return rowsCount;
    }
}
