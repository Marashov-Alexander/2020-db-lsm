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
    private int fileSize;

    private static final ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private static final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

    /**
     * Creates SSTable from file.
     */
    public SSTable(@NotNull final File file) {
        try {
            fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            fileSize = (int) fileChannel.size();
            fileChannel.read(intBuffer.rewind(), fileChannel.size() - Integer.BYTES);
            rowsCount = intBuffer.rewind().getInt();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * Saves cells to file.
     */
    public static void serialize(final Iterator<Cell> iterator,
                                 final int rowsCount, final File file) throws IOException {
        try (FileChannel channel =
                     FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            final ByteBuffer indexesBuffer = ByteBuffer.allocate((rowsCount + 1) * Integer.BYTES);

            final AtomicInteger position = new AtomicInteger(0);
            final AtomicInteger currentRowIndex = new AtomicInteger(0);

            iterator.forEachRemaining(it -> {
                indexesBuffer.putInt(position.get());

                long timestamp = it.getValue().getTimestamp();
                final boolean isTombstone = it.getValue().isTombstone();

                if (isTombstone) {
                    timestamp *= -1;
                }

                intBuffer.rewind().putInt(it.getKey().capacity()).rewind();
                longBuffer.rewind().putLong(timestamp).rewind();
                try {
                    position.addAndGet(channel.write(intBuffer));
                    position.addAndGet(channel.write(it.getKey()));
                    position.addAndGet(channel.write(longBuffer));
                    if (!isTombstone) {
                        intBuffer.rewind().putInt(it.getValue().getData().capacity()).rewind();
                        position.addAndGet(channel.write(intBuffer));
                        position.addAndGet(channel.write(it.getValue().getData()));
                    }
                } catch (IOException e) {
                    log.error(e.getMessage());
                }

                currentRowIndex.incrementAndGet();
            });

            indexesBuffer.putInt(rowsCount);
            indexesBuffer.flip();
            try {
                channel.write(indexesBuffer);
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }

    private int getIntFrom(final int offset) throws IOException {
        intBuffer.position(0);
        fileChannel.read(intBuffer, offset);
        intBuffer.flip();
        return intBuffer.getInt();
    }

    private long getLongFrom(final int offset) throws IOException {
        longBuffer.position(0);
        fileChannel.read(longBuffer, offset);
        longBuffer.flip();
        return longBuffer.getLong();
    }

    private ByteBuffer getFrom(final int offset, final int size) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        fileChannel.read(buffer, offset);
        buffer.flip();
        return buffer;
    }

    private int getOffset(int row) throws IOException {
        intBuffer.rewind();
        fileChannel.read(intBuffer, fileSize - (1 + rowsCount - row) * Integer.BYTES);
        intBuffer.rewind();
        return intBuffer.getInt();
    }

    @NotNull
    private ByteBuffer key(final int row) throws IOException {
        final int offset = getOffset(row);
        final int keyLength = getIntFrom(offset);
        return getFrom(offset + Integer.BYTES, keyLength);
    }

    @NotNull
    private Value value(final int row) throws IOException {
        final int offset = getOffset(row);
        final int keyLength = getIntFrom(offset);

        long timestamp = getLongFrom(offset + Integer.BYTES + keyLength);
        if (timestamp < 0) {
            timestamp *= -1;
            return new Value(timestamp);
        }

        final int valueLength = getIntFrom(offset + Integer.BYTES + keyLength + Long.BYTES);
        final ByteBuffer valueBuffer = getFrom(
                offset + Integer.BYTES + keyLength + Long.BYTES + Integer.BYTES,
                valueLength
        );
        return new Value(timestamp, valueBuffer);
    }

    private int binarySearch(@NotNull final ByteBuffer from) throws IOException {
        assert rowsCount > 0;

        int left = 0;
        int right = rowsCount - 1;

        while (left <= right) {
            final int center = (left + right) / 2;
            int compare = key(center).compareTo(from);
            if (compare < 0) {
                left = center + 1;
            } else if (compare > 0) {
                right = center - 1;
            } else {
                return center;
            }
        }
        return left;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {

            private int rowIndex = binarySearch(from);

            @Override
            public boolean hasNext() {
                return rowIndex < rowsCount;
            }

            @Override
            public Cell next() {
                try {
                    final Cell cell = new Cell(key(rowIndex), value(rowIndex));
                    ++rowIndex;
                    return cell;
                } catch (IOException e) {
                    log.error(e.getMessage());
                    return null;
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public long sizeInBytes() {
        return fileSize;
    }

    @Override
    public int size() {
        return rowsCount;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
