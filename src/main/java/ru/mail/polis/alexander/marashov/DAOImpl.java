package ru.mail.polis.alexander.marashov;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class DAOImpl implements DAO {

    private static final Logger log = LoggerFactory.getLogger(DAOImpl.class);

    private static String SUFFIX = ".dat";
    private static String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushThreshold;

    private Table memTable = new MemTable();
    private final NavigableMap<Integer, Table> ssTables;

    private int generation = 0;

    public DAOImpl(@NotNull final File storage, final long flushThreshold) throws IOException {
        assert flushThreshold > 0L;
        this.flushThreshold = flushThreshold;
        this.storage = storage;
        this.ssTables = new TreeMap<>();
        try (final Stream<Path> stream = Files.list(storage.toPath())) {
            stream.filter(p -> p.toString().endsWith(SUFFIX))
                    .forEach(f -> {
                        String name = f.getFileName().toString();
                        final int generation = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                        ssTables.put(generation, new SSTable(f.toFile()));
                        this.generation = Math.max(this.generation, generation);
                    });
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        Iterator<Cell> memIter = memTable.iterator(from);
        if (memIter.hasNext()) {
            iters.add(memIter);
        }
        for (Table t : ssTables.descendingMap().values()) {
            Iterator<Cell> tableIter = t.iterator(from);
            if (tableIter.hasNext()) {
                iters.add(tableIter);
            }
        }
        final Iterator<Cell> merged = Iterators.mergeSorted(iters, Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, i -> !i.getValue().isTombstone());
        return Iterators.transform(alive, i -> Record.of(i.getKey(), i.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key.asReadOnlyBuffer(), value.asReadOnlyBuffer());
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key.asReadOnlyBuffer());
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    public void flush() throws IOException {
        final File file = new File(storage, generation + TEMP);
        SSTable.serialize(
                memTable.iterator(ByteBuffer.allocate(0)),
                memTable.size(),
                file
        );
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        memTable = new MemTable();
        ssTables.put(generation, new SSTable(dst));
        ++generation;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }
    }
}
