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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * Persistent storage.
 *
 * @author Alexander Marashov
 */
public class DAOImpl implements DAO {

    private static final Logger log = LoggerFactory.getLogger(DAOImpl.class);

    private static String SUFFIX = ".dat";
    private static String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushThreshold;

    private Table memTable = new MemTable();
    private final NavigableMap<Integer, Table> ssTables;

    private int generation;

    /**
     * Creates DAO from storage file with flushThreshold data limit.
     */
    public DAOImpl(@NotNull final File storage, final long flushThreshold) {
        assert flushThreshold > 0L;
        this.flushThreshold = flushThreshold;
        this.storage = storage;
        this.ssTables = new TreeMap<>();
        doWithFiles(storage.toPath(), (gen, path) -> {
            ssTables.put(gen, new SSTable(path.toFile()));
            generation = Math.max(generation, gen);
        });
        ++generation;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memTable.iterator(from));
        for (final Table t : ssTables.descendingMap().values()) {
            iters.add(t.iterator(from));
        }
        final Iterator<Cell> merged = Iterators.mergeSorted(iters, Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, i -> !i.getValue().isTombstone());
        return Iterators.transform(alive, i -> Record.of(i.getKey(), i.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.asReadOnlyBuffer(), value.asReadOnlyBuffer());
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.asReadOnlyBuffer());
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    /**
     * Saving data on the disk.
     */
    public void flush() throws IOException {
        final File file = new File(storage, generation + TEMP);
        SSTable.serialize(
                memTable.iterator(ByteBuffer.allocate(0)),
                memTable.size(),
                file
        );
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        memTable.close();
        memTable = new MemTable();
        ssTables.put(generation, new SSTable(dst));
        ++generation;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }
        ssTables.forEach((i, t) -> {
            try {
                t.close();
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
            }
        });
    }

    @Override
    public void compact() throws IOException {
        if (memTable.size() != 0) {
            flush();
        }
        final int lastGen = generation;

        final List<Triple<Integer, Iterator<Cell>, Cell>> tripleList = new ArrayList<>(ssTables.size());
        ssTables.forEach((gen, table) -> {
            try {
                final Iterator<Cell> iterator = table.iterator(ByteBuffer.allocate(0));
                final Cell bufferedCell = iterator.hasNext() ? iterator.next() : null;
                tripleList.add(new Triple<>(gen - 1, iterator, bufferedCell));
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        });

        mergeTables(tripleList);
        doWithFiles(storage.toPath(), (gen, path) -> {
            if (gen < lastGen) {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        });
    }

    private void doWithFiles(final Path storagePath, final BiConsumer<Integer, Path> genBiConsumer) {
        try (Stream<Path> stream = Files.list(storagePath)) {
            stream.filter(p -> p.toString().endsWith(SUFFIX))
                    .forEach(path -> {
                        final String name = path.getFileName().toString();
                        final String genStr = name.substring(0, name.indexOf(SUFFIX));
                        if (genStr.matches("[0-9]+")) {
                            final int gen = Integer.parseInt(genStr);
                            genBiConsumer.accept(gen, path);
                        }
                    });
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
        }
    }

    private void mergeTables(final List<Triple<Integer, Iterator<Cell>, Cell>> tripleList) throws IOException {
        final Map<ByteBuffer, List<Integer>> map = new HashMap<>();
        while (true) {
            final Cell minCell = getMinCell(tripleList, map);
            if (minCell == null) {
                // end of compaction
                break;
            }
            final List<Integer> tableIndexesList = map.get(minCell.getKey());
            for (final Integer index : tableIndexesList) {
                final Triple<Integer, Iterator<Cell>, Cell> triple = tripleList.get(index);
                triple.third = triple.second.hasNext() ? triple.second.next() : null;
            }
            if (minCell.getValue().isTombstone()) {
                remove(minCell.getKey());
            } else {
                upsert(minCell.getKey(), minCell.getValue().getData());
            }
        }
    }

    private Cell getMinCell(
            final List<Triple<Integer, Iterator<Cell>, Cell>> tripleList,
            final Map<ByteBuffer, List<Integer>> map
    ) {
        map.clear();
        Cell minCell = null;
        for (final Triple<Integer, Iterator<Cell>, Cell> triple : tripleList) {
            final Cell cell = triple.third;
            if (cell != null) {
                final List<Integer> tableIndexesList = map.computeIfAbsent(cell.getKey(), k -> new ArrayList<>());
                tableIndexesList.add(triple.first);
                if (minCell == null || minCell.getKey().compareTo(cell.getKey()) >= 0) {
                    minCell = cell;
                }
            }
        }
        return minCell;
    }
}
