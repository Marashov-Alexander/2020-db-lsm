package ru.mail.polis.alexander.marashov;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Class for access to Table data with buffered Cell value.
 */
public class TableIterator {

    public Integer generation;
    public Iterator<Cell> cellIterator;
    public Cell bufferedCell;

    /**
     * Creates an TableIterator instance.
     */
    public TableIterator(
            final Integer generation,
            final Table ssTable
    ) throws IOException {
        this.generation = generation;
        this.cellIterator = ssTable.iterator(ByteBuffer.allocate(0));
        this.bufferedCell = cellIterator.hasNext() ? cellIterator.next() : null;
    }

    public void next() {
        bufferedCell = cellIterator.hasNext() ? cellIterator.next() : null;
    }
}
