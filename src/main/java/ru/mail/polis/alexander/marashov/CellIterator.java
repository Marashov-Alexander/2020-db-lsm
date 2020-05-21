package ru.mail.polis.alexander.marashov;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class CellIterator implements Iterator<Cell> {

    private final List<TableIterator> iterators;
    private final Map<ByteBuffer, List<Integer>> keyEqualityMap;
    private Cell minCell;

    public CellIterator(List<TableIterator> tableIteratorList) {
        this.iterators = tableIteratorList;
        this.keyEqualityMap = new HashMap<>();
    }

    @Override
    public boolean hasNext() {
        if (minCell != null) {
            return true;
        }

        findMinCell();
        if (minCell == null) {
            return false;
        }
        final List<Integer> tableIndexesList = keyEqualityMap.get(minCell.getKey());
        for (final Integer index : tableIndexesList) {
            iterators.get(index).next();
        }
        return true;

    }

    @Override
    public Cell next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more cells");
        }
        final Cell result = minCell;
        minCell = null;
        return result;
    }

    private void findMinCell() {
        keyEqualityMap.clear();
        minCell = null;
        for (final TableIterator tableIterator : iterators) {
            final Cell cell = tableIterator.bufferedCell;
            if (cell != null) {
                final List<Integer> tableIndexesList = keyEqualityMap.computeIfAbsent(
                        cell.getKey(),
                        k -> new ArrayList<>()
                );
                tableIndexesList.add(tableIterator.generation);
                if (minCell == null || minCell.getKey().compareTo(cell.getKey()) >= 0) {
                    minCell = cell;
                }
            }
        }
    }
}
