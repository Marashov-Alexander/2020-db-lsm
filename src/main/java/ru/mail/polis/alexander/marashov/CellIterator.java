package ru.mail.polis.alexander.marashov;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class CellIterator implements Iterator<Cell> {

    private final PriorityQueue<TableIterator> tableIteratorPriorityQueue;

    public CellIterator(final List<TableIterator> tableIteratorList) {
        tableIteratorPriorityQueue = new PriorityQueue<>(tableIteratorList.size(), (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }
            final int comp = o1.bufferedCell.getKey().compareTo(o2.bufferedCell.getKey());
            if (comp != 0) {
                return comp;
            } else {
                return o1.generation > o2.generation ? 1 : -1;
            }
        }) {
            @Override
            public boolean add(TableIterator tableIterator) {
                if (tableIterator.bufferedCell != null) {
                    return super.add(tableIterator);
                } else {
                    return false;
                }
            }
        };
        tableIteratorPriorityQueue.addAll(tableIteratorList);
    }

    @Override
    public boolean hasNext() {
        return !tableIteratorPriorityQueue.isEmpty();
    }

    @Override
    public Cell next() {
        if (tableIteratorPriorityQueue.isEmpty()) {
            throw new NoSuchElementException("No more cells");
        }
        final TableIterator topTable = tableIteratorPriorityQueue.poll();
        final Cell result = topTable.bufferedCell;
        while (!tableIteratorPriorityQueue.isEmpty()) {
            final Cell nextCell = tableIteratorPriorityQueue.peek().bufferedCell;
            if (nextCell.getKey().compareTo(result.getKey()) != 0) {
                break;
            }
            final TableIterator tableIterator = tableIteratorPriorityQueue.poll();
            tableIterator.next();
            tableIteratorPriorityQueue.add(tableIterator);
        }
        topTable.next();
        tableIteratorPriorityQueue.add(topTable);
        return result;
    }
}
