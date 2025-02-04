package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.DataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractCompactor<K extends Comparable<K>, V> implements Compactor<K, V> {
    SSTable<K, V> merge(SSTable<K, V> olderTable, SSTable<K, V> newerTable) throws IOException {
        List<DataRecord<K, V>> result = new ArrayList<>();

        List<DataRecord<K, V>> oldLines = olderTable.getAllLines();
        List<DataRecord<K, V>> newLines = newerTable.getAllLines();

        int i = 0;
        int j = 0;

        while (true) {
            DataRecord<K, V> nextValue;

            if (i < oldLines.size() && j < newLines.size()) {
                int comparisonResult = oldLines.get(i).key().compareTo(newLines.get(j).key());

                if (comparisonResult < 0) {
                    nextValue = oldLines.get(i);
                    i++;
                } else if (comparisonResult > 0) {
                    nextValue = newLines.get(j);
                    j++;
                } else {
                    nextValue = newLines.get(j);
                    i++;
                    j++;
                }
            } else if (i < oldLines.size()) {
                nextValue = oldLines.get(i);
                i++;
            } else if (j < newLines.size()){
                nextValue = newLines.get(j);
                j++;
            } else {
                break;
            }

            result.add(nextValue);
        }

        SSTable<K, V> newTable = new SSTable<>("sstable-" + UUID.randomUUID(), olderTable.getKeySerDe(), olderTable.getValueSerDe());
        newTable.write(result);
        return newTable;
    }
}
