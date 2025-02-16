package com.tmsvr.databases.lsmtree.sstable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class RowCountBasedCompactor<K extends Comparable<K>, V> extends AbstractCompactor<K, V> {

    private final int compactionSizeLimit;

    public RowCountBasedCompactor(int compactionSizeLimit) {
        this.compactionSizeLimit = compactionSizeLimit;
    }

    @Override
    public List<SSTable<K, V>> compact(List<SSTable<K, V>> tables) throws IOException {
        log.info("Running compaction on tables: " + tables.stream().map(SSTable::getName).collect(Collectors.joining(", ")));
        List<SSTable<K, V>> result = new ArrayList<>();

        for (int i = 0; i < tables.size(); i++) {
            SSTable<K, V> table = tables.get(i);

            if (table.getSize() > compactionSizeLimit) {
                result.add(table);
            } else {
                SSTable<K, V> mergedTable = table;

                while (i + 1 < tables.size() && mergedTable.getSize() <= compactionSizeLimit) {
                    mergedTable = merge(mergedTable, tables.get(i + 1));
                    i++;
                }

                result.add(mergedTable);
            }
        }

        log.info("Compaction result tables: " + result.stream().map(SSTable::getName).collect(Collectors.joining(", ")));
        return result;
    }
}
