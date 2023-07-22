package com.tmsvr.databases.lsmtree.sstable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RowCountBasedCompactor extends AbstractCompactor {

    private final int compactionSizeLimit;

    public RowCountBasedCompactor(int compactionSizeLimit) {
        this.compactionSizeLimit = compactionSizeLimit;
    }

    @Override
    public List<SSTable> compact(List<SSTable> tables) throws IOException {
        List<SSTable> result = new ArrayList<>();

        for (int i = 0; i < tables.size(); i++) {
            SSTable table = tables.get(i);

            if (table.getSize() > compactionSizeLimit) {
                result.add(table);
            } else {
                SSTable mergedTable = table;

                while (i + 1 < tables.size() && mergedTable.getSize() <= compactionSizeLimit) {
                    mergedTable = merge(mergedTable, tables.get(i + 1));
                    i++;
                }

                result.add(mergedTable);
            }
        }

        return result;
    }

}
