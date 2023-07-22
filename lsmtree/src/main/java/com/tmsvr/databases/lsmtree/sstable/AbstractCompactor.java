package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.DataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractCompactor implements Compactor {
    SSTable merge(SSTable olderTable, SSTable newerTable) throws IOException {
        List<DataRecord> result = new ArrayList<>();

        List<DataRecord> oldLines = olderTable.getAllLines();
        List<DataRecord> newLines = newerTable.getAllLines();

        int i = 0;
        int j = 0;

        while (true) {
            DataRecord nextValue;

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

        SSTable newTable = new SSTable("sstable-" + UUID.randomUUID());
        newTable.write(result);
        return newTable;
    }
}
