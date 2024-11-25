package com.tmsvr.databases.lsmtree.sstable;

import java.io.IOException;
import java.util.List;

public interface Compactor {
    List<SSTable> compact(List<SSTable> tables) throws IOException;
}
