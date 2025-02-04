package com.tmsvr.databases.lsmtree.sstable;

import java.io.IOException;
import java.util.List;

public interface Compactor<K extends Comparable<K>, V> {
    List<SSTable<K, V>> compact(List<SSTable<K, V>> tables) throws IOException;
}
