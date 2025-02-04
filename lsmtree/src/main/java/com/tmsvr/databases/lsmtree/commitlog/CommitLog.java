package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;

import java.io.IOException;
import java.util.List;

public interface CommitLog<K extends Comparable<K>, V> {
    void append(DataRecord<K,V> entry) throws IOException;

    List<DataRecord<K,V>> readCommitLog() throws IOException;

    long getSize();

    void clear() throws IOException;
}
