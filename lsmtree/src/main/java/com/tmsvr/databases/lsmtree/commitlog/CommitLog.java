package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;

import java.io.IOException;
import java.util.List;

public interface CommitLog {
    void append(DataRecord entry) throws IOException;

    List<DataRecord> readCommitLog() throws IOException;

    long getSize();

    void clear() throws IOException;
}
