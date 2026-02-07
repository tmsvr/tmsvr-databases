package com.tmsvr.databases.lsmtree.commitlog;

import java.io.IOException;
import java.util.List;

/**
 * Factory for creating and recovering commit log segments.
 * Each segment is owned by a single Memtable instance.
 */
public interface CommitLog<K extends Comparable<K>, V> {
    /**
     * Create a new commit log segment with a unique ID.
     */
    CommitLogSegment<K, V> createSegment() throws IOException;

    /**
     * Recover all existing segments from disk (used during startup for crash
     * recovery).
     */
    List<CommitLogSegment<K, V>> recoverSegments() throws IOException;
}
