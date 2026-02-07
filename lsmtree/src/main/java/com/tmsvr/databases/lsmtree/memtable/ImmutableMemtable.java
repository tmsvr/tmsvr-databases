package com.tmsvr.databases.lsmtree.memtable;

import com.tmsvr.databases.lsmtree.commitlog.CommitLogSegment;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;

public final class ImmutableMemtable<K extends Comparable<K>, V> {

    private final NavigableMap<K, V> data;
    private final CommitLogSegment<K, V> commitLogSegment;

    ImmutableMemtable(NavigableMap<K, V> data, CommitLogSegment<K, V> commitLogSegment) {
        this.data = data;
        this.commitLogSegment = commitLogSegment;
    }

    public V get(K key) {
        return data.get(key);
    }

    public Iterable<Map.Entry<K, V>> entries() {
        return Collections.unmodifiableSet(data.entrySet());
    }

    public void deleteCommitLogSegment() throws IOException {
        commitLogSegment.delete();
    }

    public String getCommitLogSegmentId() {
        return commitLogSegment.getSegmentId();
    }
}