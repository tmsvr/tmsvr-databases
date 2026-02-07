package com.tmsvr.databases.lsmtree.memtable;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.lsmtree.commitlog.CommitLogSegment;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

public final class Memtable<K extends Comparable<K>, V> {

    private ConcurrentSkipListMap<K, V> data;
    private CommitLogSegment<K, V> commitLogSegment;

    public Memtable(CommitLogSegment<K, V> commitLogSegment) {
        this.data = new ConcurrentSkipListMap<>();
        this.commitLogSegment = commitLogSegment;
    }

    public void put(DataRecord<K, V> record) throws IOException {
        var localData = data;
        var localSegment = commitLogSegment;

        if (localData == null || localSegment == null) {
            throw new IllegalStateException("Memtable is frozen");
        }

        localSegment.append(record);
        localData.put(record.key(), record.value());
    }

    public V get(K key) {
        var localData = data;
        return localData != null ? localData.get(key) : null;
    }

    public long getSize() {
        var localData = data;
        return localData != null ? localData.size() : 0;
    }

    /**
     * Freeze this memtable.
     * Ownership of data + WAL segment is transferred exactly once.
     */
    public synchronized ImmutableMemtable<K, V> freeze() {
        if (data == null) {
            throw new IllegalStateException("Memtable already frozen");
        }

        var frozenData = data;
        var frozenSegment = commitLogSegment;

        data = null;
        commitLogSegment = null;

        return new ImmutableMemtable<>(frozenData, frozenSegment);
    }
}
