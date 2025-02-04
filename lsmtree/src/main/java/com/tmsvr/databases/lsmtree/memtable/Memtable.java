package com.tmsvr.databases.lsmtree.memtable;

import com.tmsvr.databases.DataRecord;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Memtable<K extends Comparable<K>, V> {
    private final Map<K, V> dataMap;

    public Memtable() {
        this.dataMap = new TreeMap<>();
    }

    public Memtable(List<DataRecord<K, V>> records) {
        this.dataMap = new TreeMap<>();
        records.forEach(record -> dataMap.put(record.key(), record.value()));
    }

    public void put(DataRecord<K, V> record) {
        dataMap.put(record.key(), record.value());
    }

    public V get(K key) {
        return dataMap.get(key);
    }

    public Map<K, V> getAsMap() {
        return dataMap;
    }

    public long getSize() {
        return dataMap.size();
    }

    public void clear() {
        dataMap.clear();
    }
}
