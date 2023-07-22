package com.tmsvr.databases.lsmtree.memtable;

import com.tmsvr.databases.DataRecord;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Memtable {
    private final Map<String, String> dataMap;

    public Memtable() {
        this.dataMap = new TreeMap<>();
    }

    public Memtable(List<DataRecord> records) {
        this.dataMap = new TreeMap<>();
        records.forEach(record -> dataMap.put(record.key(), record.value()));
    }

    public void put(DataRecord record) {
        dataMap.put(record.key(), record.value());
    }

    public String get(String key) {
        return dataMap.get(key);
    }

    public Map<String, String> getAsMap() {
        return dataMap;
    }

    public long getSize() {
        return dataMap.size();
    }

    public void clear() {
        dataMap.clear();
    }
}
