package com.tmsvr.databases;

public record DataRecord(String key, String value) implements Comparable<DataRecord> {

    @Override
    public int compareTo(DataRecord o) {
        return this.key.compareTo(o.key);
    }
}
