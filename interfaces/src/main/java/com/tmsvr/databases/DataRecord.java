package com.tmsvr.databases;

public record DataRecord<K extends Comparable<K>, V>(K key, V value) implements Comparable<DataRecord<K, V>> {

    @Override
    public int compareTo(DataRecord<K, V> other) {
        return this.key.compareTo(other.key);
    }
}

