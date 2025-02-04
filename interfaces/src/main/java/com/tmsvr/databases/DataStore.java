package com.tmsvr.databases;

import java.io.IOException;
import java.util.Optional;

public interface DataStore<K extends Comparable<K>, V> {
    void put(K key, V value) throws IOException;

    Optional<V> get(K key) throws IOException;

    void delete(K key) throws IOException;
}
