package com.tmsvr.databases;

import java.io.IOException;
import java.util.Optional;

public interface DataStore {
    void put(String key, String value) throws IOException;

    Optional<String> get(String key) throws IOException;

    void delete(String key) throws IOException;
}
