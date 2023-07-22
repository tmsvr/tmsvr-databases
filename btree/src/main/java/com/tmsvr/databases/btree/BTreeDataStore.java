package com.tmsvr.databases.btree;

import com.tmsvr.databases.DataStore;

import java.io.IOException;
import java.util.Optional;

public class BTreeDataStore implements DataStore {
    @Override
    public void put(String key, String value) throws IOException {

    }

    @Override
    public Optional<String> get(String key) throws IOException {
        return Optional.empty();
    }

    @Override
    public void delete(String key) throws IOException {

    }
}
