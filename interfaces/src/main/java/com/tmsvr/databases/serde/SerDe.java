package com.tmsvr.databases.serde;

public interface SerDe<T> {
    T deserialize(String input);

    String serialize(T input);
}
