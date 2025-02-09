package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.serde.SerDe;

import java.util.function.Function;

public class LsmSerDe<T> implements SerDe<T> {
    private static final String TOMBSTONE = "<TOMBSTONE>";
    public static final String SEPARATOR = "::";

    private final Function<String, T> deserializer;
    private final Function<T, String> serializer;

    public LsmSerDe(Function<String, T> deserializer, Function<T, String> serializer) {
        this.deserializer = deserializer;
        this.serializer = serializer;
    }

    @Override
    public T deserialize(String input) {
        if (TOMBSTONE.equalsIgnoreCase(input)) {
            return null;
        }

        return deserializer.apply(input);
    }

    @Override
    public String serialize(T input) {
        if (input == null) {
            return TOMBSTONE;
        }

        String result = serializer.apply(input);

        if (result == null || result.contains(SEPARATOR)) {
            throw new IllegalArgumentException("Illegal character in serialized object: " + SEPARATOR);
        }

        return result;
    }
}