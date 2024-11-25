package com.tmsvr.databases.lsmtree.sstable;

import java.util.HashMap;
import java.util.Map;

public abstract class SSTableFixtures {
    final static String FILENAME = "test-sstable";
    final static String KEY_1 = "key1";
    final static String VALUE_1 = "value1";
    final static String KEY_2 = "key2";
    final static String VALUE_2 = "value2";
    final static String KEY_3 = "key3";
    final static String VALUE_3 = "value3";

    static Map<String, String> aDataSet() {
        HashMap<String, String> data = new HashMap<>();
        data.put(KEY_1, VALUE_1);
        data.put(KEY_2, VALUE_2);
        data.put(KEY_3, VALUE_3);

        return data;
    }
}
