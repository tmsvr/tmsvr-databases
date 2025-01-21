package com.tmsvr.databases.btree;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class BTreeDataStoreTest {

    private BTreeDataStore dataStore;

    @BeforeEach
    void setUp() {
        dataStore = new BTreeDataStore();
    }

    @Test
    void testPutAndGetSingleRecord() {
        dataStore.put("key1", "value1");
        Optional<String> result = dataStore.get("key1");

        assertTrue(result.isPresent(), "Expected key1 to be present");
        assertEquals("value1", result.get(), "Expected value1 for key1");
    }

    @Test
    void testPutMultipleRecords() {
        dataStore.put("key1", "value1");
        dataStore.put("key2", "value2");
        dataStore.put("key3", "value3");

        assertEquals(Optional.of("value1"), dataStore.get("key1"), "Expected value1 for key1");
        assertEquals(Optional.of("value2"), dataStore.get("key2"), "Expected value2 for key2");
        assertEquals(Optional.of("value3"), dataStore.get("key3"), "Expected value3 for key3");
    }

    @Test
    void testGetNonExistentKey() {
        dataStore.put("key1", "value1");

        Optional<String> result = dataStore.get("nonExistentKey");

        assertFalse(result.isPresent(), "Expected nonExistentKey to not be present");
    }

    @Test
    void testOverwriteValueForExistingKey() {
        dataStore.put("key1", "value1");

        assertThrows(IllegalArgumentException.class, () -> dataStore.put("key1", "newValue1"), "Duplicate keys should not be allowed");
    }

    @Test
    void testBTreeHandlesSplit() {
        // Insert enough keys to cause a split in the BTree
        dataStore.put("a", "1");
        dataStore.put("b", "2");
        dataStore.put("c", "3");
        dataStore.put("d", "4");
        dataStore.put("e", "5");
        dataStore.put("f", "6");

        // Validate that all keys are correctly retrievable after the split
        assertEquals(Optional.of("1"), dataStore.get("a"));
        assertEquals(Optional.of("2"), dataStore.get("b"));
        assertEquals(Optional.of("3"), dataStore.get("c"));
        assertEquals(Optional.of("4"), dataStore.get("d"));
        assertEquals(Optional.of("5"), dataStore.get("e"));
        assertEquals(Optional.of("6"), dataStore.get("f"));
    }

    @Test
    void testEmptyTree() {
        Optional<String> result = dataStore.get("nonExistentKey");
        assertFalse(result.isPresent(), "Expected an empty tree to return no result for any key");
    }

    @Test
    void testPutNullKeyThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> dataStore.put(null, "value"), "Expected NullPointerException for null key");
    }
}
