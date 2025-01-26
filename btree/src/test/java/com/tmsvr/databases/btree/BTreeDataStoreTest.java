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
        dataStore.put("key1", "newValue1");

        Optional<String> valueFromDb = dataStore.get("key1");
        assertTrue(valueFromDb.isPresent());
        assertEquals("newValue1", valueFromDb.get());
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

    @Test
    void testDeleteLeafKey() {
        dataStore.put("key1", "value1");
        dataStore.put("key2", "value2");

        dataStore.delete("key1");

        assertFalse(dataStore.get("key1").isPresent(), "Expected key1 to be deleted");
        assertEquals(Optional.of("value2"), dataStore.get("key2"), "key2 should still exist");
    }

    @Test
    void testDeleteInternalKeyWithPredecessor() {
        dataStore.put("a", "1");
        dataStore.put("b", "2");
        dataStore.put("c", "3");
        dataStore.put("d", "4");

        dataStore.delete("b");

        assertFalse(dataStore.get("b").isPresent(), "Expected key 'b' to be deleted");
        assertEquals(Optional.of("1"), dataStore.get("a"), "Key 'a' should still exist");
        assertEquals(Optional.of("3"), dataStore.get("c"), "Key 'c' should still exist");
        assertEquals(Optional.of("4"), dataStore.get("d"), "Key 'd' should still exist");
    }

    @Test
    void testDeleteKeyCausesMerge() {
        dataStore.put("a", "1");
        dataStore.put("b", "2");
        dataStore.put("c", "3");
        dataStore.put("d", "4");
        dataStore.put("e", "5");
        dataStore.put("f", "6");

        // Delete keys to force merging of nodes
        dataStore.delete("f");
        dataStore.delete("e");
        dataStore.delete("d");

        assertFalse(dataStore.get("f").isPresent(), "Expected key 'f' to be deleted");
        assertFalse(dataStore.get("e").isPresent(), "Expected key 'e' to be deleted");
        assertFalse(dataStore.get("d").isPresent(), "Expected key 'd' to be deleted");

        assertEquals(Optional.of("1"), dataStore.get("a"), "Key 'a' should still exist");
        assertEquals(Optional.of("2"), dataStore.get("b"), "Key 'b' should still exist");
        assertEquals(Optional.of("3"), dataStore.get("c"), "Key 'c' should still exist");
    }

    @Test
    void testDeleteRootKey() {
        dataStore.put("a", "1");
        dataStore.put("b", "2");
        dataStore.put("c", "3");

        dataStore.delete("b");

        assertFalse(dataStore.get("b").isPresent(), "Expected key 'b' to be deleted");
        assertEquals(Optional.of("1"), dataStore.get("a"), "Key 'a' should still exist");
        assertEquals(Optional.of("3"), dataStore.get("c"), "Key 'c' should still exist");
    }

    @Test
    void testDeleteNonExistentKey() {
        dataStore.put("key1", "value1");
        dataStore.put("key2", "value2");

        dataStore.delete("nonExistentKey");

        assertEquals(Optional.of("value1"), dataStore.get("key1"), "Key1 should still exist");
        assertEquals(Optional.of("value2"), dataStore.get("key2"), "Key2 should still exist");
    }

    @Test
    void testDeleteKeyInComplexTree() {
        // Insert enough keys to create a multi-level BTree
        dataStore.put("a", "1");
        dataStore.put("b", "2");
        dataStore.put("c", "3");
        dataStore.put("d", "4");
        dataStore.put("e", "5");
        dataStore.put("f", "6");
        dataStore.put("g", "7");
        dataStore.put("h", "8");
        dataStore.put("i", "9");

        // Delete a key that will require rebalancing
        dataStore.delete("e");

        assertFalse(dataStore.get("e").isPresent(), "Expected key 'e' to be deleted");
        assertEquals(Optional.of("1"), dataStore.get("a"), "Key 'a' should still exist");
        assertEquals(Optional.of("2"), dataStore.get("b"), "Key 'b' should still exist");
        assertEquals(Optional.of("3"), dataStore.get("c"), "Key 'c' should still exist");
        assertEquals(Optional.of("4"), dataStore.get("d"), "Key 'd' should still exist");
        assertEquals(Optional.of("6"), dataStore.get("f"), "Key 'f' should still exist");
        assertEquals(Optional.of("7"), dataStore.get("g"), "Key 'g' should still exist");
        assertEquals(Optional.of("8"), dataStore.get("h"), "Key 'h' should still exist");
        assertEquals(Optional.of("9"), dataStore.get("i"), "Key 'i' should still exist");
    }
}
