package com.tmsvr.databases.lsmtree.sstable.bloomfilter;

import com.tmsvr.databases.lsmtree.sstable.bloomfilter.BloomFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    private BloomFilter<String> bloomFilter;

    @BeforeEach
    void setUp() {
        // Expecting 1000 elements with 1% false positive rate
        bloomFilter = new BloomFilter<>(1000, 0.01);
    }

    @Test
    void testEmptyFilter() {
        assertFalse(bloomFilter.isPresent("empty"), "An empty Bloom filter should return false for any lookup");
    }

    @Test
    void testAddAndCheckPresence() {
        bloomFilter.add("hello");
        bloomFilter.add("world");

        assertTrue(bloomFilter.isPresent("hello"), "Element 'hello' should be present");
        assertTrue(bloomFilter.isPresent("world"), "Element 'world' should be present");
    }

    @Test
    void testNonExistingElement() {
        bloomFilter.add("test");
        bloomFilter.add("apple");
        bloomFilter.add("tree");

        assertFalse(bloomFilter.isPresent("random"), "Element 'random' should not be present");
    }

    @Test
    void testFalsePositiveRate() {
        int falsePositives = 0;
        int testSize = 10000;

        for (int i = 0; i < 1000; i++) {
            bloomFilter.add("test" + i);
        }

        for (int i = 1000; i < testSize; i++) {
            if (bloomFilter.isPresent("test" + i)) {
                falsePositives++;
            }
        }

        double falsePositiveRate = (double) falsePositives / testSize;
        System.out.println("False Positive Rate: " + falsePositiveRate);

        assertTrue(falsePositiveRate <= 0.01, "False positive rate should be within expected range");
    }

    @Test
    void testSerializationAndDeserialization() {
        for (int i = 0; i < 1000; i++) {
            bloomFilter.add("test" + i);
        }

        String serializedData = bloomFilter.serialize();
        BloomFilter<String> reconstructedFilter = new BloomFilter<>(1000, 0.01, serializedData);

        assertNotNull(serializedData, "Serialized data should not be null");
        for (int i = 0; i < 2000; i++) {
            assertEquals(bloomFilter.isPresent("test" + i), reconstructedFilter.isPresent("test" + i));
        }
    }
}
