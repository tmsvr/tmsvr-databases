package com.tmsvr.databases.lsmtree.sstable.bloomfilter;

import com.tmsvr.databases.lsmtree.sstable.bloomfilter.hash.HashFunction;
import com.tmsvr.databases.lsmtree.sstable.bloomfilter.hash.MurmurHashFunction;

import java.util.Base64;
import java.util.BitSet;

public class BloomFilter<K> {

    private final BitSet bitSet;
    private final int size;
    private final HashFunction hashFunction = new MurmurHashFunction();
    private final int hashFunctions;

    /**
     * Constructor that configures the bloom filter.
     *
     * @param expectedElements  number of expected elements in this filter
     * @param falsePositiveRate the rate of false positives we can accept 1 = 100%, 0.01 = 1% etc
     */
    public BloomFilter(int expectedElements, double falsePositiveRate) {
        this(expectedElements, falsePositiveRate, null);
    }

    public BloomFilter(int expectedElements, double falsePositiveRate, String data) {
        this.size = calculateSize(expectedElements, falsePositiveRate);
        this.hashFunctions = calculateHashFunctions(expectedElements, size);
        this.bitSet = (data != null) ? BitSet.valueOf(Base64.getDecoder().decode(data)) : new BitSet(size);
    }

    public void add(K key) {
        int hash1 = hashFunction.hash(toBytes(key));
        int hash2 = (hash1 >>> 16) | (hash1 << 16); // Mix bits for variation

        for (int i = 0; i < hashFunctions; i++) {
            int hash = (hash1 + i * hash2) & Integer.MAX_VALUE; // Ensure positive index
            int index = hash % size;
            bitSet.set(index);
        }
    }

    public boolean isPresent(K key) {
        int hash1 = hashFunction.hash(toBytes(key));
        int hash2 = (hash1 >>> 16) | (hash1 << 16);

        for (int i = 0; i < hashFunctions; i++) {
            int hash = (hash1 + i * hash2) & Integer.MAX_VALUE;
            int index = hash % size;
            if (!bitSet.get(index)) {
                return false;
            }
        }
        return true;
    }

    public String serialize() {
        byte[] bytes = bitSet.toByteArray();
        return Base64.getEncoder().encodeToString(bytes);
    }

    private int calculateSize(int expectedElements, double falsePositiveRate) {
        return (int) Math.ceil(-expectedElements * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
    }

    private int calculateHashFunctions(int expectedElements, int size) {
        return (int) Math.round((size / (double) expectedElements) * Math.log(2));
    }

    private byte[] toBytes(K key) {
        return key.toString().getBytes();
    }
}
