package com.tmsvr.databases.lsmtree.sstable.bloomfilter;

import com.tmsvr.databases.lsmtree.sstable.bloomfilter.hash.HashFunction;
import com.tmsvr.databases.lsmtree.sstable.bloomfilter.hash.MurmurHashFunction;
import com.tmsvr.databases.lsmtree.util.FileUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.BitSet;
import java.util.function.Consumer;

@Slf4j
public class BloomFilter<K> {
    private static final String FILTER_FILE_SUFFIX = ".filter";

    private final BitSet bitSet;
    private final int size;
    private final HashFunction hashFunction = new MurmurHashFunction();
    private final int hashFunctions;

    private final Path filterFile;

    /**
     * Constructor that configures the bloom filter.
     *
     * @param expectedElements  number of expected elements in this filter
     * @param falsePositiveRate the rate of false positives we can accept 1 = 100%, 0.01 = 1% etc
     * @param filename          name of the SSTable this filter belongs to
     */
    public BloomFilter(int expectedElements, double falsePositiveRate, String filename) throws IOException {
        this.filterFile = Paths.get(filename + FILTER_FILE_SUFFIX);
        this.size = calculateSize(expectedElements, falsePositiveRate);
        this.hashFunctions = calculateHashFunctions(expectedElements, size);

        this.bitSet = loadFromDisk();
    }

    public void add(K key) {
        processKeyHashes(key, bitSet::set);
    }

    public boolean isPresent(K key) {
        return processKeyHashes(key, index -> {
            if (!bitSet.get(index)) {
                throw new RuntimeException("Not present");
            }
        });
    }

    public void saveToDisk() throws IOException {
        FileUtils.saveToDisk(filterFile, bitSet, x -> Base64.getEncoder().encodeToString(x.toByteArray()));
    }

    private boolean processKeyHashes(K key, Consumer<Integer> indexProcessor) {
        int hash1 = hashFunction.hash(key.toString().getBytes());
        int hash2 = (hash1 >>> 16) | (hash1 << 16); // Mix bits for variation

        for (int i = 0; i < hashFunctions; i++) {
            int hash = (hash1 + i * hash2) & Integer.MAX_VALUE; // Ensure positive index
            int index = hash % size;
            try {
                indexProcessor.accept(index);
            } catch (RuntimeException e) {
                return false;
            }
        }
        return true;
    }

    private int calculateSize(int expectedElements, double falsePositiveRate) {
        return (int) Math.ceil(-expectedElements * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
    }

    private int calculateHashFunctions(int expectedElements, int size) {
        return (int) Math.round((size / (double) expectedElements) * Math.log(2));
    }

    private BitSet loadFromDisk() throws IOException {
        return FileUtils.loadFromDisk(filterFile, dataFromDisk -> {
            if (dataFromDisk != null && !dataFromDisk.isBlank()) {
                return BitSet.valueOf(Base64.getDecoder().decode(dataFromDisk));
            } else {
                return new BitSet(size);
            }
        }, new BitSet(size));
    }
}
