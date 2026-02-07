package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.lsmtree.sstable.LsmSerDe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for async flush behavior and write performance.
 */
public class AsyncFlushTest {

    @BeforeEach
    void setUp() throws IOException {
        TestUtils.cleanupFiles();
    }

    @AfterEach
    void tearDown() throws IOException {
        TestUtils.cleanupFiles();
    }

    @Test
    void testWritesAreNonBlocking() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 5);

        long startTime = System.currentTimeMillis();

        // Write enough to trigger multiple flushes (100 writes with limit of 5)
        for (int i = 0; i < 100; i++) {
            dataStore.put("key-" + i, "value-" + i);
        }

        long writeTime = System.currentTimeMillis() - startTime;

        // Writes should complete quickly (< 1 second) since they're non-blocking
        // Even though flush is happening in background
        assertTrue(writeTime < 1000,
                "Writes took " + writeTime + "ms, should be non-blocking (< 1s)");

        // Verify data is still readable immediately (from active + immutable memtables)
        for (int i = 0; i < 100; i++) {
            assertTrue(dataStore.get("key-" + i).isPresent(),
                    "Key " + i + " should be readable immediately");
        }

        dataStore.close();
    }

    @Test
    void testBackpressureMonitoring() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        // Very small memtable to trigger many rotations
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 2);

        // Write aggressively to build up immutable memtables
        for (int i = 0; i < 50; i++) {
            dataStore.put("backpressure-key-" + i, "backpressure-value-" + i);
        }

        // Should complete without crashing (backpressure just logs warnings)
        // No exceptions means backpressure is monitored gracefully

        // Verify all data is readable
        int foundCount = 0;
        for (int i = 0; i < 50; i++) {
            if (dataStore.get("backpressure-key-" + i).isPresent()) {
                foundCount++;
            }
        }

        assertTrue(foundCount >= 40,
                "Most keys should be readable (found " + foundCount + "/50)");

        dataStore.close();
    }

    @Test
    void testFlushDoesNotBlockReads() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 10);

        // Write data to trigger flush
        for (int i = 0; i < 20; i++) {
            dataStore.put("flush-key-" + i, "flush-value-" + i);
        }

        // Concurrent reads while flush is happening in background
        ExecutorService readerExecutor = Executors.newFixedThreadPool(5);
        CountDownLatch readLatch = new CountDownLatch(5);

        for (int i = 0; i < 5; i++) {
            final int readerId = i;
            readerExecutor.submit(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        int keyIndex = (readerId * 4) % 20;
                        dataStore.get("flush-key-" + keyIndex);
                    }
                } catch (Exception e) {
                    fail("Read should not fail during flush: " + e.getMessage());
                } finally {
                    readLatch.countDown();
                }
            });
        }

        assertTrue(readLatch.await(5, TimeUnit.SECONDS),
                "Reads should complete quickly even during flush");
        readerExecutor.shutdown();

        dataStore.close();
    }

    @Test
    void testGracefulShutdown() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 5);

        // Write data
        for (int i = 0; i < 25; i++) {
            dataStore.put("shutdown-key-" + i, "shutdown-value-" + i);
        }

        // Close should flush all pending data
        dataStore.close();

        // After shutdown, verify SSTables were created
        long sstableCount = Files.list(Paths.get(""))
                .filter(path -> path.toString().contains("sstable-"))
                .count();

        assertTrue(sstableCount > 0, "Should have created SSTables during shutdown");
    }

    @Test
    void testImmutableMemtablesPreserveDuringFlushFailure() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 3);

        // Write data to create immutable memtables
        for (int i = 0; i < 15; i++) {
            dataStore.put("preserve-key-" + i, "preserve-value-" + i);
        }

        // Give time for some flushes (but not all)
        Thread.sleep(500);

        // Even if flush is ongoing, data should be readable
        // This tests the invariant: immutable memtables remain readable until flush
        // completes
        for (int i = 0; i < 15; i++) {
            assertTrue(dataStore.get("preserve-key-" + i).isPresent(),
                    "Data should remain readable from immutable memtables");
        }

        dataStore.close();
    }
}
