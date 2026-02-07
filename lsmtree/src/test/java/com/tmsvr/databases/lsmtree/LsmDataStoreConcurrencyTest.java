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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LsmDataStoreConcurrencyTest {

    private static final String TEST_DIR = "test-data";

    @BeforeEach
    void setUp() throws IOException {
        // Clean up any existing test data
        TestUtils.cleanupFiles();
        Files.createDirectories(Paths.get(TEST_DIR));
    }

    @AfterEach
    void tearDown() throws IOException {
        TestUtils.cleanupFiles();
    }

    @Test
    void testConcurrentWrites() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        // Small memtable to force flushes
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 10);

        int threadCount = 20;
        int operationsPerThread = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        String key = "key-" + threadId + "-" + j;
                        String value = "value-" + threadId + "-" + j;
                        dataStore.put(key, value);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

        // Give flush executor more time to complete all pending flushes
        Thread.sleep(5000);

        // Verify data
        int foundCount = 0;
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < operationsPerThread; j++) {
                String key = "key-" + i + "-" + j;
                if (dataStore.get(key).isPresent()) {
                    foundCount++;
                }
            }
        }

        assertEquals(threadCount * operationsPerThread, foundCount, "Lost updates due to race conditions!");

        // Clean shutdown
        dataStore.close();
    }

    @Test
    void testConcurrentRotations() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        // Very small memtable to trigger many rotations
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 3);

        int threadCount = 10;
        int operationsPerThread = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        String key = "rotation-key-" + threadId + "-" + j;
                        String value = "rotation-value-" + threadId + "-" + j;
                        dataStore.put(key, value); // Will trigger many concurrent rotations
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

        // Give flush executor time
        Thread.sleep(2000);

        // Verify all data is present
        int foundCount = 0;
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < operationsPerThread; j++) {
                String key = "rotation-key-" + i + "-" + j;
                if (dataStore.get(key).isPresent()) {
                    foundCount++;
                }
            }
        }

        assertEquals(threadCount * operationsPerThread, foundCount,
                "Lost updates during concurrent rotations!");

        // Clean shutdown
        dataStore.close();
    }
}
