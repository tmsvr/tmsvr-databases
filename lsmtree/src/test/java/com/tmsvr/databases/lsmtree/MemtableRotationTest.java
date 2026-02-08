package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.lsmtree.commitlog.CommitLogSegment;
import com.tmsvr.databases.lsmtree.memtable.ImmutableMemtable;
import com.tmsvr.databases.lsmtree.memtable.Memtable;
import com.tmsvr.databases.lsmtree.sstable.LsmSerDe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.tmsvr.databases.lsmtree.TestUtils.stringSerDe;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for memtable rotation using CAS (Compare-And-Swap) pattern.
 */
public class MemtableRotationTest {

    @BeforeEach
    void setUp() throws IOException {
        TestUtils.cleanupFiles();
    }

    @AfterEach
    void tearDown() throws IOException {
        TestUtils.cleanupFiles();
    }

    @Test
    void testRotationPreservesImmutability() throws IOException {
        CommitLogSegment<String, String> segment1 = new CommitLogSegment<>("rotation-1", stringSerDe(), stringSerDe());
        Memtable<String, String> memtable = new Memtable<>(segment1);

        memtable.put(new com.tmsvr.databases.DataRecord<>("key1", "value1"));
        memtable.put(new com.tmsvr.databases.DataRecord<>("key2", "value2"));

        // Freeze memtable
        ImmutableMemtable<String, String> immutable = memtable.freeze();

        // Verify immutable memtable preserves data
        assertEquals("value1", immutable.get("key1"));
        assertEquals("value2", immutable.get("key2"));

        // Verify segment is associated
        assertEquals(segment1.getSegmentId(), immutable.getCommitLogSegmentId());

        segment1.close();
        segment1.delete();
    }

    @Test
    void testWALSegmentPairing() throws IOException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("pairing-test", stringSerDe(), stringSerDe());
        Memtable<String, String> memtable = new Memtable<>(segment);

        memtable.put(new com.tmsvr.databases.DataRecord<>("a", "b"));
        memtable.put(new com.tmsvr.databases.DataRecord<>("c", "d"));

        ImmutableMemtable<String, String> immutable = memtable.freeze();

        // Verify atomic pairing: immutable owns the correct segment
        assertSame(segment.getSegmentId(), immutable.getCommitLogSegmentId());

        // Verify WAL file exists
        assertTrue(Files.exists(Paths.get("commit-log-pairing-test.wal")));

        // Cleanup
        segment.close();
        segment.delete();
        assertFalse(Files.exists(Paths.get("commit-log-pairing-test.wal")));
    }

    @Test
    void testZeroCopyFreeze() throws IOException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("zero-copy", stringSerDe(), stringSerDe());
        Memtable<String, String> memtable = new Memtable<>(segment);

        // Add data
        for (int i = 0; i < 1000; i++) {
            memtable.put(new com.tmsvr.databases.DataRecord<>("key-" + i, "value-" + i));
        }

        long startTime = System.nanoTime();
        ImmutableMemtable<String, String> immutable = memtable.freeze();
        long freezeTime = System.nanoTime() - startTime;

        // Freeze should be very fast (< 1ms) since it's zero-copy
        assertTrue(freezeTime < 1_000_000,
                "Freeze took " + (freezeTime / 1_000_000) + "ms, should be zero-copy (< 1ms)");

        // Verify data is accessible
        assertEquals("value-0", immutable.get("key-0"));
        assertEquals("value-999", immutable.get("key-999"));

        segment.close();
        segment.delete();
    }

    @Test
    void testConcurrentRotationCASBehavior() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 3);

        int threadCount = 10;
        int writesPerThread = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulWrites = new AtomicInteger(0);

        // All threads write simultaneously to trigger concurrent rotations
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    for (int j = 0; j < writesPerThread; j++) {
                        dataStore.put("cas-key-" + threadId + "-" + j, "cas-value-" + threadId + "-" + j);
                        successfulWrites.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown(); // Start all threads simultaneously
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        Thread.sleep(1000); // Give flush time

        // Verify all writes succeeded (CAS allowed only one rotation winner per
        // attempt)
        assertEquals(threadCount * writesPerThread, successfulWrites.get());

        // Verify all data is readable
        int foundCount = 0;
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < writesPerThread; j++) {
                if (dataStore.get("cas-key-" + i + "-" + j).isPresent()) {
                    foundCount++;
                }
            }
        }

        assertEquals(threadCount * writesPerThread, foundCount,
                "All writes should be readable after CAS rotations");

        dataStore.close();
    }

    @Test
    void testRotationCreatesNewSegment() throws Exception {
        LsmSerDe<String> stringSerDe = new LsmSerDe<>(Function.identity(), Function.identity());
        LsmDataStore<String, String> dataStore = new LsmDataStore<>(stringSerDe, stringSerDe, 3);

        // Count segments at the very start (should be 1: initial segment)
        long initialSegmentCount = Files.list(Paths.get(""))
                .filter(path -> path.toString().matches(".*commit-log-.*\\.wal$"))
                .count();

        // Write to trigger rotations (10 writes / 3 per memtable = ~3 rotations)
        for (int i = 0; i < 10; i++) {
            dataStore.put("segment-key-" + i, "segment-value-" + i);
        }

        // Count segments immediately (before flush completes and deletes them)
        long midSegmentCount = Files.list(Paths.get(""))
                .filter(path -> path.toString().matches(".*commit-log-.*\\.wal$"))
                .count();

        // Should have more segments now (active + immutable waiting for flush)
        // At minimum: 1 active + some immutable = at least 2
        assertTrue(midSegmentCount >= 2,
                "Rotation should create new segments (initial=" + initialSegmentCount +
                        ", current=" + midSegmentCount + ")");

        dataStore.close();
    }
}
