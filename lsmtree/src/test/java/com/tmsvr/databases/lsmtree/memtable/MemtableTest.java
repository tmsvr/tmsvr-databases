package com.tmsvr.databases.lsmtree.memtable;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.lsmtree.commitlog.CommitLogSegment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.tmsvr.databases.lsmtree.TestUtils.stringSerDe;
import static org.junit.jupiter.api.Assertions.*;

class MemtableTest {

    @AfterEach
    void cleanup() throws IOException {
        // Clean up any test commit log segments
        Files.list(Paths.get(""))
                .filter(path -> path.toString().matches(".*commit-log-.*\\.txt$"))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
    }

    @Test
    void testInitialisationOk() throws IOException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("test-1", stringSerDe(), stringSerDe());
        Memtable<String, String> memtable = new Memtable<>(segment);

        assertEquals(0, memtable.getSize());

        segment.close();
        segment.delete();
    }

    @Test
    void testPutOk() throws IOException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("test-2", stringSerDe(), stringSerDe());
        Memtable<String, String> memtable = new Memtable<>(segment);

        memtable.put(new DataRecord<>("a", "b"));
        memtable.put(new DataRecord<>("a", "c"));
        memtable.put(new DataRecord<>("b", "d"));

        assertEquals(2, memtable.getSize());
        assertEquals("c", memtable.get("a"));
        assertEquals("d", memtable.get("b"));

        segment.close();
        segment.delete();
    }

    @Test
    void testFreezeOk() throws IOException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("test-3", stringSerDe(), stringSerDe());
        Memtable<String, String> memtable = new Memtable<>(segment);

        memtable.put(new DataRecord<>("a", "b"));
        memtable.put(new DataRecord<>("c", "d"));

        ImmutableMemtable<String, String> immutable = memtable.freeze();

        assertNotNull(immutable);
        assertEquals("b", immutable.get("a"));
        assertEquals("d", immutable.get("c"));
        assertEquals(segment.getSegmentId(), immutable.getCommitLogSegmentId());

        segment.close();
        segment.delete();
    }

    @Test
    void testConcurrentPuts() throws IOException, InterruptedException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("test-4", stringSerDe(), stringSerDe());
        Memtable<String, String> memtable = new Memtable<>(segment);

        // Test thread-safety with concurrent puts
        int threadCount = 10;
        int putsPerThread = 100;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < putsPerThread; j++) {
                        String key = "key-" + threadId + "-" + j;
                        String value = "value-" + threadId + "-" + j;
                        memtable.put(new DataRecord<>(key, value));
                    }
                } catch (IOException e) {
                    fail("Put failed: " + e.getMessage());
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all keys are present
        assertEquals(threadCount * putsPerThread, memtable.getSize());

        segment.close();
        segment.delete();
    }
}