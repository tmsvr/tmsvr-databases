package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.tmsvr.databases.lsmtree.TestUtils.stringSerDe;
import static org.junit.jupiter.api.Assertions.*;

class CommitLogTest {

    @AfterEach
    void cleanup() throws IOException {
        // Clean up any test commit log segments
        Files.list(Paths.get(""))
                .filter(path -> path.toString().matches(".*commit-log-.*\\.wal$"))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
    }

    @Test
    void testCreateSegment() throws IOException {
        DefaultCommitLog<String, String> commitLog = new DefaultCommitLog<>(stringSerDe(), stringSerDe());

        CommitLogSegment<String, String> segment = commitLog.createSegment();
        assertNotNull(segment);
        assertNotNull(segment.getSegmentId());

        assertTrue(Files.exists(Paths.get("commit-log-" + segment.getSegmentId() + ".wal")));

        segment.close();
        segment.delete();
    }

    @Test
    void testSegmentAppendAndDelete() throws IOException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("test-segment", stringSerDe(), stringSerDe());

        segment.append(new DataRecord<>("a", "b"));
        segment.append(new DataRecord<>("c", "d"));

        assertTrue(Files.exists(Paths.get("commit-log-test-segment.wal")));

        segment.close();
        segment.delete();

        assertFalse(Files.exists(Paths.get("commit-log-test-segment.wal")));
    }

    @Test
    void testRecoverSegments() throws IOException {
        DefaultCommitLog<String, String> commitLog = new DefaultCommitLog<>(stringSerDe(), stringSerDe());

        // Create multiple segments
        CommitLogSegment<String, String> seg1 = commitLog.createSegment();
        CommitLogSegment<String, String> seg2 = commitLog.createSegment();
        CommitLogSegment<String, String> seg3 = commitLog.createSegment();

        seg1.append(new DataRecord<>("a", "b"));
        seg2.append(new DataRecord<>("c", "d"));
        seg3.append(new DataRecord<>("e", "f"));

        seg1.close();
        seg2.close();
        seg3.close();

        // Recover segments
        List<CommitLogSegment<String, String>> recovered = commitLog.recoverSegments();

        assertEquals(3, recovered.size(), "Should recover all 3 segments");

        // Cleanup
        for (CommitLogSegment<String, String> seg : recovered) {
            seg.close();
            seg.delete();
        }
    }

    @Test
    void testSegmentSynchronizedAppend() throws IOException, InterruptedException {
        CommitLogSegment<String, String> segment = new CommitLogSegment<>("test-concurrent", stringSerDe(),
                stringSerDe());

        int threadCount = 10;
        int appendsPerThread = 50;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < appendsPerThread; j++) {
                        segment.append(new DataRecord<>("key-" + threadId, "value-" + j));
                    }
                } catch (IOException e) {
                    fail("Append failed: " + e.getMessage());
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        segment.close();

        // Verify file exists and has content
        assertTrue(Files.exists(Paths.get("commit-log-test-concurrent.wal")));
        long lineCount = Files.lines(Paths.get("commit-log-test-concurrent.wal")).count();
        assertEquals(threadCount * appendsPerThread, lineCount, "All appends should be present");

        segment.delete();
    }
}