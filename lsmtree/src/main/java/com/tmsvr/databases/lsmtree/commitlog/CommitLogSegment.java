package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.serde.SerDe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

/**
 * Asynchronous commit log segment with group commit.
 * Durability model:
 * - append() returns after enqueue (NOT fsync)
 * - fsync happens periodically
 * - crash may lose last FSYNC_INTERVAL_MS of data
 */
@Slf4j
public class CommitLogSegment<K extends Comparable<K>, V> {

    // Tunables
    private static final int FLUSH_EVERY_N_RECORDS = 8_192;
    private static final long POLL_TIMEOUT_MS = 10;
    private static final long FSYNC_INTERVAL_MS = 50;
    private static final int QUEUE_CAPACITY = 500_000;

    @Getter
    private final String segmentId;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;
    private final FileChannel fileChannel;
    private final Path filePath;

    private final BlockingQueue<DataRecord<K, V>> queue;
    private final Thread writerThread;
    private volatile boolean running = true;

    // Reused write buffer (writer thread only)
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(1 << 20); // 1 MB

    public CommitLogSegment(String segmentId,
                            SerDe<K> keySerDe,
                            SerDe<V> valueSerDe) throws IOException {

        this.segmentId = segmentId;
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.filePath = Paths.get("commit-log-" + segmentId + ".wal");

        this.fileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );

        this.queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        this.writerThread = new Thread(this::runWriter, "commit-log-writer-" + segmentId);
        this.writerThread.start();

        log.debug("Created async commit log segment: {}", segmentId);
    }

    /**
     * Enqueue record for asynchronous WAL append.
     * Blocks under backpressure (no data loss).
     */
    public void append(DataRecord<K, V> record) throws IOException {
        if (!running) {
            throw new IOException("Commit log segment is closed");
        }

        try {
            queue.put(record); // BLOCKING backpressure
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while enqueueing WAL record", e);
        }
    }

    private void runWriter() {
        List<DataRecord<K, V>> batch = new ArrayList<>(FLUSH_EVERY_N_RECORDS);
        long lastFsyncTime = System.nanoTime();

        try {
            while (running || !queue.isEmpty()) {
                DataRecord<K, V> first = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }

                batch.add(first);
                queue.drainTo(batch, FLUSH_EVERY_N_RECORDS);

                writeBatch(batch);
                batch.clear();

                long now = System.nanoTime();
                if (TimeUnit.NANOSECONDS.toMillis(now - lastFsyncTime) >= FSYNC_INTERVAL_MS) {
                    fileChannel.force(false); // data only
                    lastFsyncTime = now;
                }
            }

            // Final durability barrier
            fileChannel.force(true);

        } catch (Exception e) {
            log.error("Commit log writer thread failed", e);
        }
    }

    private void writeBatch(List<DataRecord<K, V>> batch) throws IOException {
        for (DataRecord<K, V> record : batch) {
            writeLine(record);
        }

        writeBuffer.flip();
        while (writeBuffer.hasRemaining()) {
            fileChannel.write(writeBuffer);
        }
        writeBuffer.clear();
    }

    private void writeLine(DataRecord<K, V> record) {
        String line =
                keySerDe.serialize(record.key()) +
                        SEPARATOR +
                        valueSerDe.serialize(record.value()) +
                        '\n';

        byte[] bytes = line.getBytes();

        if (bytes.length > writeBuffer.remaining()) {
            flushBuffer();
        }

        writeBuffer.put(bytes);
    }

    private void flushBuffer() {
        try {
            writeBuffer.flip();
            while (writeBuffer.hasRemaining()) {
                fileChannel.write(writeBuffer);
            }
            writeBuffer.clear();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush WAL buffer", e);
        }
    }

    public void close() throws IOException {
        if (!running) return;

        running = false;
        try {
            writerThread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        fileChannel.close();
        log.debug("Closed commit log segment: {}", segmentId);
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(filePath);
        log.debug("Deleted commit log segment: {}", segmentId);
    }
}
